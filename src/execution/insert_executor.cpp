//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // table_info_ = Catalog->GetTable(plan_->GetTableOid());
  // indexes_ = Catalog->GetTableIndexes(table_info_->name_);
  table_info_ = exec_ctx_ -> GetCatalog() -> GetTable(plan_->GetTableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

/** Initialize the insert */
void InsertExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  child_executor_->Init();
  done_ = false;
}
/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // Insert 不是流式算子，它的输出只有一行
  // InsertExecutor 知道要插入哪张表，是因为执行计划里带着目标表信息
  tuple_batch->clear();
  rid_batch->clear();

  if(done_) {
    return false; // 第二次调用直接结束，否则无限插入
  }
  done_ = true;

  int32_t count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *table_heap = table_info_->table_.get();
  const auto *schema = &table_info_->schema_;

  // 把子节点彻底抽干
  while(child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for(auto &t : child_tuples) {
      // P4 Task#4.1：唯一性冲突检测 + 找可复用的已删除 slot。
      // 对每个（主键）索引，ScanKey 检查 key 是否已存在：
      //  - 若已存在的 key 指向的行对当前事务"可见"（仍然存在）→ 唯一性冲突。
      //  - 若不可见（已删除 / 尚未插入）→ 记录这个 slot，稍后复用。
      std::optional<RID> reuse_rid;
      for(auto &idx : indexes_) {
        auto key = t.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());

        std::vector<RID> existing_rids;
        idx->index_->ScanKey(key, &existing_rids, txn);

        for (auto &existing_rid : existing_rids) {
          auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, existing_rid);
          auto undo_logs = CollectUndoLogs(existing_rid, meta, tuple, undo_link, txn, txn_mgr);

          if (!undo_logs.has_value()) {
            // CollectUndoLogs 返回 nullopt：这行对当前事务"尚未插入"（未提交的新行，
            // 或提交发生在当前事务 read_ts 之后）。但 key 已被占用 → 唯一性冲突。
            txn->SetTainted();
            throw ExecutionException("duplicate key in primary index");
          }

          auto visible = ReconstructTuple(schema, tuple, meta, *undo_logs);
          if (visible.has_value()) {
            // 这个 key 对应的行对当前事务可见 → 违反唯一性约束。
            txn->SetTainted();
            throw ExecutionException("duplicate key in primary index");
          }
          // 走到这里：该行"已删除"（可见链上命中删除标记）→ 复用这个 slot。
          reuse_rid = existing_rid;
        }
      }

      if (reuse_rid.has_value()) {
        // P4 Task#4.2：复用已删除的 slot，原地插入新版本。
        auto [meta, old_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, *reuse_rid);

        bool is_my_temp = (meta.ts_ == txn->GetTransactionTempTs());
        bool is_my_link = undo_link.has_value() && undo_link->IsValid() &&
                          undo_link->prev_txn_ == txn->GetTransactionId();

        std::optional<UndoLink> new_link = undo_link;
        if (is_my_temp && !is_my_link) {
          // 自己插入又删除的行：整行都是自己的操作，对别人不可见，不生成 undo log。
        } else if (is_my_temp && is_my_link) {
          // 自己删除的行：合并已有 undo log（撤销后仍是"删除前的旧值"，is_deleted 保持 false）。
          auto old_log = txn->GetUndoLog(undo_link->prev_log_idx_);
          auto updated = GenerateUpdatedUndoLog(schema, &old_tuple, &t, old_log);
          txn->ModifyUndoLog(undo_link->prev_log_idx_, updated);
        } else {
          // 别人删除的行：生成新 undo log，记录"撤销这次插入后回到删除态"（is_deleted = true）。
          UndoLog undo_log;
          undo_log.is_deleted_ = true;
          undo_log.ts_ = meta.ts_;
          undo_log.prev_version_ = undo_link.value_or(UndoLink{});
          undo_log.modified_fields_.assign(schema->GetColumnCount(), true);
          std::vector<Value> old_values;
          std::vector<uint32_t> all_cols;
          for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
            old_values.push_back(old_tuple.GetValue(schema, i));
            all_cols.push_back(i);
          }
          auto all_schema = Schema::CopySchema(schema, all_cols);
          undo_log.tuple_ = Tuple(old_values, &all_schema);
          new_link = txn->AppendUndoLog(undo_log);
        }

        // 原地把 base tuple 写成新值（临时时间戳，未删除）。
        auto new_meta = TupleMeta{txn->GetTransactionTempTs(), false};
        auto check = [&](const TupleMeta &cur_meta, const Tuple &, RID, std::optional<UndoLink>) {
          return !IsWriteWriteConflict(cur_meta.ts_, txn);
        };
        UpdateTupleAndUndoLink(txn_mgr, *reuse_rid, new_link, table_heap, txn, new_meta, t, check);

        txn->AppendWriteSet(plan_->GetTableOid(), *reuse_rid);
        // 不新建索引项：复用已有的 key -> RID 条目。
      } else {
        // P4: 正常插入。新 tuple 时间戳设为临时时间戳，提交时改写成 commit_ts_。
        // 新插入的 tuple 不需要 undo log（插入前不存在，别的老事务会因"不可见 + 无日志"看到 nullopt）。
        auto rid_opt = table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, t);
        if(!rid_opt.has_value()) {
          continue; // 页满等异常
        }
        auto rid = rid_opt.value();

        // 记录"我插入了这一行"，提交时要把它的时间戳改写成 commit_ts_。
        txn->AppendWriteSet(plan_->GetTableOid(), rid);

        // 对每一个索引同步插入。InsertEntry 返回 false 说明并发下另一个事务抢先插入了
        // 同一个 key（唯一性被破坏），必须 SetTainted + throw 使本事务失败。
        for(auto &idx : indexes_) {
          auto key = t.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
          if (!idx->index_->InsertEntry(key, rid, exec_ctx_->GetTransaction())) {
            txn->SetTainted();
            throw ExecutionException("duplicate key in primary index");
          }
        }
      }

      count ++;
    }
  }


  // 造出一行一列的结果 tuple
  std::vector<Value> values{ValueFactory::GetIntegerValue(count)};
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();

  return true;
}

}  // namespace bustub
