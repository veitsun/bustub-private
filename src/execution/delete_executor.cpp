//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

/** Initialize the delete */
void DeleteExecutor::Init() { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  child_executor_->Init();
  done_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows deleted from the table
 * @param[out] rid_batch The next tuple RID batch produced by the delete (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if(done_) {
    return false;
  }
  done_ = true;

  auto *txn = exec_ctx_->GetTransaction();  // 同一个事务， 贯穿整个 DeleteExecutor 生命周期
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *table_heap = table_info_->table_.get();
  const auto *schema = &table_info_->schema_;

  int32_t count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  // 先拉完所有待删的 rid（pipeline breaker，避免边扫边改带来的边界问题）。
  std::vector<RID> rids;
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (auto &rid : child_rids) {
      rids.push_back(rid);  //  这里只需要收集 待删除 RID ，扫描完全结束后，再统一删除
    }
  }

  // 对每个 RID 走 MVCC 删除流程， 这些 rid 全部属于“当前这一个事务” 的处理范围， 更准确说， 是这个事务正在执行的这条 Delete 语句要删除的行
  // 这些 rid 从哪里来，来自 child_executor_ (通常是 SeqScan)
  for (auto &rid : rids) {
    // 原子读取 base tuple + 版本链入口。
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);

    // 写写冲突检测：这行是否被"别人"在我读之后抢先改过。
    if (IsWriteWriteConflict(meta.ts_, txn)) {
      txn->SetTainted();
      throw ExecutionException("delete: write-write conflict on rid " + rid.ToString());
    }

    // 三分支判断：
    //   1) 我插入的行（base ts 是我的临时 ts，且 undo link 不指向我）→ 不生成 undo log。
    //   2) 我已改过的行（base ts 是我的临时 ts，且 undo link 指向我）→ 合并已有 undo log。
    //   3) 别人的行（base ts 不是我的临时 ts）→ 生成新 undo log。
    bool is_my_temp_ts = (meta.ts_ == txn->GetTransactionTempTs());
    bool is_my_undo_link = undo_link.has_value() && undo_link->IsValid() &&
                           undo_link->prev_txn_ == txn->GetTransactionId();

    if (is_my_temp_ts && !is_my_undo_link) {
      // 我插入的行：不生成 undo log，undo_link 保持原样（无效）。
    } else if (is_my_temp_ts && is_my_undo_link) {
      // 已改过：合并差异到已有 undo log。
      auto old_log = txn->GetUndoLog(undo_link->prev_log_idx_);
      auto updated_log = GenerateUpdatedUndoLog(schema, &tuple, nullptr, old_log);
      txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
    } else {
      // 别人的行：生成新 undo log，记录删除前的整行旧值。
      auto undo_log = GenerateNewUndoLog(schema, &tuple, nullptr, meta.ts_, undo_link.value_or(UndoLink{}));
      undo_link = txn->AppendUndoLog(undo_log);
    }

    // 原地逻辑删除：ts 设为临时时间戳，标记 is_deleted_。
    auto new_meta = TupleMeta{txn->GetTransactionTempTs(), true};
    auto check = [&](const TupleMeta &cur_meta, const Tuple &, RID, std::optional<UndoLink>) {
      // 写锁内的二次校验，防止并发期间被别人改掉。
      return !IsWriteWriteConflict(cur_meta.ts_, txn);
    };
    if (!UpdateTupleAndUndoLink(txn_mgr, rid, undo_link, table_heap, txn, new_meta, tuple, check)) {
      txn->SetTainted();
      throw ExecutionException("delete: write-write conflict (concurrent) on rid " + rid.ToString());
    }

    txn->AppendWriteSet(plan_->GetTableOid(), rid);
    // 注意：P4 里索引项永不物理删除，所以这里不删除索引。
    count++;
  }

  std::vector<Value> values{ValueFactory::GetIntegerValue(count)};
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();
  return true;
}

}  // namespace bustub
