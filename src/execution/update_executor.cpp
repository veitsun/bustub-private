//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
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

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

/** Initialize the update */
void UpdateExecutor::Init() { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  // 整棵 executor 树只有根节点的 Init() 会被外部调用一次，其他节点的 Init() 全靠父节点自己转发
  child_executor_->Init();
  done_ = false;
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if(done_) {
    return false;
  }
  done_ = true;

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *table_heap = table_info_->table_.get();
  const auto *schema = &table_info_->schema_;

  int32_t count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  // 先拉完所有 (rid, 旧值) 对（pipeline breaker）。因为 Update 的子节点往往是 SeqScan， 而 Update 会原地改这些行。 如果边扫边改，扫描迭代器会陷入改了自己正在扫的行的混乱
  std::vector<std::pair<RID, Tuple>> updates;
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); ++i) {
      updates.emplace_back(child_rids[i], child_tuples[i]);
    }
  }

  // 先为每行算出新值，并判断更新是否涉及索引列（主键列）。
  std::vector<Tuple> new_tuples;
  new_tuples.reserve(updates.size());
  bool index_col_changed = false;
  for (auto &[rid, old_tuple] : updates) {
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(new_values, schema);
    new_tuples.push_back(new_tuple);

    // 检查任何索引的 key 列是否发生变化。
    for (auto &idx : indexes_) {
      for (auto key_attr : idx->index_->GetKeyAttrs()) {
        if (!old_tuple.GetValue(schema, key_attr).CompareExactlyEquals(new_tuple.GetValue(schema, key_attr))) {
          index_col_changed = true;
        }
      }
    }
  }

  if (index_col_changed) {
    // P4 Task#4.3：主键（索引列）更新，拆成"删旧 + 插新"两阶段。
    //   阶段1 删旧：逻辑删除旧行 + 索引 DeleteEntry（打 tombstone）
    //   阶段2 插新：插入新行 + 索引 InsertEntry（复活 tombstone 或新建）
    // 必须先删完所有旧行、再插所有新行，否则循环移位（如 col1+1）会因"新 key 还被旧行占用"而冲突。

    // 阶段1：删旧。
    for (size_t i = 0; i < updates.size(); ++i) {
      auto &[old_rid, old_tuple] = updates[i];
      auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, old_rid);

      if (IsWriteWriteConflict(meta.ts_, txn)) {
        txn->SetTainted();
        throw ExecutionException("update: write-write conflict on rid " + old_rid.ToString());
      }

      // 三分支生成/合并 undo log（删除语义，target = nullptr）。
      bool is_my_temp_ts = (meta.ts_ == txn->GetTransactionTempTs());
      bool is_my_undo_link = undo_link.has_value() && undo_link->IsValid() &&
                             undo_link->prev_txn_ == txn->GetTransactionId();
      std::optional<UndoLink> new_link = undo_link;
      if (is_my_temp_ts && !is_my_undo_link) {
        // 我插入的行：不生成 undo log。
      } else if (is_my_temp_ts && is_my_undo_link) {
        auto old_log = txn->GetUndoLog(undo_link->prev_log_idx_);
        auto updated_log = GenerateUpdatedUndoLog(schema, &base_tuple, nullptr, old_log);
        txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
      } else {
        auto undo_log = GenerateNewUndoLog(schema, &base_tuple, nullptr, meta.ts_, undo_link.value_or(UndoLink{}));
        new_link = txn->AppendUndoLog(undo_log);
      }

      // 逻辑删除旧行。
      auto new_meta = TupleMeta{txn->GetTransactionTempTs(), true};
      auto check = [&](const TupleMeta &cur_meta, const Tuple &, RID, std::optional<UndoLink>) {
        return !IsWriteWriteConflict(cur_meta.ts_, txn);
      };
      if (!UpdateTupleAndUndoLink(txn_mgr, old_rid, new_link, table_heap, txn, new_meta, base_tuple, check)) {
        txn->SetTainted();
        throw ExecutionException("update: write-write conflict (concurrent) on rid " + old_rid.ToString());
      }
      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);

      // 索引打 tombstone（逻辑删除）。
      for (auto &idx : indexes_) {
        auto key = old_tuple.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->DeleteEntry(key, old_rid, txn);
      }
    }

    // 阶段2：插新。
    for (size_t i = 0; i < updates.size(); ++i) {
      auto &new_tuple = new_tuples[i];
      auto new_rid_opt = table_heap->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple);
      if (!new_rid_opt.has_value()) {
        continue;
      }
      auto new_rid = new_rid_opt.value();
      txn->AppendWriteSet(plan_->GetTableOid(), new_rid);

      for (auto &idx : indexes_) {
        auto key = new_tuple.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->InsertEntry(key, new_rid, txn);
      }
      count++;
    }
  } else {
    // 非主键更新：原地更新。
    for (size_t i = 0; i < updates.size(); ++i) {
      auto &[old_rid, old_tuple] = updates[i];
      auto &new_tuple = new_tuples[i];

      auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, old_rid);

      if (IsWriteWriteConflict(meta.ts_, txn)) {
        txn->SetTainted();
        throw ExecutionException("update: write-write conflict on rid " + old_rid.ToString());
      }

      bool is_my_temp_ts = (meta.ts_ == txn->GetTransactionTempTs());
      bool is_my_undo_link = undo_link.has_value() && undo_link->IsValid() &&
                             undo_link->prev_txn_ == txn->GetTransactionId();

      if (is_my_temp_ts && !is_my_undo_link) {
        // 我插入的行：不生成 undo log。
      } else if (is_my_temp_ts && is_my_undo_link) {
        auto old_log = txn->GetUndoLog(undo_link->prev_log_idx_);
        auto updated_log = GenerateUpdatedUndoLog(schema, &base_tuple, &new_tuple, old_log);
        txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
      } else {
        auto undo_log = GenerateNewUndoLog(schema, &base_tuple, &new_tuple, meta.ts_, undo_link.value_or(UndoLink{}));
        undo_link = txn->AppendUndoLog(undo_log);
      }

      auto new_meta = TupleMeta{txn->GetTransactionTempTs(), false};
      auto check = [&](const TupleMeta &cur_meta, const Tuple &, RID, std::optional<UndoLink>) {
        return !IsWriteWriteConflict(cur_meta.ts_, txn);
      };
      if (!UpdateTupleAndUndoLink(txn_mgr, old_rid, undo_link, table_heap, txn, new_meta, new_tuple, check)) {
        txn->SetTainted();
        throw ExecutionException("update: write-write conflict (concurrent) on rid " + old_rid.ToString());
      }

      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);
      count++;
    }
  }

  std::vector<Value> values{ValueFactory::GetIntegerValue(count)};
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();
  return true;
}

}  // namespace bustub
