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

  for (auto &[old_rid, old_tuple] : updates) {
    // 根据 SET 表达式基于"我看到的旧值"生成新 tuple。
    std::vector<Value> new_values;
    // target_expressions_ 是 Update 计划里每个目标列的新值表达式，它的数量正好等于表的列数
    new_values.reserve(plan_->target_expressions_.size());
    for (auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(new_values, &table_info_->schema_);  // 用算出的新值构造出新 tuple

    // 原子读取 base tuple + 版本链入口（用于冲突检测 + 生成 undo log）。
    auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, old_rid);

    // meta 是通过 GetTupleAndUndoLink 读到的这一行（old_rid） 当前 base tuple 的元信息， meta.ts_  就是这行现在最新版本是谁，什么时候写的
    // txn 当前执行 update 的这个事务
    // 写写冲突检测。
    // 这行是我写的（meta.ts_ == 我的临时ts）？        → 不冲突
    // 这行是别人写、且晚于我读（meta.ts_ > 我的read_ts_）？ → 冲突
    // 这行是别人写、但早于我读（meta.ts_ <= 我的read_ts_）？→ 不冲突
    // 判断这行是不是被我读之后的别人抢先改了
    if (IsWriteWriteConflict(meta.ts_, txn)) {
      txn->SetTainted();
      throw ExecutionException("update: write-write conflict on rid " + old_rid.ToString());
    }

    // 什么是我插入的行： 这行数据是我这个事务用 Insert 刚刚造出来的，在我插入之前，表里根本没有这一行
    // 什么是别人的行，这行数据在我这个事务开始之前就已经存在于表里了，是别人（之前事务） 写进去的
    // 三分支判断（同 DeleteExecutor）： 用于判断这次修改，该怎么处理 undo log 
    //   1) 我插入的行 → 不生成 undo log。
    //   2) 我已改过的行 → 合并已有 undo log。
    //   3) 别人的行 → 生成新 undo log。
    bool is_my_temp_ts = (meta.ts_ == txn->GetTransactionTempTs());
    bool is_my_undo_link = undo_link.has_value() && undo_link->IsValid() &&
                           undo_link->prev_txn_ == txn->GetTransactionId();

    if (is_my_temp_ts && !is_my_undo_link) {
      // 我插入的行：不生成 undo log，undo_link 保持原样（无效）。
    } else if (is_my_temp_ts && is_my_undo_link) {
      // 已改过：合并差异到已有 undo log。
      auto old_log = txn->GetUndoLog(undo_link->prev_log_idx_);
      auto updated_log = GenerateUpdatedUndoLog(schema, &base_tuple, &new_tuple, old_log);
      txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
    } else {
      // 别人的行：生成新 undo log，记录修改前的旧值。
      auto undo_log = GenerateNewUndoLog(schema, &base_tuple, &new_tuple, meta.ts_, undo_link.value_or(UndoLink{}));
      undo_link = txn->AppendUndoLog(undo_log);
    }

    // 原地更新 base tuple 为新值（ts 设为临时时间戳）。
    auto new_meta = TupleMeta{txn->GetTransactionTempTs(), false};
    // 定义 check 回调， 用来做写锁内的二次冲突校验。 它接收的参数是“写锁内重新读到最新 cur_meta”, 返回“是否可以安全写”
    auto check = [&](const TupleMeta &cur_meta, const Tuple &, RID, std::optional<UndoLink>) {
      return !IsWriteWriteConflict(cur_meta.ts_, txn); // 如果最新状态没有冲突（返回 true）， 才允许写
    };
    if (!UpdateTupleAndUndoLink(txn_mgr, old_rid, undo_link, table_heap, txn, new_meta, new_tuple, check)) {
      txn->SetTainted();
      throw ExecutionException("update: write-write conflict (concurrent) on rid " + old_rid.ToString());
    }

    txn->AppendWriteSet(plan_->GetTableOid(), old_rid);
    // 注意：P4 里 Update 采用原地更新；索引列的更新属于 Task #4，这里暂不处理索引。
    count++;
  }

  std::vector<Value> values{ValueFactory::GetIntegerValue(count)};
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();
  return true;
}

}  // namespace bustub
