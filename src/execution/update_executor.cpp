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
#include "common/macros.h"
#include "common/rid.h"
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
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();

  if(done_) {
    return false;
  }
  done_ = true;
  /**
  UPDATE student
  SET age = age + 1
  WHERE id = 10;
   */

  int32_t count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    // 找到 满足 where 条件的旧 tuple
    for(size_t i = 0; i < child_tuples.size(); ++i) {
      // 
      auto &old_tuple = child_tuples[i];
      auto &old_rid = child_rids[i];

      // 根据 SET 表达式生成新 tuple
      std::vector<Value> new_values;
      new_values.reserve(plan_->target_expressions_.size());
      for(auto &expr : plan_ -> target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
      }

      Tuple new_tuple(new_values, &table_info_->schema_);

      // 先删旧索引项
      for(auto &idx : indexes_) {
        auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      }

      // 堆里逻辑删除旧行 + 插入新行 → 拿到新 RID（RID 会变！）
      table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, old_rid);
      auto new_rid_opt = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple);
      if (!new_rid_opt.has_value()) {
        continue;
      }
      auto new_rid = new_rid_opt.value();

      for (auto &idx : indexes_) {
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
      }
      

      count ++;
    }
  }

  std::vector<Value> values{ValueFactory::GetIntegerValue(count)};
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();
  return true;


}

}  // namespace bustub
