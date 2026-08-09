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
#include "common/macros.h"
#include "common/rid.h"
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
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // 要拿 rid 而不是 tuple 内容，但删索引要用旧 tuple 算 key
  tuple_batch->clear();
  rid_batch->clear();

  if(done_) {
    return false;
  }
  done_ = true;

  int32_t count = 0;
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  // 把子节点（通常是 SeqScan ） 彻底抽干
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for(size_t i = 0; i < child_tuples.size(); ++ i) {
      auto &tuple = child_tuples[i];
      auto &rid = child_rids[i];

      // 逻辑删除，只改 TupleMeta.is_deleted_ ,不物理删除数据
      table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, rid);

      // 索引也要删
      for(auto &idx : indexes_) {
        auto key = tuple.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
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
