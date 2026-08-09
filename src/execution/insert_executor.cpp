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
#include "common/macros.h"
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

  // 把子节点彻底抽干
  while(child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for(auto &t : child_tuples) {
      //
      // count ++;
      // 插入 tuple到 tuple heap
      // 得到新 tuple 的 RID 
      // 更新相关索引
      // 计数加一

      auto rid_opt = table_info_->table_->InsertTuple(TupleMeta{0, false}, t);
      if(!rid_opt.has_value()) {
        continue; // 页满等异常
      }
      auto rid = rid_opt.value();


      // 对每一个索引同步插入
      for(auto &idx : indexes_) {
        auto key = t.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs());
        idx->index_->InsertEntry(key, rid, exec_ctx_->GetTransaction());
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
