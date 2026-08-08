//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) , plan_(plan), table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()) ){
  // UNIMPLEMENTED("TODO(P3): Add implementation.");

}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  // 这里需要建迭代器，为什么要在这里建 迭代器？
  iter_.emplace(table_info_->table_->MakeIterator()); // emplace 是原地构造
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // 从表中按顺序读取 tuple，一次最多读取 batch_size 条，把读取到的 tuple 放进 tuple_batch ， 对应的 RID 放进 rid_batch ，然后返回是否成功读到数据。 这是一个分批扫描表数据的函数
  // Next() 是唯一 真正搬数据的地方，因为只有它被反复调用，只有它拿到输出 vector 的指针
  tuple_batch->clear();
  rid_batch->clear(); // 这两个 vector 在 while 外面声明

  // 扫描进度就在 iter_ 里，所以不需要额外的 offset 断点
  // 这里的 iter_ 是 SeqScanExecutor 里的成员变量，用来表示当前扫描到表中的哪个位置。 每次 Next() ，都会从上一次停止的位置继续扫描
  while(!iter_->IsEnd()) {
    auto [meta, tuple] = iter_->GetTuple();
    auto rid = iter_->GetRID(); // 这里为什么需要知道 RID， 因为很多后续操作可能需要知道 tuple 的位置，
    ++(*iter_);

    if(meta.is_deleted_) {
      continue;
    }


    tuple_batch->push_back(tuple);
    rid_batch->push_back(rid);

    if(tuple_batch->size() >= batch_size) {
      // 我这次成功返回了一批数据，你可以先处理，下次再调用 Next() 时，我会继续往后扫描
      return true; // 攒满一批，下次从 iter_ 继续
    }

  }

  // 表扫完了，手上有货就得吐出去，写成 return false 会丢掉最后一批
  return !tuple_batch->empty();
  
}

}  // namespace bustub
