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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  // 这里实现乐观并发控制
  // 读： 不加锁， 靠 read_ts_ 快照， 读的是版本链里的历史版本，天然不被并发写影响
  // 写： 不预先加锁， 写之前做 “写写冲突检测”， 冲突就 SetTainted + throw ， 认输重试而非阻塞等待




  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // 从表中按顺序读取 tuple，一次最多读取 batch_size 条，把读取到的 tuple 放进 tuple_batch ， 对应的 RID 放进 rid_batch ，然后返回是否成功读到数据。 这是一个分批扫描表数据的函数
  // Next() 是唯一 真正搬数据的地方，因为只有它被反复调用，只有它拿到输出 vector 的指针
  tuple_batch->clear();
  rid_batch->clear(); // 这两个 vector 在 while 外面声明

  // 扫描进度就在 iter_ 里，所以不需要额外的 offset 断点
  // 这里的 iter_ 是 SeqScanExecutor 里的成员变量，用来表示当前扫描到表中的哪个位置。 每次 Next() ，都会从上一次停止的位置继续扫描
  auto predicate = plan_->filter_predicate_;
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *table_heap = table_info_->table_.get();
  const auto *schema = &table_info_->schema_;

  while(!iter_->IsEnd()) {
    auto rid = iter_->GetRID(); // 这里为什么需要知道 RID， 因为很多后续操作可能需要知道 tuple 的位置，
    ++(*iter_);

    // 原子读取 base tuple + 它的版本链入口（undo link）。
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);  // 这里可以诗选原子地读想要的一些信息

    // 收集当前事务能看到的 undo logs；若该行在快照里"不存在"，跳过。
    auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link, txn, txn_mgr);
    if (!undo_logs.has_value()) {
      continue;
    }

    // 回放出当前事务可见的历史版本；若最终是"已删除"，跳过。
    auto visible_tuple = ReconstructTuple(schema, tuple, meta, *undo_logs);
    if (!visible_tuple.has_value()) {
      continue;
    }

    // 谓词下推：filter_predicate_ 由 OptimizeMergeFilterScan 塞进来，SeqScan 没有子节点，
    // 求值要用自己的 GetOutputSchema()
    if(predicate != nullptr) {
      auto value = predicate->Evaluate(&(*visible_tuple), GetOutputSchema());
      if(value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }

    tuple_batch->push_back(*visible_tuple);
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
