//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // 表元信息、索引元信息整个查询期间不变，构造函数里查一次缓存住，避免 Next() 里重复哈希查找
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
}

/** Initialize the index scan */
void IndexScanExecutor::Init() {
  cursor_ = 0;
  candidate_rids_.clear();
  iter_.reset();

  point_lookup_ = !plan_->pred_keys_.empty();

  if (point_lookup_) {
    // 点查找模式：pred_keys_ 里每个表达式都是一个常量，对应一次等值探测（用于 WHERE v = c
    // 或者 v = c1 OR v = c2 这种被优化器识别成多次点查的场景）
    for (const auto &key_expr : plan_->pred_keys_) {  // 对列表里的每一个值
      // 常量表达式求值不依赖任何 tuple/schema，传 nullptr/空 schema 即可
      auto value = key_expr->Evaluate(nullptr, Schema({}));
      Tuple key_tuple({value}, &index_info_->key_schema_);  // 把这个值包成一个查询用的小卡片

      std::vector<RID> rids;
      index_info_->index_->ScanKey(key_tuple, &rids, exec_ctx_->GetTransaction()); // 拿这张卡片去索引里查，索引会给你一堆位置RID， 存进 rids
      candidate_rids_.insert(candidate_rids_.end(), rids.begin(), rids.end());  // 把这些位置全部塞进 candidate_rids_ 这个总列表里
    }
    return;
  }

  // 全表有序扫描模式（来自 OptimizeOrderByAsIndexScan）：本项目所有索引都硬编码成
  // GenericKey<8>（见 b_plus_tree_index.h），downcast 拿到具体类型才能用Begin()/End()
  auto *bpt_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  BUSTUB_ASSERT(bpt_index != nullptr, "IndexScanExecutor only supports BPlusTreeIndex full scan");
  iter_.emplace(bpt_index->GetBeginIterator());
}

/**
 * Yield the next tuple batch from the index scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto predicate = plan_->filter_predicate_;  // 这个是额外的筛选条件

  if (point_lookup_) {
    // 候选 RID 已经在 Init() 里全部收集好，这里只需要逐个回表+ 过滤
    while (cursor_ < candidate_rids_.size()) {
      // 逐个从 candidate_rids_ 里取位置
      // 拿完这个位置，进度指针往后挪一格，下次从这继续，不会重复拿
      auto rid = candidate_rids_[cursor_++];
      auto [meta, tuple] = table_info_->table_->GetTuple(rid); // 回表--拿着位置里把这一行完整的数据真正取出来（因为索引卡片上只写了值 和 位置， 没写这一行其他列的内容）

      if (meta.is_deleted_) {
        // 检查这行是不是已经被删掉但还没真正清理掉的僵尸数据，是的话跳过（数据库删除通常是“打个删除标记”， 不是马上把数据物理抹掉）
        continue;
      }
      if (predicate != nullptr) {
        auto value = predicate->Evaluate(&tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue;
        }
      }

      // 塞进两个输出列表
      tuple_batch->push_back(tuple);
      rid_batch->push_back(rid);

      if (tuple_batch->size() >= batch_size) {
        // 凑够一批（batch_size） 就返回 true，把这批交出去，下次再从当前进度继续
        return true;
      }
    }
    // 如果列表已经取完了，但手上还攒着没凑够一批的数据，也要返回 true 把这些吐出去（这是最容易漏的坑：如果这里错误地直接返回false，会把最后一批数据悄悄丢掉，查询结果会莫名其妙少几行）
    return !tuple_batch->empty();
  }

  // 全表有序扫描：跟 SeqScanExecutor完全一样的三阶段骨架，只是候选 RID 来自 B+Tree 迭代器
  while (!iter_->IsEnd()) {
    // 这里是从 “那根手指” iter_ 当前指着的位置直接读，读完就把手指往后挪一格， 继续指向卡片盒里的下一张
    auto [key, value] = **iter_;
    auto rid = value;
    ++(*iter_);  // 这里一定要先把手指挪到下一个位置，再判断这一行要不要跳过。如果反过来（先判断要跳过就直接 continue， 忘了挪手指）
    // 手指会一直卡在同一个位置不动，程序就会卡死在死循环里

    auto [meta, tuple] = table_info_->table_->GetTuple(rid);

    if (meta.is_deleted_) {
      continue;
    }
    if (predicate != nullptr) {
      auto pred_value = predicate->Evaluate(&tuple, GetOutputSchema());
      if (pred_value.IsNull() || !pred_value.GetAs<bool>()) {
        continue;
      }
    }

    tuple_batch->push_back(tuple);
    rid_batch->push_back(rid);

    if (tuple_batch->size() >= batch_size) {
      return true;
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
