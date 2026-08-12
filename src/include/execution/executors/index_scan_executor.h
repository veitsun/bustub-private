//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  // 用索引去查表，而不是从头到尾扫一遍表
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  /** The index scan plan node to be executed. */
  // 这是任务说明书，告诉这个执行器该查哪张表，哪个索引，有没有筛选条件。这是上层优化器事先算好塞给它的，它自己不做判断，只是照做
  const IndexScanPlanNode *plan_;

  // 表元信息、索引元信息只查一次，缓存在构造函数里（跟 SeqScanExecutor 的table_info_ 同理）
  std::shared_ptr<TableInfo> table_info_;  // 知道表在哪里，表结构长什么样
  std::shared_ptr<IndexInfo> index_info_;  // 知道索引存在哪，索引对应哪一列

  // 是否是点查找模式（pred_keys_ 非空）；否则是全表有序扫描模式
  bool point_lookup_{false};

  // 点查找模式：先把所有候选 RID 收集好，Next() 里逐个回表
  std::vector<RID> candidate_rids_;  // 查到所有“书的位置” ，（RID 书在哪个架子第几层） 先全部收集在这个列表里
  size_t cursor_{0};

  // 全表扫描模式：直接借用 B+Tree 自身的有序迭代器（本项目所有索引都是 GenericKey<8>，
  // 见 b_plus_tree_index.h 的 BPlusTreeIndexForTwoIntegerColumn）
  std::optional<BPlusTreeIndexIteratorForTwoIntegerColumn> iter_;
};
}  // namespace bustub
