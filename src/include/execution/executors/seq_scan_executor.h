//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <optional>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"
#include "storage/table/table_iterator.h"


namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
 // 构造函数制作一次性的，不需要重置的准备
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  // 问自己“这个东西需要在 Init() 时归零吗”
  // 需要归零的话，扫描进度，哈希表，堆， 放 Init
  // 不需要，plan 指针， 表元信息， schema 放构造函数
  void Init() override;

  // 推进 iter_ + 填 batch 实际干活
  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  std::shared_ptr<TableInfo> table_info_;  // 缓存表元信息，避免每次 Next() 都查 catalog 
  std::optional<TableIterator> iter_; // 扫描进度，批量接口下 Next() 会被反复调用，进度必须存成成员

  
};
}  // namespace bustub
