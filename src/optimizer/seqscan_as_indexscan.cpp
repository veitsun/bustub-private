//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 *
 * 把 `SeqScan { filter = (col = const) }` 优化成 `IndexScan { pred_keys = [const] }`。
 * 前置条件是 OptimizeMergeFilterScan 已经把 Filter + SeqScan 合并成了带 filter 的 SeqScan。
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));  // 这里先递归优化子节点
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));  // 用优化后的子树重建

  // 然后才是对当前节点做匹配和改写。先处理叶子，再处理上层，保证整棵树的每个节点都被检查一次

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  const auto &filter = seq_scan.filter_predicate_;
  if (filter == nullptr) {
    return optimized_plan;
  }

  // filter 必须是 `col = const` 的等值比较。
  const auto *cmp = dynamic_cast<const ComparisonExpression *>(filter.get());
  if (cmp == nullptr || cmp->comp_type_ != ComparisonType::Equal) {
    return optimized_plan;
  }

  // 一边是列引用，一边是常量。
  const auto *left_col = dynamic_cast<const ColumnValueExpression *>(cmp->GetChildAt(0).get());
  const auto *right_const = dynamic_cast<const ConstantValueExpression *>(cmp->GetChildAt(1).get());
  if (left_col == nullptr || right_const == nullptr) {
    return optimized_plan;
  }

  const uint32_t col_idx = left_col->GetColIdx();

  // 查找该表上是否有单列索引，且索引列恰好是这个列。
  const auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
  const auto indices = catalog_.GetTableIndexes(table_info->name_);

  // 再遍历表上的所有索引，对比每个索引的 key_attrs (它记录着“这个索引建再表的哪几列上”)
  for (const auto &index : indices) {
    const auto &key_attrs = index->index_->GetKeyAttrs();
    if (key_attrs.size() == 1 && key_attrs[0] == col_idx) {
      // 命中：生成 IndexScan，pred_keys 存放常量，供点查找使用；同时保留 filter 以便过滤。
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_, index->index_oid_,
                                                 filter, std::vector<AbstractExpressionRef>{cmp->GetChildAt(1)});
    }
  }

  return optimized_plan;
}

}  // namespace bustub
