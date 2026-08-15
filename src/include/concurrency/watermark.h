//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.h
//
// Identification: src/include/concurrency/watermark.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  timestamp_t commit_ts_;

  timestamp_t watermark_;   // 这是一条安全线， 比它还老的版本， ts_ <  watermark ， 没有任何活跃事务会穿过它继续往前找更早的版本， 因为所有活跃事务在那之前就已经停下来了

  /** Ordered map: read_ts_ -> 目前有多少个活跃事务持有这个 read_ts_。
   *  用 std::map（有序）而不是 unordered_map，是为了让 begin() 直接给出当前最小的
   *  read_ts_（即 watermark），insert/erase/find 均为 O(log N)。
   *  必须计数而不是直接用 set，因为多个事务可能拥有相同的 read_ts_
   *  （比如连续两次 Begin 之间没有任何 Commit）。 */
  std::map<timestamp_t, int> current_reads_;
  // 用于判断某些事务是否还活着， 这是 watermark 里的一张计数表
  // 事务 Begin()  -> addtxn  ->   这个 tead_ts 的计数 +1
  // 事务 end()   ->  RemoveTxn   ->  这个 read_ts 的计数 -1
  // 所以还有 3 个事务活着，本质就是， 计数表里还有 3 个 read_ts 条目没有归零 （或有重复计数加起来等于 3）
};

};  // namespace bustub
