//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"

#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // 计数 +1，若这是第一个持有该 read_ts_ 的事务，map 里会新建一个 entry。
  current_reads_[read_ts]++;

  // 有序 map 的 begin() 就是当前所有活跃事务里最小的 read_ts_，即新的 watermark。
  watermark_ = current_reads_.begin()->first;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    throw Exception("read ts not found in watermark");
  }

  it->second--;
  if (it->second == 0) {
    current_reads_.erase(it);
  }

  // 若还有活跃事务，更新 watermark 为当前最小值；
  // 若已经没有活跃事务，watermark_ 不再有意义，GetWatermark() 会直接返回 commit_ts_。
  if (!current_reads_.empty()) {
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
