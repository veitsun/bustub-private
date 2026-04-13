//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  // this->sketch_matrix_.resize(width_ * depth_);
  if(width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("width and depth must be > 0");
  }
  const uint32_t n = width_ * depth_;
  this->sketch_matrix_ = std::vector<std::atomic<uint32_t>>(n);
  for(auto &x : sketch_matrix_) {
    x.store(0, std::memory_order_relaxed);
  }

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  // 构造函数还没有写完
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  sketch_matrix_ = std::move(other.sketch_matrix_);
  for(uint32_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  // 重载赋值运算符
  if(this == &other) return *this;
  width_ = other.width_;
  depth_ = other.depth_;
  sketch_matrix_ =  std::move(other.sketch_matrix_);

  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for(uint32_t i = 0; i < depth_; i ++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  
  for(unsigned int row = 0; row < depth_; row++ ) {
    uint32_t col = hash_functions_[row](item);
    uint32_t idx = row * width_ + col;
    (this->sketch_matrix_[idx]).fetch_add(1);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  uint32_t flag = 0;
  for(auto &x : sketch_matrix_) {
    uint32_t num = other.sketch_matrix_[flag ++];
    x.fetch_add(num); 
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  // 每行取列号，读对应桶，取最小值
  unsigned int minValue = std::numeric_limits<uint32_t>::max();
  for(unsigned int row = 0; row < this->depth_; row ++) {
    unsigned int col = this->hash_functions_[row](item); // 这是该行的桶号
    // 取出每行对应每个列的桶的值
    unsigned int idx = row * width_ + col;
    unsigned int value = this->sketch_matrix_[idx].load(std::memory_order_relaxed); // 读桶里面的值
    if(value < minValue) {
      minValue = value;
    }
  }
  if(minValue < 0xffff) {
    return minValue;
  }
  return 0;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  // 把 sketch_matix_ 中的所有原子计数器归零
  // sketch_matrix_ 是一个一维数组，模拟二维矩阵
  for(auto &x : sketch_matrix_) {
    x.store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  // 从候选列表中找出 count 最高的前 k 个， 按降序返回
  if(candidates.empty()) return {};
    
  std::vector<std::pair<KeyType, uint32_t>> result;
  // 对每个候选元素调用 count, 遍历 candidates，对每个 item 调用已经实现的 Count(item), 得到 item，count 对
  // std::pair<KeyType, uint32_t> couple;
  for(auto &x : candidates) {
    auto num = Count(x);
    result.emplace_back(std::pair<KeyType, uint32_t> (x, num));
  }

  // 最小堆维护 TopK
  auto cmp = [](const std::pair<KeyType, uint32_t> &a, const std::pair<KeyType, uint32_t> &b) {
    return a.second > b.second;
  };

  std::priority_queue<std::pair<KeyType, uint32_t>,std::vector<std::pair<KeyType, uint32_t>>, decltype(cmp)> queue(cmp);
  for(auto &cp : result) {
    queue.push(cp);
    if(queue.size() > k) {
      queue.pop();
    }
  }

  result.clear();

  while(!queue.empty()) {
    // 我这里面有一个致命的 bug， while 循环里面没有 pop()
    result.emplace_back(queue.top());
    queue.pop();
  }
  std::reverse(result.begin(), result.end());
  return result;


  // return {};
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
