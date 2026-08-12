//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <memory>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>


// 整个 b+ 树叶子链表上的位置迭代器
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  // IndexIterator();
  ~IndexIterator();  // NOLINT

  // guard_ 是 move-only 的 ReadPageGuard，隐式拷贝构造会被删除；而用户声明了析构函数会
  // 抑制隐式移动构造的生成 —— 两条规则叠加，导致这个类默认既不可拷贝也不可移动。
  // P3 的 IndexScanExecutor 需要把迭代器存成 std::optional<IndexIterator> 成员以跨次 Next() 调用，
  // 必须显式声明移动构造/移动赋值（和 P2 的 TableIterator 用的是同一个套路）。
  IndexIterator(const IndexIterator &) = delete;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;
  IndexIterator(IndexIterator &&) noexcept = default;
  auto operator=(IndexIterator &&) noexcept -> IndexIterator & = default;

  IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard guard, page_id_t page_id, int index, KeyComparator comparator); // ReadPageGuard 是 move-only 类型，不能拷贝，所以参数按值接收是可以的，但调用方必须 std::move()

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    // UNIMPLEMENTED("TODO(P2): Add implementation."); 
    if(page_id_ == INVALID_PAGE_ID && itr.page_id_ == INVALID_PAGE_ID) {
      return true;
    }

    return page_id_ == itr.page_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    // UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return !(*this == itr);
  }

 private:

  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>;


  // add your own private member variables here
  std::shared_ptr<TracedBufferPoolManager> bpm_; // 这个目的是在跨页的时候要去 buffer pool 里面读下一个页
  ReadPageGuard guard_; // 保证当前页在迭代器活着时仍然有效，避免解引用悬空
  page_id_t page_id_{INVALID_PAGE_ID}; // 当前在哪个叶子页， 统一约定 INVALID_PAGE_ID 就表示 end iterator
  int index_{0};  // 当前是在这个叶子页的第几个 entry
  KeyComparator comparator_; // 判断当前 key 是都出现在 tombstone 集合里


  void AdvanceToNextVisible();   // 迭代器内部状态维护逻辑
  auto IsCurrentEntryDeleted(const LeafPage *leaf) -> bool;

};

}  // namespace bustub
