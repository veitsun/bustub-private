//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>
#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
// FULL_INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS 
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard guard, page_id_t page_id, int index, KeyComparator comparator) : bpm_(std::move(bpm)), guard_(std::move(guard)), page_id_(page_id), index_(index), comparator_(std::move(comparator)){
  //
  AdvanceToNextVisible();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {return page_id_ == INVALID_PAGE_ID; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  BUSTUB_ASSERT(!IsEnd(), "cannot dereference end iterator");
  auto leaf = guard_.As<LeafPage>();
  return {leaf->KeyAtRef(index_), leaf->ValueAtRef(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // 先把当前位置往后挪一格
  // 至于后面是否越页、是否遇到 tombstone，都交给 AdvanceToNextVisible() 处理
  if(!IsEnd()) {
    index_ ++;
    AdvanceToNextVisible();
  }
  return *this;
}

// 把当前迭代器状态推进到“下一个合法可见的 entry”， 如果后面已经没有可见元素了，就把自己变成 End() 
FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::AdvanceToNextVisible() {
  // 这里使用循环，是因为推进可能不够
  // 1. 当前位置可能越过了当前叶子页末尾，需要跳到下一页
  // 2. 跳到下一页后，第一个元素可能又是 tombstone，要继续跳
  // 3. 甚至可能连续跨多个空页 / 全 tombstone 页
  while(true) {
    if(page_id_ == INVALID_PAGE_ID) {
      return ;
    }

    // 把当前 guard 持有的页解释成叶子页
    // 索引迭代器只会在叶子页上移动，不会停在 Internal page
    auto leaf = guard_.As<LeafPage>();

    // 如果当前下标已经走到当前叶子页末尾，说明当前页已经扫描完了
    if(index_ >= leaf->GetSize()) {
      // 那么就取出叶子链表中的下一页指针
      page_id_t next_page_id = leaf->GetNextPageId();


      if(next_page_id == INVALID_PAGE_ID) {
        guard_.Drop();

        page_id_ = INVALID_PAGE_ID;
        index_ = 0;
        return ;
      }
      // 如果 还有下一页，就跳到下一页继续找
      guard_ = bpm_->ReadPage(next_page_id); // 这里重新从 buffer pool 读下一张叶子页
      page_id_ = next_page_id;
      index_ = 0;
      continue;
    }

    // 当前 index_ 还在当前页范围内，但这个位置上的 key 可能已经被 tombstone 逻辑删除了
    KeyType current_key = leaf->KeyAt(index_);
    bool deleted = false;

    // 遍历当前叶子页记录的 tombstone key
    // 只要发现当前 key 在 tombstone 中，就说明这个 entry 对迭代器来说不可见

    for(const auto &tomb_key : leaf->GetTombstones()) {
      if(comparator_(tomb_key, current_key) == 0) {
        deleted = true;
        break;
      }
    }

    // 如果当前 entry 已经被逻辑删除，就跳过它， 继续检查下一个位置
    if(deleted) {
      index_ ++;
      continue;
    }


    // 能走到这里，说明迭代器现在已经停在一个合法可见的 entry 上了
    return ;

  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
