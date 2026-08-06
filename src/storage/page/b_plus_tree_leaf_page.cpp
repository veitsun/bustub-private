//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 将页面设置为 leaf page
  this->SetPageType(IndexPageType::LEAF_PAGE);
  // 设置当前 size 为 0
  this->SetSize(0);
  // 设置 max size
  this->SetMaxSize(max_size);
  // 设置 next_page_id_ 为 INVALID_PAGE_ID
  this->SetNextPageId(INVALID_PAGE_ID);
  // 初始化 num_tombstones_ 为 0
  this->num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombstone_keys;
  tombstone_keys.reserve(num_tombstones_);

  for (size_t i = 0; i < num_tombstones_; i++) {
    BUSTUB_ASSERT(static_cast<int>(tombstones_[i]) < GetSize(), "tombstone index out of range");
    tombstone_keys.push_back(key_array_[tombstones_[i]]);
  }

  return tombstone_keys;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return next_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAtRef(int index) const -> const KeyType & {
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAtRef(int index) const -> const ValueType & {
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  // 物理删除一个 entry。必须先修正 tombstone 记账，再搬 entry：
  // 1) 指向 index 本身的 tombstone 记录要删掉（这个 entry 都不存在了）
  // 2) 指向 index 之后的 tombstone 下标要整体左移一格
  ClearTombstoneAt(index);
  for(size_t i = 0; i < num_tombstones_; i ++) {
    if(static_cast<int>(tombstones_[i]) > index) {
      tombstones_[i] --;
    }
  }

  for(int i = index; i < GetSize() - 1; i ++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  ChangeSizeBy(-1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key,  const ValueType &value) {
  key_array_[index] = key;
  rid_array_[index] = value;
}

/*****************************************************************************
 * TOMBSTONE（懒删除）
 *****************************************************************************/

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNumTombstones() const -> size_t { return num_tombstones_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::TombKeyIndexAt(size_t tomb_pos) const -> size_t {
  BUSTUB_ASSERT(tomb_pos < num_tombstones_, "tombstone position out of range");
  return tombstones_[tomb_pos];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstoned(int index) const -> bool {
  for(size_t i = 0; i < num_tombstones_; i ++) {
    if(static_cast<int>(tombstones_[i]) == index) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::AddTombstone(int index) -> bool {
  // 容量为 0 时 num_tombstones_(0) >= 0 成立，直接返回 false，
  // 顺带避免了对零长数组 tombstones_[0] 的越界写
  if(num_tombstones_ >= TombCapacity()) {
    return false;
  }
  tombstones_[num_tombstones_] = static_cast<size_t>(index);
  num_tombstones_ ++;
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopOldestTombstone() -> int {
  if(num_tombstones_ == 0) {
    return -1;
  }
  int oldest = static_cast<int>(tombstones_[0]);
  for(size_t i = 1; i < num_tombstones_; i ++) {
    tombstones_[i - 1] = tombstones_[i];
  }
  num_tombstones_ --;
  return oldest;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ClearTombstoneAt(int index) -> bool {
  for(size_t i = 0; i < num_tombstones_; i ++) {
    if(static_cast<int>(tombstones_[i]) == index) {
      // 保持 FIFO 相对顺序：把后面的整体前移一格
      for(size_t j = i + 1; j < num_tombstones_; j ++) {
        tombstones_[j - 1] = tombstones_[j];
      }
      num_tombstones_ --;
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ClearTombstones() { num_tombstones_ = 0; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetTombstoneIndexes(const std::vector<size_t> &indexes) {
  num_tombstones_ = 0;
  for(auto idx : indexes) {
    if(num_tombstones_ >= TombCapacity()) {
      break;
    }
    BUSTUB_ASSERT(static_cast<int>(idx) < GetSize(), "tombstone index out of range");
    tombstones_[num_tombstones_] = idx;
    num_tombstones_ ++;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  // 从后往前腾出index 这个位置
  for(int i = GetSize(); i > index; i --) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  // 被右移的 entry，其 tombstone 下标要同步 +1
  for(size_t i = 0; i < num_tombstones_; i ++) {
    if(static_cast<int>(tombstones_[i]) >= index) {
      tombstones_[i] ++;
    }
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  ChangeSizeBy(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
