//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf pages also contain a fixed buffer of "tombstone" indexes for entries
 * that have been deleted.
 *
 * Leaf page format (keys are stored in order, tomb order is up to you):
 *  --------------------
 * | HEADER | TOMB_SIZE | (where TOMB_SIZE is num_tombstones_)
 *  --------------------
 *  -----------------------------------
 * | TOMB(0) | TOMB(1) | ... | TOMB(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  auto GetTombstones() const -> std::vector<KeyType>;

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void RemoveAt(int index);

  auto KeyAtRef(int index) const -> const KeyType &;
  auto ValueAtRef(int index) const -> const ValueType &;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

  // 做一个插入的辅助函数
  void SetKeyValueAt(int index, const KeyType &key,  const ValueType &value);

  /* ───────────────────────── tombstone（懒删除）接口 ─────────────────────────
   * tombstones_ 是一个容量固定为 LEAF_PAGE_TOMB_CNT 的 FIFO 队列，
   * 里面存的是 key_array_ 的**下标**（不是 key 本身）。
   * 约定：tombstones_[0] 最老，tombstones_[num_tombstones_-1]最新。
   * 任何会移动 entry 的操作都必须同步修正这些下标。
   */

  /** @return tombstone 缓冲区容量（NumTombs <= 0 时为 0，即退化为纯物理删除） */
  static constexpr auto TombCapacity() -> size_t { return LEAF_PAGE_TOMB_CNT; }

  /** @return 当前已记录的 tombstone 数量 */
  auto GetNumTombstones() const -> size_t;

  /** @return FIFO 中第 tomb_pos 个 tombstone 所指向的 key 下标 */
  auto TombKeyIndexAt(size_t tomb_pos) const -> size_t;

  /** @return 下标 index 处的 entry 是否已被逻辑删除 */
  auto IsTombstoned(int index) const -> bool;

  /** 把下标 index 追加到 FIFO 队尾。缓冲区已满或容量为 0 时返回 false */
  auto AddTombstone(int index) -> bool;

  /** 弹出队首（最老）的 tombstone，返回它指向的 key 下标；队列为空返回 -1 */
  auto PopOldestTombstone() -> int;

  /** 移除指向 index 的那条 tombstone 记录（用于插入时「复活」该 key）。不存在则返回 false */
  auto ClearTombstoneAt(int index) -> bool;

  /** 清空整个 tombstone 队列（只清记账，不动 entry） */
  void ClearTombstones();

  /** 批量覆盖 tombstone 队列（split / merge 时重建用），超出容量的部分被丢弃 */
  void SetTombstoneIndexes(const std::vector<size_t> &indexes);

  /** 在 index 处插入一个 entry：右移后续 entry，并同步修正 tombstone 下标 */
  void InsertAt(int index, const KeyType &key, const ValueType &value);


 private:
  page_id_t next_page_id_;  // 把所有叶子串成链表，便于范围查询
  size_t num_tombstones_;   // 作用是延迟删除，记录“哪些位置逻辑上已经删了”
  // Fixed-size tombstone buffer (indexes into key_array_ / rid_array_).
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
