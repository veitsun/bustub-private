//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  // 首先要找到目标叶子页 和 然后插入并处理分裂

  // 首先找到目标叶子页
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  // ctx.root_page_id_ = ;
  // 先拿到 header page ，因为里面有 根节点在哪个页的信息
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>(); // 把从 bufferpool 取出的那一页的页内存解释成 BPlusTreeHeaderPage
  // 取出根叶子节点
  auto root_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;

  if(root_page_id == INVALID_PAGE_ID) {
    
    // 如果这个树是空的
    // 新建一个叶子页作为根， Init() , 插入 key/calue, 更新 header，返回 true
    auto new_page_id = bpm_->NewPage();
    auto guard = bpm_->WritePage(new_page_id);
    auto root_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    root_page->Init(leaf_max_size_);

    // 这里把第一个 （key value） 插进去
    // 然后把 size 变成 1
    root_page->SetKeyValueAt(0, key, value);
    root_page->SetSize(1);


    header_page->root_page_id_ = new_page_id;
    ctx.root_page_id_ = new_page_id;
    return true;

  }
  // 否则这个树不是空的, 那么就要遍历找叶子页
  // 怎么找叶子节点，通过子节点找，
  auto current_page_id = root_page_id;
  // auto tree_page = LookUpLeafPage(root_page_id, ctx);
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf;
  while (true) {
    auto guard = bpm_->WritePage(current_page_id);
    auto tree_page = guard.As<BPlusTreePage>();
    if(tree_page->IsLeafPage()) {
      // 那么就看看能不能插入了
      leaf = guard.AsMut<LeafPage>();
      // tree_page->
      // 做插入
      ctx.write_set_.push_back(guard);
      break;
    }
    
    // 说明是 Internal Page
    // 这里需要找子节点吧，那下一个节点不管是 Leaf Page 还是 Internal Page，继续下一次迭代循环
    // 用二分找下一个 child page_id （不应该从0 开始二分，对于内部页，KeyAt（0） 永远是无效的）
    // current_page_id 
    // tree_page = std::reinterpret_pointer_cast<InternalPage>(tree_page);
    auto internal_page = guard.As<InternalPage>();
    int child_idx = 0; // child_idx 可以是 0
    int left = 1;  // 有效 key 从 1 开始
    int right = tree_page->GetSize() - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      // KeyAt(0) 是无效的，所以 left 要从 1 开始
      if(comparator_(internal_page->KeyAt(mid), key) <=0) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }
    }
    // ValueAt(0) 是有意义的，所有 child_idx 可以初始化为 0
    // ValueAt(0) 有效， 它表示最左孩子，也就是“所有小于第一个有效 key 的那棵子树”
    current_page_id = internal_page->ValueAt(child_idx);
  
  }
  // 对叶子节点做插入, 这时候就要考虑叶子有没有满了，如果叶子没有满，直接插入；如果叶子已经满了，需要分裂
  // 在叶子页未满的时候
  if(leaf->GetSize() < leaf_max_size_) {
    // 在 key_array_ 中找到插入位置（二分），检查是否重复 key -> 返回 false
    int insert_pos = leaf->GetSize();
    int left = 0;
    int right = leaf->GetSize() - 1;

    while(left <= right) {
      int mid = left + (right - left) / 2;
      if(comparator_(leaf->KeyAt(mid), key) == 0) {
        return false;
      }
      if(comparator_(leaf->KeyAt(mid), key) < 0) {
        // insert_pos = mid;
        left = mid + 1;
      }
      else {
        insert_pos = mid;
        right = mid - 1;
      }

    }

    // insert_idx 就是他要插入的位置
    // 后移腾出位置
    for(int i = leaf->GetSize(); i > insert_pos; i--) {
      leaf->SetKeyValueAt(i, leaf->KeyAt(i-1), leaf->ValueAt(i - 1));
    }
    leaf->SetKeyValueAt(insert_pos, key, value);
    leaf->ChangeSizeBy(1);
    return true;
  }

  // 叶子已经满的时候需要分裂
  
  // 
  /**
    分裂步骤
    1. 把当前叶子的所有 KV + 新 KV 临时排好序 （共 max_size + 1 个）
    2. 从 buffer pool 申请一个新叶子页，Init()
    3. 前半部分留在原叶子，后半部分移到新叶子
    4. 更新链表指针：new_leaf->next = old_leaf->next，old_leaf->next = new_page_id
    5. 把新叶子的第一个 key 和 new_page_id 上推到父节点（这一步会递归触发内部节点的分裂）
   */

  


  // 申请新页
  auto new_page_id = bpm_->NewPage();
  auto new_guard = bpm_->WritePage(new_page_id);
  auto new_leaf = new_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);

  // WritePageGuard
  int total = leaf_max_size_ + 1;
  int split = total / 2;

  // 我需要一个临时数组存放所有 max_size + 1 个 KV （按 key 插入新 Key）
  // KeyType tmp_key_array[total];
  // ValueType tmp_value_array[total];
  std::vector<KeyType> tmp_key_array(total);
  std::vector<ValueType> tmp_value_array(total);
  // 找这个新 kv 要插入新数组的位置，用二分找
  // 先二分找新 key 在原叶子中的插入位置
  int new_insert_pos = leaf->GetSize();
  int left = 0;
  int right = leaf->GetSize() - 1;
  while(left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf->KeyAt(mid), key);
    if(cmp < 0) {
      left = mid + 1;
    }
    else {
      new_insert_pos = mid;
      right = mid - 1;
    }
  }
  // 分三阶段填入临时数组

  for(int i = 0; i < new_insert_pos; i ++) {
    tmp_key_array[i] = leaf->KeyAt(i);
    tmp_value_array[i] = leaf->ValueAt(i);
  }
  tmp_key_array[new_insert_pos] = key;
  tmp_value_array[new_insert_pos] = value;
  for(int i = new_insert_pos; i < leaf_max_size_; i ++) {
    tmp_key_array[i + 1] = leaf->KeyAt(i);
    tmp_value_array[i + 1] = leaf->ValueAt(i);
  }
  // leaf->KeyAt(i);
  // leaf->ValueAt(i);

  // 原叶子截断到 split 个
  leaf->SetSize(split);

  // 新叶子填入后半部分
  for(int i = split; i < total; i ++) {
    new_leaf->SetKeyValueAt(i - split, tmp_key_array[i], tmp_value_array[i]);
  }
  new_leaf->SetSize(total - split);

  // 更新链表
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_page_id);


  // 上推到 new_leaf -> KeyAt(0) 到 父节点
  KeyType push_up_key = new_leaf->KeyAt(0);
  // 然后调用 InsertIntoParent(old_page_id, push_up_key, new_page_id, ctx)
  /**
    父节点 InternalPage 存的是  [key, child_page_id] 的映射。分裂后有两个叶子
    old_leaf (old_page_id)  |  new_leaf (new_page_id)
    父节点需要插入一条新记录：(push_up_key=4, new_page_id)，但这条记录要插在 old_page_id
    的右边。父节点里 old_page_id 在哪个位置，决定了新记录插在哪里。所以必须用 old_page_id
   在父节点中定位。
    
   */
  InsertIntoParent(current_page_id, push_up_key, new_page_id, ctx);

  return true;

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
