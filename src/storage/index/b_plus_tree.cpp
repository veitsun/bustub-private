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
#include <utility>
#include <vector>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // 
  // 唯一我能知道的叶信息就是 header_page_id 了
  // auto guard = 
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;

}

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
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  // Context ctx;

  // 需要从 header page 读 root page id
  // if(IsEmpty()) {return false; }
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;
  if(root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 从 root 向下，每层用 二分查找 找到对应 子节点
  // 到达叶子页后，二分查找目标 key
  // 注意跳过 tomstone 中的 key
  auto current_page_id = root_page_id;
  while (true) {
    auto guard = bpm_->ReadPage(current_page_id);
    auto page = guard.As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      // 如果是叶子节点的话
      auto leaf = guard.As<LeafPage>();
      // 下面是二分找 key 的过程
      int left = 0;
      int right = leaf->GetSize() - 1;
      int found = -1;
      while (left <= right) {
        int mid = left + (right - left) / 2;
        int cmp = comparator_(leaf->KeyAt(mid), key);
        if(cmp == 0) {
          found = mid;
          break;
        }
        else if(cmp < 0) {
          left = mid + 1;
        }
        else {
          right = mid - 1;
        }
      }
      if(found == -1) {
        return false;
      }

      // 检查是否在 tombstone 中
      auto tombs = leaf->GetTombstones();
      for(const auto &tomb_key : tombs) {
        if(comparator_(tomb_key, key) == 0) {
          return false;
        }
      }

      result->push_back(leaf->ValueAt(found));
      return true;
    }


    // 不走 if 的话，说明这是 Internal page : 需要二分找 child， key[0] 是无效的
    auto internal = guard.As<InternalPage>();
    int child_idx = 0;
    int left = 1;
    int right = page->GetSize() - 1;
    // 这里的二分只需要找到第一个大于等于的 key
    while (left <= right) {
      int mid = left + (right - left) / 2;
      if(comparator_(internal->KeyAt(mid), key) <= 0) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }
    }
    current_page_id = internal->ValueAt(child_idx);

  }

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
  // 1， 先读 header
  // 2,  空树，单独处理
  // 3,  optimistic path： ReadPage 找 leaf， leaf 未满时只写 leaf
  // 4,  fallback path： 再用 Context + WritePage 路径做分裂插入

  // 首先找到目标叶子页
  page_id_t root_page_id;
  {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  // 先拿到 header page ，因为里面有 根节点在哪个页的信息
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  // 取出根叶子节点
  root_page_id = header_page->root_page_id_;
  }
  // 现在的 insert 无论会不会分裂，都直接拿写锁一路锁下去

  // Context ctx;
  // // std::deque<page_id_t> page_id_stack;
  // // 首先要找到目标叶子页 和 然后插入并处理分裂
  // ctx.root_page_id_ = root_page_id;

  if(root_page_id == INVALID_PAGE_ID) {
    // 这时才会去拿 header 写锁
    auto header_w_guard = bpm_->WritePage(header_page_id_);
    auto header_w_page = header_w_guard.AsMut<BPlusTreeHeaderPage>();

    if(header_w_page->root_page_id_ == INVALID_PAGE_ID) {
      // 双重检查，多线程情况
   
      auto new_page_id = bpm_->NewPage();
      auto root_guard = bpm_->WritePage(new_page_id);
      auto root_leaf = root_guard.AsMut<LeafPage>();

      root_leaf->Init(leaf_max_size_);
      root_leaf->SetKeyValueAt(0, key, value);
      root_leaf->SetSize(1);

      header_w_page->root_page_id_ = new_page_id;
      return true;
    }
  }


  // 否则这个树不是空的, 那么就要遍历找叶子页
  // 怎么找叶子节点，通过子节点找，
  page_id_t target_leaf_page_id = INVALID_PAGE_ID;
  auto current_page_id = root_page_id;
  // auto tree_page = LookUpLeafPage(root_page_id, ctx);
  // BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf;
  while (true) {
    auto guard = bpm_->ReadPage(current_page_id);
    auto page = guard.As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      // 那么就看看能不能插入了
      auto leaf = guard.As<LeafPage>();
      if(leaf->GetSize() < leaf_max_size_) {
        target_leaf_page_id = current_page_id;
      }
      
      break;
    }
    // page_id_stack.push_back(current_page_id);
    
    // 说明是 Internal Page
    // 这里需要找子节点吧，那下一个节点不管是 Leaf Page 还是 Internal Page，继续下一次迭代循环
    // 用二分找下一个 child page_id （不应该从0 开始二分，对于内部页，KeyAt（0） 永远是无效的）
    // current_page_id 
    // tree_page = std::reinterpret_pointer_cast<InternalPage>(tree_page);
    auto internal = guard.As<InternalPage>();
    int child_idx = 0; // child_idx 可以是 0
    int left = 1;  // 有效 key 从 1 开始
    int right = internal->GetSize() - 1;
    
    while (left <= right) {
      int mid = left + (right - left) / 2;
      // KeyAt(0) 是无效的，所以 left 要从 1 开始
      if(comparator_(internal->KeyAt(mid), key) <=0) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }
    }
    // ValueAt(0) 是有意义的，所有 child_idx 可以初始化为 0
    // ValueAt(0) 有效， 它表示最左孩子，也就是“所有小于第一个有效 key 的那棵子树”
    current_page_id = internal->ValueAt(child_idx);
    
    // ctx.write_set_.push_back(std::move(guard));
  }

  if(target_leaf_page_id != INVALID_PAGE_ID) {
    auto leaf_guard = bpm_->WritePage(target_leaf_page_id);
    auto leaf = leaf_guard.AsMut<LeafPage>();

    // if(leaf->GetSize() < leaf_max_size_) {
    //   // 这里做 “未满 leaf 插入”
    //   // 成功返回 return true， 重复键 return false
    // }
    if(leaf->GetSize() < leaf_max_size_) {
      // 在 key_array_ 中找到插入位置（二分），检查是否重复 key -> 返回 false
      int insert_pos = leaf->GetSize();
      int left = 0;
      int right = leaf->GetSize() - 1;

      while(left <= right) {
        int mid = left + (right - left) / 2;
        int cmp = comparator_(leaf->KeyAt(mid), key);
        if(cmp == 0) {
          return false;
        }
        if(cmp < 0) {
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
    
  }

  // 如果 optimistic path 发现 leaf 满了，才进入慢路径
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_w_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  root_page_id = header_w_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;


  // 然后用 WritePage 一路往下找 leaf，同时保存父路径

  current_page_id = root_page_id;
  WritePageGuard leaf_guard;
  LeafPage *leaf = nullptr;
  page_id_t leaf_page_id = INVALID_PAGE_ID;


  while(true) {
    auto guard = bpm_->WritePage(current_page_id);
    auto page = guard.As<BPlusTreePage>();


    if(page->IsLeafPage()) {
      leaf_page_id = current_page_id;
      leaf_guard = std::move(guard);
      leaf = leaf_guard.AsMut<LeafPage>();
      break;
    }


    auto internal = guard.As<InternalPage>();
    int child_idx = 0;
    int left = 1;
    int right = internal->GetSize() - 1;
    while(left <= right) {
      int mid = left + (right - left) / 2;
      if(comparator_(internal->KeyAt(mid), key) <= 0) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }
    }

    current_page_id = internal->ValueAt(child_idx);
    ctx.write_set_.push_back(std::move(guard));
  }



if(leaf->GetSize() < leaf_max_size_) {
  // 在 key_array_ 中找到插入位置（二分），检查是否重复 key -> 返回 false
  int insert_pos = leaf->GetSize();
  int left = 0;
  int right = leaf->GetSize() - 1;

  while(left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf->KeyAt(mid), key);
    if(cmp == 0) {
      return false;
    }
    if(cmp < 0) {
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
  int total = leaf->GetSize() + 1;
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
    else if(cmp == 0) {
      return false;
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
  for(int i = 0; i < split; i ++) {
    leaf->SetKeyValueAt(i, tmp_key_array[i], tmp_value_array[i]);
  }
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
  InsertIntoParent(leaf_page_id, push_up_key, new_page_id, ctx);

  return true;

}


/*****************************************************************************
 * InsertIntoParent
 *****************************************************************************/
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(page_id_t old_page_id, const KeyType &push_up_key, page_id_t new_page_id, Context &ctx) {
  // 这里逻辑上分为三种情况

  // old 本身是根节点， 没有父节点。 需要新建一个 Internal page 作为新根
  if(ctx.IsRootPage(old_page_id)) {
    auto new_root_id = bpm_->NewPage();
    auto guard = bpm_->WritePage(new_root_id);
    auto new_root = guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);

    // 内部页 key[0]  ， value[0] 是左孩子， value[1] 是右孩子
    new_root->SetValueAt(0, old_page_id);
    new_root->SetValueAt(1, new_page_id);
    new_root->SetKeyAt(1, push_up_key);
    new_root->SetSize(2);

    // 这个 新 Internal Page 为根
    ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
    ctx.root_page_id_ = new_root_id;
    return ;
  }

  // 父节点未满，直接插入。从 ctx.write_set_ 取出父节点， 找到 old_page_id 的位置，其右边插入（push_up_key, new_page_id）
  // 
  auto &parent_guard = ctx.write_set_.back();
  auto parent = parent_guard.AsMut<InternalPage>();
  if(parent->GetSize() < internal_max_size_) {
    int idx = parent->ValueIndex(old_page_id);
    // 把  把 [idx+1, size) 整体右移
    for(int i = parent->GetSize() - 1; i > idx; i --) {
      parent->SetKeyAt(i + 1, parent->KeyAt(i));
      parent->SetValueAt(i + 1, parent->ValueAt(i));
    }
    parent->SetKeyAt(idx + 1, push_up_key);
    parent->SetValueAt(idx + 1, new_page_id);
    parent->ChangeSizeBy(1);

    return ;
  }

  // 父节点也满了，递归分裂
  // total = internal_max_size_ + 1 个 value，internal_max_size_ 个有效 key
  int total = parent->GetSize() + 1;
  // 先把父节点现有内容 + 新 KV 放入临时数组
  //   key_tmp[0] 无效，key_tmp[i] 对应 val_tmp[i]
  std::vector<KeyType> key_tmp(total);
  std::vector<page_id_t> val_tmp(total);
  int idx = parent->ValueIndex(old_page_id);
  // 拷贝 [0, idx] 部分
  for(int i = 0; i <= idx; i ++) {
    key_tmp[i] = parent->KeyAt(i);
    val_tmp[i] = parent->ValueAt(i);
  }
  // 插入新 kv 在 idx + 1
  key_tmp[idx + 1] = push_up_key;
  val_tmp[idx + 1] = new_page_id;
  // 拷贝 [idx + 1, size) 部分
  for(int i = idx + 1; i < parent->GetSize(); i ++) {
    key_tmp[i + 1] = parent->KeyAt(i);
    val_tmp[i + 1] = parent->ValueAt(i);
  }

  // 分裂点：左侧保留 [0, split), 上推 key_tmp[split], 右侧保留[split + 1, total)
  int split = total / 2;
  KeyType new_push_up_key = key_tmp[split];

  // 原父节点截断为左半部分
  parent->SetSize(split); // 保留 val[0..split-1]，key[0..split-1]
  for(int i = 0; i < split; i ++) {
    if(i > 0) {
      parent->SetKeyAt(i, key_tmp[i]);

    }
    parent->SetValueAt(i, val_tmp[i]);
  }

  // 新建右半部分 Internal Page
  auto new_internal_page_id = bpm_->NewPage();
  auto new_guard = bpm_->WritePage(new_internal_page_id);
  auto new_internal = new_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);

  int right_size = total - split;
  new_internal->SetSize(right_size);
  // 右侧从 split + 1 开始， 但 new_internal 的 index 是从 0 开始
  
  // new_internal 的 key[0] 是无效的，但 value[0] 有效
  // 右页的最左孩子应该接住 val_tmp[split], 因为 key_tmp[split] 被上推到父节点了

  new_internal->SetValueAt(0, val_tmp[split]); // 右页的最左孩子先放到 value[0]
  
  // 从 key[1] 、 value[1] 开始填右半边剩余内容
  int new_idx = 1;

  // for(int i = 0; i < right_size; i ++) {
  //   new_internal->SetKeyAt(i, key_tmp[split + 1 + i]);
  //   new_internal->SetValueAt(i, val_tmp[split + 1 + i]);
  // }
  for(int i = split + 1; i < total; i ++) {
    new_internal->SetKeyAt(new_idx, key_tmp[i]);
    new_internal->SetValueAt(new_idx, val_tmp[i]);
    new_idx ++;
  }


  // 递归上推
  page_id_t old_internal_page_id = ctx.write_set_.back().GetPageId();
  ctx.write_set_.pop_back();
  // page_id_stack.pop_back();

  InsertIntoParent(old_internal_page_id, new_push_up_key, new_internal_page_id, ctx);


  // 关于 ctx.write_set_ 的使用
  // 这就是为什么遍历时要把每一层的 guard 存进 ctx.write_set_ ， 而不是用局部变量。分裂时需要
  // 从栈顶（write_set_.back()） 取父节点，处理完后 pop_back() ， 如果父节点也分裂了就继续取
  // 上一层，天然形成递归向上的结构
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
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  ctx.header_page_ = bpm_->WritePage(header_page_id_);

  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;

  if(root_page_id == INVALID_PAGE_ID) {
    return ;
  }

  // 遍历到叶子，路径存入 write_set_
  auto current_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  LeafPage *leaf = nullptr;
  WritePageGuard leaf_guard;

  while (true) {
    auto guard = bpm_->WritePage(current_page_id);
    auto page = guard.As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      leaf_page_id = current_page_id;
      leaf_guard = std::move(guard);
      leaf = leaf_guard.AsMut<LeafPage>();
      break;
    }

    // 否则是 Internal page
    auto internal = guard.As<InternalPage>();
    int child_idx = 0;
    int left = 1;
    int right = internal->GetSize() - 1;
    while(left <= right) {
      int mid = left + (right - left) / 2;
      if(comparator_(internal->KeyAt(mid), key) <=0) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }

    }
    current_page_id = internal->ValueAt(child_idx);
    ctx.write_set_.push_back(std::move(guard));
  }

  // 二分查找 key
  int lo = 0;
  int hi = leaf->GetSize() - 1;
  int found = -1;
  while(lo <= hi) {
    int mid = lo + (hi - lo) / 2;
    int cmp = comparator_(leaf->KeyAt(mid), key);
    if(cmp == 0) {
      found = mid;
      break;
    }
    else if(cmp < 0) {
      lo = mid + 1;
    }
    else {
      hi = mid - 1;
    }
  }
  if(found == -1) {
    return ;
  }

  // 检查查找到的是否已经 tombstoned
  for(const auto &tk : leaf->GetTombstones()) {
    if(comparator_(tk, key) == 0) {
      return ;
    }
  }
  
  // 优先 tombstone 懒删除，先不管这个
  

  // 物理删除
  leaf->RemoveAt(found);

  // 叶子是根， 允许为空 （树变空时， 更新 header）
  if(ctx.write_set_.empty()) {
    if(leaf->GetSize() == 0) {
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    }
    return ;
  }

  // 处理 underflow
  int leaf_min = (leaf_max_size_ + 1) / 2;
  if(leaf->GetSize() >= leaf_min) {
    return ;
  }

  // underflow 下溢出的处理流程
  auto &parent_guard = ctx.write_set_.back();
  auto parent = parent_guard.AsMut<InternalPage>();
  int idx = parent->ValueIndex(leaf_page_id);

  // 尝试从左兄弟借
  if(idx > 0) {
    auto left_guard = bpm_->WritePage(parent->ValueAt(idx -1));
    auto left_sib = left_guard.template AsMut<LeafPage>();
    if(left_sib->GetSize() > leaf_min) {
      // 把左兄弟的最后一个 kv 移到当前叶子的最前面
      int last = left_sib->GetSize() - 1;
      for(int i = leaf->GetSize() - 1; i > 0 ; i --) {
        leaf->SetKeyValueAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i  - 1));
      }
      leaf->SetKeyValueAt(0, left_sib->KeyAt(last), left_sib->ValueAt(last));
      leaf->ChangeSizeBy(1);
      left_sib->ChangeSizeBy(-1);
      parent->SetKeyAt(idx, leaf->KeyAt(0)); // 更新分割 key
      return ;
    }
  }

  // 尝试从右兄弟处借
  if(idx < parent->GetSize() - 1) {
    auto right_guard = bpm_->WritePage(parent->ValueAt(idx + 1));
    auto right_sib  = right_guard.template AsMut<LeafPage>();
    if(right_sib->GetSize() > leaf_min) {
      // 把右兄弟第一个 kv 移到当前叶子的后面
      leaf->SetKeyValueAt(leaf->GetSize(), right_sib->KeyAt(0), right_sib->ValueAt(0));
      leaf->ChangeSizeBy(1);
      right_sib->ChangeSizeBy(-1);
      right_sib->RemoveAt(0);
      parent->SetKeyAt(idx + 1, right_sib->KeyAt(0));
      return ;
    }
  }

  // 合并：优先与左兄弟合并
  if(idx > 0) {
    // 把当前叶子合并进左兄弟
    auto left_guard = bpm_->WritePage(parent->ValueAt(idx - 1));
    auto left_sib = left_guard.template AsMut<LeafPage>();
    for(int i = 0; i < leaf->GetSize(); i ++) {
      left_sib->SetKeyValueAt(left_sib->GetSize() + i, leaf->KeyAt(i), leaf->ValueAt(i));
    }

    left_sib->ChangeSizeBy(leaf->GetSize());

    left_sib->SetNextPageId(leaf->GetNextPageId());

    // 从父节点删除 idx 处 的 key 和 value （即  leaf_page_id 对应的槽）
    parent->RemoveAt(idx);
  }
  else{
    // 与右兄弟合并（把右兄弟合并进当前叶子）
    auto right_guard = bpm_->WritePage(parent->ValueAt(idx + 1));
    auto right_sib = right_guard.template AsMut<LeafPage>();
    for(int i = 0; i < right_sib->GetSize(); i ++) {
      leaf->SetKeyValueAt(leaf->GetSize() + i, right_sib->KeyAt(i), right_sib->ValueAt(i));

      leaf->ChangeSizeBy(right_sib->GetSize());

      leaf->SetNextPageId(right_sib->GetNextPageId());

      parent->RemoveAt(idx + 1);
    }
  }


  // 父节点也可能是 underflow, 递归向上处理
  int internal_min = (internal_max_size_ + 1) / 2;
  page_id_t child_id  = parent_guard.GetPageId();
  ctx.write_set_.pop_back();

  while(parent->GetSize() < internal_min && !ctx.write_set_.empty()) {
    auto &grand_guard = ctx.write_set_.back();
    auto grand = grand_guard.template AsMut<InternalPage>();

    int pidx = grand->ValueIndex(child_id);


    // 尝试从左兄弟借
    if(pidx > 0) {
      auto ls_guard = bpm_ -> WritePage(grand->ValueAt(pidx - 1));
      auto ls = ls_guard.template AsMut<InternalPage>();

      if(ls->GetSize() > internal_min) {
        // 把 grand 的分隔 key 下推到 parent 的最前面，ls 的最后一个 value 到当前这个 parent 来
        for(int i = parent->GetSize(); i > 0; i --) {
          parent->SetKeyAt(i, parent->KeyAt(i - 1));
          parent->SetValueAt(i, parent->ValueAt(i - 1));

        }
        parent->SetKeyAt(1, grand->KeyAt(pidx));
        parent->SetValueAt(0, ls->ValueAt(ls->GetSize() - 1));
        parent->ChangeSizeBy(1);
        grand->SetKeyAt(pidx, ls->KeyAt(ls->GetSize()  - 1));
        ls->ChangeSizeBy(-1);
        break;

      }
    }


    // 尝试从右兄弟借
    if(pidx  <  grand->GetSize() - 1) {
      auto rs_guard = bpm_->WritePage(grand->ValueAt(pidx + 1));
      auto rs = rs_guard.template AsMut<InternalPage>();

      if(rs->GetSize() > internal_min) {
        parent -> SetKeyAt(parent->GetSize(), grand->KeyAt(pidx + 1));
        parent->SetValueAt(parent->GetSize(), rs->ValueAt(0));
        parent->ChangeSizeBy(1);
        grand->SetKeyAt(pidx + 1, rs->KeyAt(1));
        rs->RemoveAt(0);
        break;
      }
    }

    // 合并 Internal page
    if(pidx > 0) {
      auto ls_guard = bpm_ -> WritePage(grand->ValueAt(pidx - 1));
      auto ls = ls_guard.template AsMut<InternalPage>();
      // 把 grand 的分隔 key 下推，再把 parent 的内容追加到 ls
      ls->SetKeyAt(ls->GetSize(), grand->KeyAt(pidx));
      ls->SetValueAt(ls->GetSize(), parent->ValueAt(0));
      ls->ChangeSizeBy(1);

      for(int i = 1; i < parent->GetSize(); i ++) {
        ls->SetKeyAt(ls->GetSize(), parent->KeyAt(i));
        ls->SetValueAt(ls->GetSize(), parent->ValueAt(i));
        ls->ChangeSizeBy(1);
      }
      grand->RemoveAt(pidx);
    }
    else {
      auto rs_guard = bpm_ -> WritePage(grand->ValueAt(pidx + 1));
      auto rs = rs_guard.template AsMut<InternalPage>();
      parent->SetKeyAt(parent->GetSize(), grand->KeyAt(pidx + 1));
      parent->SetValueAt(parent->GetSize(), rs->ValueAt(0));
      parent->ChangeSizeBy(1);

      for(int i = 1; i < rs->GetSize(); i ++) {
        parent->SetKeyAt(parent->GetSize(), rs->KeyAt(i));
        parent->SetValueAt(parent->GetSize(), rs->ValueAt(i));

        parent->ChangeSizeBy(1);
      }
      grand->RemoveAt(pidx + 1);
    }

    child_id = grand_guard.GetPageId();
    parent = grand;
    ctx.write_set_.pop_back();


  }

  // 如果根节点只剩一个 child， 收缩树高
  if(ctx.write_set_.empty() && parent->GetSize() == 1) {
    ctx.header_page_ -> AsMut<BPlusTreeHeaderPage>() -> root_page_id_ = parent->ValueAt(0);
    ctx.root_page_id_ = parent->ValueAt(0);
  }



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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 

  // 返回最左叶节点里的第一个可见 key

  // 先读 header page ， 拿 root_page_id_
  auto header_guard =  bpm_->ReadPage(header_page_id_); //header_page_id_
  auto header = header_guard.As<BPlusTreeHeaderPage>();

  auto root_page_id_ = header->root_page_id_;

  // 如果 root 是 INVALID_PAGE_ID ， 直接返回 END() 
  if(root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  // 从 root 开始，一直往左走，直到叶子页
  auto current_page_id_ = root_page_id_;
  while (true) {
    auto current_page_guard = bpm_->ReadPage(current_page_id_);
    auto current_page = current_page_guard.As<BPlusTreePage>();
    if(current_page->IsLeafPage()) {
      // auto Iter = INDEXITERATOR_TYPE(bpm_, std::move(current_page_guard), current_page_id_, 0, comparator_);
      // Iter.AdvanceToNextVisible();
      // return Iter;
      return INDEXITERATOR_TYPE(bpm_, std::move(current_page_guard), current_page_id_, 0, comparator_);
    }

    // 否则到这里，当前页不是叶子节点，是中间节点，那么就要更新当前 current_page_id_ , 使读到最左叶子节点
    // 先用 internal page 的结构映射当前页
    // auto current_internal_page_guard = bpm_->ReadPage(current_page_id_);
    auto internal_page = current_page_guard.As<InternalPage>();
    current_page_id_ = internal_page->ValueAt(0);
  
  }


}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.As<BPlusTreeHeaderPage>();
  auto root_page_id = header->root_page_id_;

  if(root_page_id == INVALID_PAGE_ID) {
    return End();
  }

  auto current_page_id = root_page_id;

  while(true) {
    auto current_page_guard = bpm_->ReadPage(current_page_id);
    auto current_page = current_page_guard.As<BPlusTreePage>();

    if(current_page->IsLeafPage()) {
      auto leaf = current_page_guard.As<LeafPage>();

      // 在叶子里做 lower_bound, 找到第一个 >= key 的位置
      int left = 0;
      int right = leaf->GetSize();

      while(left < right) {
        int mid  = left + (right - left) / 2;
        if(comparator_(leaf->KeyAt(mid), key) < 0) {
          left = mid + 1;
        }
        else {
          right = mid;
        }
      }


      return INDEXITERATOR_TYPE(bpm_, std::move(current_page_guard), current_page_id, left, comparator_);
    }

    // 能走到这里说明还不是叶子节点
    auto internal = current_page_guard.As<InternalPage>();
    int child_idx = 0;
    int left = 1;
    int right = internal->GetSize() - 1;

    while(left <= right) {
      int mid = left + (right - left) / 2;
      if(comparator_(internal->KeyAt(mid), key) <= 0 ) {
        child_idx = mid;
        left = mid + 1;
      }
      else {
        right = mid - 1;
      }
    }

    current_page_id  = internal->ValueAt(child_idx);
  }
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return INDEXITERATOR_TYPE(bpm_, ReadPageGuard{}, INVALID_PAGE_ID, 0, comparator_);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  auto guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;

}

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
