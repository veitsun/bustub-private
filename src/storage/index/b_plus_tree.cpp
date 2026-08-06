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
#include "common/macros.h"
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

  // 螃蟹式加锁（latch crabbing）：始终「先锁住孩子，再放开父亲」，
  // 中间不存在两把锁都不持有的空窗，因此并发的分裂/合并/删页不会把脚下的页抽走。
  std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
  page_id_t current_page_id = protector->As<BPlusTreeHeaderPage>()->root_page_id_;
  if(current_page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::optional<ReadPageGuard> cur = bpm_->ReadPage(current_page_id);
  protector.reset();  // 拿到root 的读锁之后，才放开 header

  while (true) {
    auto page = cur->As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      // 如果是叶子节点的话
      auto leaf = cur->As<LeafPage>();
      // 下面是二分找 key 的过程
      int insert_pos = 0;
      int found = FindKeyInLeaf(leaf, key, &insert_pos);
      if(found == -1) {
        return false;
      }

      // 命中了，但可能已经被 tombstone 逻辑删除 —— 对外应表现为「不存在」
      if(leaf->IsTombstoned(found)) {
        return false;
      }

      result->push_back(leaf->ValueAt(found));
      return true;
    }

    // 不走 if 的话，说明这是 Internal page : 需要二分找 child， key[0] 是无效的
    auto internal = cur->As<InternalPage>();
    current_page_id = internal->ValueAt(ChildIndex(internal, key));
    auto child = bpm_->ReadPage(current_page_id);  // 先锁住孩子
    cur = std::move(child);// 移动赋值内部先 Drop() 父亲，再接管孩子
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

  // 两条路径：
  // 1) 乐观路径：读锁螃蟹式下降，只对目标叶子加写锁。覆盖「重复键」「tombstone 复活」
  //    「叶子未满直接插入」三种不会改动树结构的情况。
  // 2) 悲观路径：只有确实需要分裂（或树为空要建根）时才走。持 header 写锁 + 一路写锁下降。
  {
    page_id_t leaf_page_id = INVALID_PAGE_ID;
    bool leaf_is_root = false;
    auto leaf_guard_opt = CrabDownToLeaf(key, &leaf_page_id, &leaf_is_root);

    if(leaf_guard_opt.has_value()) {
      auto leaf = leaf_guard_opt->template AsMut<LeafPage>();
      int insert_pos = 0;
      int found = FindKeyInLeaf(leaf, key, &insert_pos);

      if(found >= 0) {
        // key 的槽位还在页里
        if(leaf->IsTombstoned(found)) {
          // 之前是被逻辑删除的 —— 这次插入相当于「复活」：撤掉 tombstone 并覆盖 value
          leaf->ClearTombstoneAt(found);
          leaf->SetKeyValueAt(found, key, value);
          return true;
        }
        // 真正的重复键
        return false;
      }

      if(leaf->GetSize() < leaf_max_size_) {
        // 未满，直接插入（InsertAt 内部会同步修正 tombstone 下标）
        leaf->InsertAt(insert_pos, key, value);
        return true;
      }
      // 走到这里：key 不存在 且 叶子已满 → 需要分裂，落到下面的悲观路径
    }
    // leaf_guard_opt 在此析构。必须在拿 header 写锁之前把所有读锁放干净，
    // 否则同一线程「读锁 header → 写锁 header」会自己把自己锁死（shared_mutex 不支持升级）。
  }

  // 如果 optimistic path 发现 leaf 满了，才进入慢路径
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_w_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_w_page->root_page_id_;

  if(root_page_id == INVALID_PAGE_ID) {
    // 空树建根。此刻已独占 header 写锁，不存在竞争，无需双重检查。
    auto new_page_id = bpm_->NewPage();
    auto root_guard = bpm_->WritePage(new_page_id);
    auto root_leaf = root_guard.AsMut<LeafPage>();

    root_leaf->Init(leaf_max_size_);
    root_leaf->SetKeyValueAt(0, key, value);
    root_leaf->SetSize(1);

    header_w_page->root_page_id_ = new_page_id;
    return true;
  }

  ctx.root_page_id_ = root_page_id;


  // 然后用 WritePage 一路往下找 leaf，同时保存父路径

  page_id_t current_page_id = root_page_id;
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
    current_page_id = internal->ValueAt(ChildIndex(internal, key));
    ctx.write_set_.push_back(std::move(guard));
  }



  //悲观路径下重新做一次判定（乐观路径的探测与加锁之间状态可能已变）
  int insert_pos = 0;
  int found = FindKeyInLeaf(leaf, key, &insert_pos);

  if(found >= 0) {
    if(leaf->IsTombstoned(found)) {
      // tombstone 复活
      leaf->ClearTombstoneAt(found);
      leaf->SetKeyValueAt(found, key, value);
      return true;
    }
    return false;
  }

  if(leaf->GetSize() < leaf_max_size_) {
    leaf->InsertAt(insert_pos, key, value);
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
    5. tombstone 按key 落到哪一页分派过去，且保持 FIFO 相对顺序
    6. 把新叶子的第一个 key 和 new_page_id 上推到父节点（这一步会递归触发内部节点的分裂）
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
  std::vector<KeyType> tmp_key_array(total);
  std::vector<ValueType> tmp_value_array(total);
  // 新 key 的插入位置已经由上面的FindKeyInLeaf 算出来了，直接复用
  int new_insert_pos = insert_pos;

  // ── tombstone 随分裂一起搬迁 ──
  // 先把旧叶子的 tombstone 下标翻译成 tmp 数组的下标：
  // 新 key 插在 new_insert_pos，位于它及其之后的旧条目在 tmp 里都要+1。
  // 然后按 split 切成左右两份，遍历顺序即 FIFO 顺序，所以相对新旧关系天然保留。
  std::vector<size_t> left_tombs;
  std::vector<size_t> right_tombs;
  for(size_t i = 0; i < leaf->GetNumTombstones(); i ++) {
    size_t t = leaf->TombKeyIndexAt(i);
    if(t >= static_cast<size_t>(new_insert_pos)) {
      t ++;
    }
    if(t < static_cast<size_t>(split)) {
      left_tombs.push_back(t);
    }
    else {
      right_tombs.push_back(t - split);
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

  // entry 都搬完、size 也定下来之后再写 tombstone（SetTombstoneIndexes 内部会断言下标合法）
  leaf->SetTombstoneIndexes(left_tombs);
  new_leaf->SetTombstoneIndexes(right_tombs);

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
 * HELPERS
 *****************************************************************************/
/**
 * @brief 在叶子页内二分查找 key。
 * @param[out] insert_pos 未命中时输出「应插入的下标」；命中时等于命中下标
 * @return 命中下标，未命中返回 -1
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindKeyInLeaf(const LeafPage *leaf, const KeyType &key, int *insert_pos) const -> int {
  int left = 0;
  int right = leaf->GetSize() - 1;
  int pos = leaf->GetSize();
  while(left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf->KeyAt(mid), key);
    if(cmp == 0) {
      *insert_pos = mid;
      return mid;
    }
    if(cmp < 0) {
      left = mid + 1;
    }
    else {
      pos = mid;
      right = mid - 1;
    }
  }
  *insert_pos = pos;
  return -1;
}

/**
 * @brief 内部页二分：返回应当下降到的 child 槽位。
 *
 * 语义是「最后一个 key[i] <= 目标 key 的 i」，因为内部页的 key[i] 是子树 i 的最小 key。
 * 注意下标约定：KeyAt(0) 是占位无效值，所以二分从 1 开始；
 * ValueAt(0) 有效（最左孩子，承载所有小于第一个有效 key 的子树），所以默认值取 0。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ChildIndex(const InternalPage *internal, const KeyType &key) const -> int {
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
  return child_idx;
}

/**
 * @brief 乐观路径专用的螃蟹式下降，返回目标叶子的写锁 guard。
 *
 * 不变式：循环中始终持有 protector（目标页父亲的读锁）。
 * 之所以够用，是因为「分裂 / 借位 / 合并 / 删页 / 换根」这些会改动某页在树中位置的操作，
 * 都必须先写锁它的父亲（根页的父亲就是 header page）。只要 protector 在手，
 * 就没有任何线程能把我们脚下的这一页搬走。
 *
 * 与之前的写法相比，关键区别是「先锁孩子，再放父亲」——
 * 原来的实现是在循环体末尾析构 guard，等于先放父亲再锁孩子，中间有一个不持任何锁的空窗。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabDownToLeaf(const KeyType &key, page_id_t *leaf_page_id, bool *leaf_is_root)
    -> std::optional<WritePageGuard> {
  std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
  page_id_t current_page_id = protector->As<BPlusTreeHeaderPage>()->root_page_id_;
  if(current_page_id == INVALID_PAGE_ID) {
    return std::nullopt;  // 空树
  }

  bool is_root = true;
  std::optional<ReadPageGuard> cur = bpm_->ReadPage(current_page_id);

  while(!cur->As<BPlusTreePage>()->IsLeafPage()) {
    auto internal = cur->As<InternalPage>();
    page_id_t child_id = internal->ValueAt(ChildIndex(internal, key));
    auto child = bpm_->ReadPage(child_id);  // 先锁住孩子
    protector = std::move(cur);             // 父亲升级为新 protector（旧 protector 在此被释放）
    cur = std::move(child);
    current_page_id = child_id;
    is_root = false;
  }

  // shared_mutex 不支持锁升级，同一线程「持读锁再要写锁」会自锁死，
  // 所以必须先放掉叶子的读锁，再去拿它的写锁。
  // 这个空窗是安全的：protector（父亲，或根叶子情况下的 header）读锁还在手上，
  // 没有线程能在此期间分裂或删掉这个叶子。
  cur.reset();
  std::optional<WritePageGuard> leaf_guard = bpm_->WritePage(current_page_id);
  protector.reset();

  *leaf_page_id = current_page_id;
  *leaf_is_root = is_root;
  return leaf_guard;
}

/**
 * @brief 两页合并之后，tombstone 数量可能超过固定容量，必须把多出来的「兑现」成物理删除。
 *
 * 策略：FIFO —— 从最老的一条开始物理删除它指向的 entry，直到剩余数量能装进缓冲区。
 * 实现上先ClearTombstones() 把页内记账清空，这样 RemoveAt() 只会搬 entry 不会干扰
 * 我们手上这份 tombs 列表，下标修正由本函数统一负责。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FlushTombstonesToFit(LeafPage *page, std::vector<size_t> &tombs) {
  page->ClearTombstones();

  while(tombs.size() > LeafPage::TombCapacity()) {
    size_t victim = tombs.front();
    tombs.erase(tombs.begin());
    page->RemoveAt(static_cast<int>(victim));
    // 被删掉的 entry 之后的下标整体左移一格
    for(auto &t : tombs) {
      if(t > victim) {
        t --;
      }
    }
  }

  page->SetTombstoneIndexes(tombs);
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
  /* ── 乐观路径 ──
   * 绝大多数删除既不借位也不合并，更不需要改父节点和 header。
   * 这条路径用读锁螃蟹式下降，只对目标叶子加一次写锁，
   * 覆盖：key 不存在 / 已 tombstone / tombstone 缓冲区还有空位 / 物理删除后不会 underflow。
   */
  {
    page_id_t opt_leaf_page_id = INVALID_PAGE_ID;
    bool leaf_is_root = false;
    auto leaf_guard_opt = CrabDownToLeaf(key, &opt_leaf_page_id, &leaf_is_root);
    if(!leaf_guard_opt.has_value()) {
      return ;  // 空树
    }

    auto opt_leaf = leaf_guard_opt->template AsMut<LeafPage>();
    int opt_insert_pos = 0;
    int opt_found = FindKeyInLeaf(opt_leaf, key, &opt_insert_pos);

    if(opt_found == -1) {
      return ;  // key 不存在
    }
    if(opt_leaf->IsTombstoned(opt_found)) {
      return ;  // 已经逻辑删除过了，幂等
    }

    // tombstone 缓冲区还有空位 → 纯逻辑删除，物理结构完全不变
    if(LeafPage::TombCapacity() > 0 && opt_leaf->GetNumTombstones() < LeafPage::TombCapacity()) {
      opt_leaf->AddTombstone(opt_found);
      return ;
    }

    // 需要真正物理删掉一条，先判断会不会破坏最小占用约束。
    // 根叶子豁免 underflow，但若会被删空就得改 header，那必须走悲观路径。
    bool safe = leaf_is_root ? (opt_leaf->GetSize() - 1 > 0)
                             : (opt_leaf->GetSize() - 1 >= opt_leaf->GetMinSize());
    if(safe) {
      if(LeafPage::TombCapacity() > 0) {
        int victim = opt_leaf->PopOldestTombstone();
        opt_leaf->RemoveAt(victim);
        if(opt_found > victim) {
          opt_found --;
        }
        opt_leaf->AddTombstone(opt_found);
      }
      else {
        opt_leaf->RemoveAt(opt_found);
      }
      return ;
    }
    // 会underflow（或要改 header）→ 释放所有锁，退化到悲观路径重做
  }

  // ── 悲观路径：持 header 写锁 + 一路写锁下降，可以安全地借位 / 合并 / 换根 ──
  Context ctx;
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
    current_page_id = internal->ValueAt(ChildIndex(internal, key));
    ctx.write_set_.push_back(std::move(guard));
  }

  // 二分查找 key
  int insert_pos = 0;
  int found = FindKeyInLeaf(leaf, key, &insert_pos);
  if(found == -1) {
    return ;
  }

  // 检查查找到的是否已经 tombstoned —— 已经逻辑删除过了，直接返回
  if(leaf->IsTombstoned(found)) {
    return ;
  }

  /* ── tombstone 懒删除 ──
   * 优先只打标记，不动物理结构：
   *   a) 缓冲区还有空位→ 纯逻辑删除，size 不变，绝不会 underflow，直接返回
   *   b) 缓冲区已满      → 淘汰最老的一条，把它「兑现」成物理删除腾出位置，
   *                        再把当前 key 记为新的 tombstone。此时 size 减 1，
   *                        才有可能 underflow，需要继续走下面的借位/合并流程
   *   c) 容量为 0        → 退化成原来的纯物理删除
   */
  if(LeafPage::TombCapacity() > 0) {
    if(leaf->GetNumTombstones() < LeafPage::TombCapacity()) {
      leaf->AddTombstone(found);
      return ;
    }
    int victim = leaf->PopOldestTombstone();
    leaf->RemoveAt(victim);
    // 物理删除会让victim 之后的下标左移，found 也要跟着修正
    if(found > victim) {
      found --;
    }
    leaf->AddTombstone(found);
  }
  else {
    // 物理删除
    leaf->RemoveAt(found);
  }

  // 叶子是根， 允许为空 （树变空时， 更新 header）
  if(ctx.write_set_.empty()) {
    if(leaf->GetSize() == 0) {
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
      leaf_guard.Drop();
      auto ok = bpm_->DeletePage(leaf_page_id);
      BUSTUB_ASSERT(ok, "failed to delete empty root leaf");
    }
    return ;
  }

  // 处理 underflow
  int leaf_min = leaf->GetMinSize();
  if(leaf->GetSize() >= leaf_min) {
    return ;
  }

  // underflow 下溢出的处理流程
  auto &parent_guard = ctx.write_set_.back();
  auto parent = parent_guard.AsMut<InternalPage>();
  int idx = parent->ValueIndex(leaf_page_id);

  // 尝试从左兄弟借
  // 注意：只借「可见」的 entry。如果待借的那条本身是 tombstone，借过来对缓解
  // underflow 毫无帮助（它依然不可见），还会把 tombstone 记账跨页搬运搞复杂，
  // 所以这种情况直接放弃借位、走下面的合并分支。
  if(idx > 0) {
    auto left_guard = bpm_->WritePage(parent->ValueAt(idx -1));
    auto left_sib = left_guard.template AsMut<LeafPage>();
    int last = left_sib->GetSize() - 1;
    if(left_sib->GetSize() > leaf_min && !left_sib->IsTombstoned(last)) {
      // 把左兄弟的最后一个 kv 移到当前叶子的最前面
      leaf->InsertAt(0, left_sib->KeyAt(last), left_sib->ValueAt(last));
      left_sib->RemoveAt(last);
      parent->SetKeyAt(idx, leaf->KeyAt(0)); // 更新分割 key
      return ;
    }
  }

  // 尝试从右兄弟处借
  if(idx < parent->GetSize() - 1) {
    auto right_guard = bpm_->WritePage(parent->ValueAt(idx + 1));
    auto right_sib  = right_guard.template AsMut<LeafPage>();
    if(right_sib->GetSize() > leaf_min && !right_sib->IsTombstoned(0)) {
      // 把右兄弟第一个 kv 移到当前叶子的后面
      leaf->InsertAt(leaf->GetSize(), right_sib->KeyAt(0), right_sib->ValueAt(0));
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

    // 合并后的 tombstone FIFO = 目标页自己的（较老，排前面）+ 被吸收页的（下标平移 base）
    int base = left_sib->GetSize();
    std::vector<size_t> merged;
    for(size_t i = 0; i < left_sib->GetNumTombstones(); i ++) {
      merged.push_back(left_sib->TombKeyIndexAt(i));
    }
    for(size_t i = 0; i < leaf->GetNumTombstones(); i ++) {
      merged.push_back(leaf->TombKeyIndexAt(i) + static_cast<size_t>(base));
    }

    for(int i = 0; i < leaf->GetSize(); i ++) {
      left_sib->SetKeyValueAt(base + i, leaf->KeyAt(i), leaf->ValueAt(i));
    }

    left_sib->ChangeSizeBy(leaf->GetSize());

    left_sib->SetNextPageId(leaf->GetNextPageId());

    // 两页的 tombstone 加起来可能超容量，超出的部分兑现成物理删除
    FlushTombstonesToFit(left_sib, merged);

    // 从父节点删除 idx 处 的 key 和 value （即  leaf_page_id 对应的槽）
    parent->RemoveAt(idx);
    leaf_guard.Drop();
    auto ok = bpm_->DeletePage(leaf_page_id);
    BUSTUB_ASSERT(ok, "failed to delete merged leaf");
  }
  else{

    // 这里被删除的是右兄弟，不是当前叶子，先记住页号，再删
    page_id_t right_page_id = parent->ValueAt(idx + 1);
    std::vector<size_t> merged;
    {
      // 与右兄弟合并（把右兄弟合并进当前叶子）
      auto right_guard = bpm_->WritePage(right_page_id);
      auto right_sib = right_guard.template AsMut<LeafPage>();

      int base = leaf->GetSize();
      for(size_t i = 0; i < leaf->GetNumTombstones(); i ++) {
        merged.push_back(leaf->TombKeyIndexAt(i));
      }
      for(size_t i = 0; i < right_sib->GetNumTombstones(); i ++) {
        merged.push_back(right_sib->TombKeyIndexAt(i) + static_cast<size_t>(base));
      }

      for(int i = 0; i < right_sib->GetSize(); i ++) {
        leaf->SetKeyValueAt(base + i, right_sib->KeyAt(i), right_sib->ValueAt(i));
      }
      leaf->ChangeSizeBy(right_sib->GetSize());
      leaf->SetNextPageId(right_sib->GetNextPageId());
      parent->RemoveAt(idx + 1);
      right_guard.Drop();
    }
    FlushTombstonesToFit(leaf, merged);
    auto ok = bpm_->DeletePage(right_page_id);
    BUSTUB_ASSERT(ok, "failed to delete merged right leaf");

  }


  // 父节点也可能是 underflow, 递归向上处理
  int internal_min = (internal_max_size_ + 1) / 2;
  // page_id_t child_id  = parent_guard.GetPageId();
  // 当前正在处理的 internal page 不能只留裸指针，要让 guard 持有它
  WritePageGuard current_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  page_id_t child_id = current_guard.GetPageId();
  parent = current_guard.AsMut<InternalPage>();

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
      // 当前 parent 合并进左兄弟，所以被删除的是 current_guard 持有的页
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

      current_guard.Drop();
      auto ok = bpm_->DeletePage(child_id);
      BUSTUB_ASSERT(ok, "failed to delete merged internal page");
    }
    else {
      // 右兄弟合并进当前 parent， 所以被删除的是右兄弟
      page_id_t right_page_id = grand->ValueAt(pidx + 1);
      auto rs_guard = bpm_ -> WritePage(right_page_id);
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

      rs_guard.Drop();
      auto ok = bpm_->DeletePage(right_page_id);
      BUSTUB_ASSERT(ok, "failed to delete merged right internal page");
    }

    child_id = grand_guard.GetPageId();
    current_guard = std::move(grand_guard);
    ctx.write_set_.pop_back();
    parent = current_guard.AsMut<InternalPage>();


  }

  // 如果根节点只剩一个 child， 收缩树高
  if(ctx.write_set_.empty() && parent->GetSize() == 1) {
    page_id_t old_root_page_id = current_guard.GetPageId();
    ctx.header_page_ -> AsMut<BPlusTreeHeaderPage>() -> root_page_id_ = parent->ValueAt(0);
    ctx.root_page_id_ = parent->ValueAt(0);
    current_guard.Drop();
    auto ok = bpm_->DeletePage(old_root_page_id);
    BUSTUB_ASSERT(ok, "failed to delete shrunk root internal page");
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
  // 同样用螃蟹式加锁：先锁孩子再放父亲，避免下降途中被并发的合并/换根抽走脚下的页

  std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
  page_id_t current_page_id_ = protector->As<BPlusTreeHeaderPage>()->root_page_id_;

  // 如果 root 是 INVALID_PAGE_ID ， 直接返回 END()
  if(current_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  std::optional<ReadPageGuard> cur = bpm_->ReadPage(current_page_id_);
  protector.reset();  // 拿到 root 读锁之后才放 header

  // 从 root 开始，一直往左走（ValueAt(0) 是最左孩子），直到叶子页
  while(!cur->As<BPlusTreePage>()->IsLeafPage()) {
    auto internal_page = cur->As<InternalPage>();
    current_page_id_ = internal_page->ValueAt(0);
    auto child = bpm_->ReadPage(current_page_id_);
    cur = std::move(child);
  }

  return INDEXITERATOR_TYPE(bpm_, std::move(*cur), current_page_id_, 0, comparator_);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
  page_id_t current_page_id = protector->As<BPlusTreeHeaderPage>()->root_page_id_;

  if(current_page_id == INVALID_PAGE_ID) {
    return End();
  }

  std::optional<ReadPageGuard> cur = bpm_->ReadPage(current_page_id);
  protector.reset();

  while(!cur->As<BPlusTreePage>()->IsLeafPage()) {
    auto internal = cur->As<InternalPage>();
    current_page_id = internal->ValueAt(ChildIndex(internal, key));
    auto child = bpm_->ReadPage(current_page_id);
    cur = std::move(child);
  }

  auto leaf = cur->As<LeafPage>();

  // 在叶子里做 lower_bound, 找到第一个 >= key 的位置。
  // 允许返回 GetSize()（key 比本页所有 key 都大），迭代器构造时会自动跨到下一页。
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

  return INDEXITERATOR_TYPE(bpm_, std::move(*cur), current_page_id, left, comparator_);
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
  // 只是读一个字段，用读锁就够了（原来用 WritePage 会白拿排他锁，把并发读全串行化）
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
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
