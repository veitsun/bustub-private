# BusTub P1 / P2 实现笔记

> 记录 Project 1（Buffer Pool Manager）与 Project 2（B+Tree Index）已完成的全部工作：
> 涉及文件、设计思路、关键实现细节、以及遗留缺口与已知风险。
>
> 代码基线：`master`，`src/` 目录。
> 测试状态：**P1 16/16、P2 18/18 全部通过**（ASan 开启）。
>
> ⚠️ 文中带「最初写错了 / 已修复」标注的段落是**刻意保留**的：
> 那些结论在第一版笔记里是反的，被并发测试打脸后才纠正。保留错误路径比只留正确答案更有价值。

---

## 目录

- [Project 1 — Buffer Pool Manager](#project-1--buffer-pool-manager)
  - [1.1 整体架构](#11-整体架构)
  - [1.2 Task #1 DiskScheduler](#12-task-1-diskscheduler)
  - [1.3 Task #2 ArcReplacer](#13-task-2-arcreplacer)
  - [1.4 Task #3 PageGuard](#14-task-3-pageguard)
  - [1.5 Task #4 BufferPoolManager](#15-task-4-bufferpoolmanager)
  - [1.6 P1 并发与加锁约定](#16-p1-并发与加锁约定)
  - [1.7 P1 遗留缺口与风险](#17-p1-遗留缺口与风险)
- [Project 2 — B+Tree Index](#project-2--btree-index)
  - [2.1 整体架构](#21-整体架构)
  - [2.2 页面层辅助方法](#22-页面层辅助方法)
  - [2.3 查找 GetValue（含latch crabbing）](#23-查找-getvalue)
  - [2.4 插入 Insert 与 InsertIntoParent](#24-插入-insert-与-insertintoparent)
    - [CrabDownToLeaf 与 protector 角色](#crabdowntoleaf乐观路径的公共下降逻辑)
    - [★ 乐观 vs悲观：protector 与 write_set_ 的分工](#乐观-vs-悲观protector-与-ctxwrite_set_-的分工)
  - [2.5 删除 Remove](#25-删除-remove)
  - [2.6 迭代器 IndexIterator](#26迭代器-indexiterator)
  - [2.7 Tombstone 懒删除](#27-tombstone-懒删除)
  - [2.8 P2 遗留缺口与风险](#28-p2-遗留缺口与风险)
- [附：文件改动清单](#附文件改动清单)

---

# Project 1 — Buffer Pool Manager

## 1.1 整体架构

```
用户代码
   │bpm->ReadPage(pid) / WritePage(pid)
   ▼
BufferPoolManager ──── page_table_ : page_id → frame_id
   │                   frames_     : frame_id → shared_ptr<FrameHeader>
   │                   free_frames_:空闲 frame 链表
   │                   bpm_latch_  : shared_ptr<std::mutex>（全局元数据锁）
   ├──► ArcReplacer      决定淘汰哪个 frame
   └──► DiskScheduler    异步磁盘 IO（后台线程 + 请求队列）
   │
   ▼
返回 ReadPageGuard / WritePageGuard（RAII，持有 frame 的 rwlatch_）
```

四个组件的职责切分：

| 组件 | 职责 | 文件 |
|---|---|---|
| `DiskScheduler` | 把读写请求投递给后台线程，异步执行，用 `promise/future` 同步 | `src/storage/disk/disk_scheduler.cpp` |
| `ArcReplacer` | 页面替换策略（ARC），决定哪个 frame 可以被淘汰 | `src/buffer/arc_replacer.cpp` |
| `PageGuard` | RAII 管理页面读写锁 + pin count | `src/storage/page/page_guard.cpp` |
| `BufferPoolManager` | 编排上面三者，实现 page 的换入换出 | `src/buffer/buffer_pool_manager.cpp` |

> 注意：本项目走的是 **ARC 路线**，`src/buffer/lru_k_replacer.cpp` 未实现且未被使用，
> `BufferPoolManager` 构造函数里注入的是 `std::make_shared<ArcReplacer>(num_frames)`。

---

## 1.2 Task #1 DiskScheduler

文件：`src/storage/disk/disk_scheduler.cpp`

### 构造 / 析构

```cpp
DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  background_thread_.emplace([&] { StartWorkerThread(); });
}
```

- 构造时立刻拉起一条后台线程跑 `StartWorkerThread()`。
- 析构时向队列投递一个 `std::nullopt` 作为**毒丸（poison pill）**，后台线程看到它就 `return`，然后 `join()`。这是本任务里最容易忽略的退出路径。

### Schedule

```cpp
for (auto &request : requests) {
  request_queue_.Put(std::make_optional(std::move(request)));
}
```

- 关键点：**必须用 `std::move`**。`DiskRequest` 内部含`std::promise`（不可拷贝）以及指向 frame 数据的指针，拷贝语义既不合法也很昂贵。

### StartWorkerThread

```cpp
while (true) {
  auto request_opt = request_queue_.Get();
  if (!request_opt.has_value()) { return; }       // 毒丸 → 退出
  auto request = std::move(request_opt.value());
  if (request.is_write_) { disk_manager_->WritePage(request.page_id_, request.data_); }
  else                   { disk_manager_->ReadPage(request.page_id_, request.data_); }
  request.callback_.set_value(true);              // 唤醒等待的前台线程
}
```

- 单一后台线程串行处理所有请求，天然避免了同页并发 IO 的乱序问题。
- `set_value(true)` 一定要在 IO 真正完成之后调用，否则前台 `future.get()` 会提前返回、读到脏数据。

---

## 1.3 Task #2 ArcReplacer

文件：`src/buffer/arc_replacer.cpp`、`src/include/buffer/arc_replacer.h`

### ARC 的四个列表

ARC（Adaptive Replacement Cache）的核心是「用两对列表同时跟踪**最近性**和**频率**」：

| 列表 | 论文记号 | 含义 | 存储内容 |
|---|---|---|---|
| `mru_` | T1 | 在缓存中，**只被访问过一次** | `frame_id_t` |
| `mfu_` | T2 | 在缓存中，**被访问过 ≥2 次** | `frame_id_t` |
| `mru_ghost_` | B1 | 从 T1 淘汰出去的**历史记录**（只留page_id，无数据） | `page_id_t` |
| `mfu_ghost_` | B2 | 从 T2 淘汰出去的历史记录 | `page_id_t` |

配套两个哈希索引，实现 O(1) 定位：

```cpp
std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>> alive_map_;  // mru_ + mfu_
std::unordered_map<page_id_t,  std::shared_ptr<FrameStatus>> ghost_map_;  // 两个 ghost 列表
```

`FrameStatus` 里除了 `page_id_ / frame_id_ / evictable_ / arc_status_`，还存了 **一个指向自己在链表中位置的迭代器 `it_`**。这是本实现的关键设计：有了它，`std::list::erase(it_)` 就是 O(1)，不需要遍历链表找节点。

### 自适应参数

```cpp
size_t mru_target_size_{0};   // 论文里的 p，mru_ 的目标长度
size_t replacer_size_;        // 论文里的 c，总容量
size_t curr_size_{0};         // 当前 evictable 的条目数（= Size() 的返回值）
```

`mru_target_size_` 是 ARC「自适应」的全部秘密：

- **命中 `mru_ghost_`** → 说明「只访问一次的页」被淘汰得太快，工作集偏最近性 → `mru_target_size_++`（上限 `replacer_size_`）
- **命中 `mfu_ghost_`** → 说明「频繁访问的页」被淘汰亏了，工作集偏频率 → `mru_target_size_--`（下限 0）

### RecordAccess：状态机

这是整个 replacer 的心脏，按「当前页处于哪个列表」分四种情况：

```
Case 1  命中 mru_ / mfu_ （alive_map_ 里有）
        → 从原列表 erase，插到 mfu_ 头部，状态置为 MFU
        （MRU→MFU 是「晋升」；MFU→MFU 是刷新最近性）

Case 2  命中 mfu_ghost_
        → mru_target_size_--      （偏向频率）
        → 从 mfu_ghost_ / ghost_map_ 删除，新建条目插入 mfu_ 头部

Case 3  命中 mru_ghost_
        → mru_target_size_++      （偏向最近性）
        → 从 mru_ghost_ / ghost_map_ 删除，新建条目插入 mfu_ 头部
        （注意：ghost 命中意味着这是"第二次"访问，所以进 mfu_ 而不是 mru_）

Case 4  全都没命中（全新页）
        → 4A: |mru_| + |mru_ghost_| == c→ 裁剪 mru_ghost_ 尾部
        → 4B: 四个列表总和 >= 2c                 → 裁剪 mfu_ghost_ 尾部
        → 新建条目插入 mru_ 头部，状态 MRU
```

Case 4 的两个裁剪分支是 ARC 保持「总记账条目 ≤ 2c」的不变式，容易漏。

### Evict：ARC 的 REPLACE 操作

```cpp
if (mru_.size() >= mru_target_size_) {
  // mru_ 超过目标 → 从 mru_ 尾部淘汰 → 进 mru_ghost_
  // 若 mru_ 全被 pin → 退化到 mfu_ 尾部淘汰 → 进 mfu_ghost_
} else {
  // 反之从 mfu_ 尾部淘汰 → 进 mfu_ghost_
  // 若 mfu_ 全被 pin → 退化到 mru_ 尾部淘汰 → 进 mru_ghost_
}
// 两侧都被 pin 住 → return std::nullopt
```

相比原论文的两处改动（writeup 已说明，也是本实现遵循的）：

1. `|T1| == p` 时不再看「最后一次访问」，直接按上面的判定走（论文说这个决策是任意的）。
2. **跳过 non-evictable 条目**。缓冲池场景下 frame 会被 pin 住，一侧全pin 时必须能退化到另一侧，否则会假性OOM。

用 `rbegin()/rend()` 反向遍历找「尾部方向上第一个 evictable」，正是对应「淘汰 LRU 端」。

### Dieout：抽出的淘汰公共逻辑

```cpp
void ArcReplacer::Dieout(alive_map_::const_iterator it,
                         std::list<frame_id_t> &list_,
                         std::list<page_id_t> &list_ghost_);
```

一次淘汰要做的 5 件事都收在这里，避免 `Evict()` 四个分支重复：

1. `list_.erase(it->second->it_)` —— 从 alive 链表摘除（O(1)）
2. `list_ghost_.emplace_front(page_id)` —— 进对应 ghost 列表头部
3. 状态转移 `MRU → MRU_GHOST` / `MFU → MFU_GHOST`，新建 `FrameStatus` 注册进 `ghost_map_`
4. `alive_map_.erase(it)`，`curr_size_--`
5. **ghost 列表长度超过 `replacer_size_` 时，从尾部丢掉最旧的一条**（同时清`ghost_map_`）

### SetEvictable / Remove / Size

- `SetEvictable`：在 `alive_map_` 里找不到 → `throw Exception`（frame_id 无效）；状态没变化 → 直接 return（**不能重复增减 `curr_size_`**）；否则按方向 `curr_size_++/--`。
- `Remove`：强制移除。找不到 → 静默 return；找到但non-evictable → `throw Exception`；否则按 `arc_status_` 从 `mru_`/`mfu_` 里 erase，清 `alive_map_`，`curr_size_--`。
  - 与 `Evict()` 的区别：**Remove 不进 ghost 列表**（页被彻底删掉了，留历史记录没意义）。
- `Size()`：返回 `curr_size_`，即「evictable 条目数」，不是「总条目数」。

所有对外接口用 `std::lock_guard<std::mutex> lk(latch_)` 保护。

---

## 1.4 Task #3 PageGuard

文件：`src/storage/page/page_guard.cpp`

### 设计要点

`ReadPageGuard` / `WritePageGuard` 是 RAII 句柄，持有两类资源：

1. **页面读写锁**：`frame_->rwlatch_`（`std::shared_mutex`）
   - `ReadPageGuard` → `std::shared_lock`（多读并存）
   - `WritePageGuard` → `std::unique_lock`（独占）
2. **pin count**：guard 存活期间该frame 不可被淘汰

```cpp
// 构造：加锁 + 标记有效
page_lock_ = std::shared_lock<std::shared_mutex>(frame_->rwlatch_);   // Read
page_lock_ = std::unique_lock<std::shared_mutex>(frame_->rwlatch_);   // Write
is_valid_= true;
```

### 移动语义（7 个字段）

移动构造与移动赋值都要搬运 7 个字段：`page_id_ / frame_ / replacer_ / bpm_latch_ / disk_scheduler_ / page_lock_ / is_valid_`，然后 **`that.is_valid_ = false`** 让源对象失效。

- `page_lock_` 的**所有权**也必须 move，否则锁会在源对象析构时被提前释放。
- 移动赋值里先 `if (this == &that) return *this;` 自赋值检查，再 `Drop()` 释放自己已持有的资源，再接管。

### Drop：释放顺序是重点

```cpp
void Drop() {
  if (!is_valid_) { return; }     // 防 double free
  is_valid_ = false;

  {
    std::lock_guard<std::mutex> lk(*bpm_latch_);   // ① 先拿bpm_latch_
    frame_->pin_count_--;                          // ② 归还 pin
    if (frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
    page_lock_.unlock();                           // ③ 最后才放页面锁
  }
}
```

**不变式：谁拿到页面锁，谁就看到 `pin_count_` 已经是干净的。**

> ⚠️ 这里最初写反了（先放页面锁、再拿 `bpm_latch_` 减 pin），当时的理由是「BPM 是先持 `bpm_latch_` 再抢页面锁，反过来会死锁」。
> 后来被 `b_plus_tree_concurrent_test.DeleteTest1` 打脸：随机崩在
> `BUSTUB_ASSERT(ok, "failed to delete merged leaf")`，3 次里能复现 2 次。
>
> 原因是旧顺序留了一个窗口：`page_lock_.unlock()` 一旦完成，别的线程立刻能抢到页面锁
> 并认为自己独占了这一页，可此时前一个持有者还没执行减 pin，`pin_count_` 仍是 1，
> 于是它调用 `DeletePage()` 会因`pin_count > 0` 失败。
>
> 那么新顺序会不会死锁？不会。BPM 里唯一「持 `bpm_latch_` 再去要页面锁」的地方是
> `CheckedRead/WritePage` 的 Case B / Case C，而那两处的 frame 分别来自 `free_frames_`
> 和 `Evict()`（要求 `pin_count == 0`）。配合新顺序，**`pin_count == 0` 蕴含页面锁已经放掉**，
> 所以那里绝不会真的阻塞在页面锁上，构不成环。
>
> 换句话说：旧顺序不是「更安全」，只是把 bug 藏得更深。

`pin_count_` 归零时才通知 replacer 该frame 变为可淘汰，这也是 `curr_size_` 保持正确的关键。
注意 `SetEvictable(true)` 与 `page_lock_.unlock()` 之间不会被 `Evict()` 插入——
`Evict()` 需要 `bpm_latch_`，而我们全程持有它。

### GetDataMut 与脏页标记

```cpp
auto WritePageGuard::GetDataMut() -> char * {
  frame_->is_dirty_ = true;      // 只要拿到可变指针就悲观地标脏
  return frame_->GetDataMut();
}
```

`ReadPageGuard` 没有 `GetDataMut()`，从类型层面保证读路径不会把页标脏。

### Flush

```cpp
void WritePageGuard::Flush() {
  if (!is_valid_ || !frame_->is_dirty_) { return; }
  frame_->is_dirty_ = false;                    // 先清标记
  auto promise = disk_scheduler_->CreatePromise();
  auto future  = promise.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back({true, frame_->GetDataMut(), page_id_, std::move(promise)});
  disk_scheduler_->Schedule(requests);
  future.get();                                 // 同步等落盘
}
```

因为持有独占写锁，flush 期间没人能改这个页，所以 `is_dirty_ = false` 是安全的。

`ReadPageGuard::Flush()` 目前是**空实现**，理由写在注释里：持共享读锁的页面理论上不该是脏的。见「遗留缺口」。

---

## 1.5 Task #4 BufferPoolManager

文件：`src/buffer/buffer_pool_manager.cpp`

### NewPage

```cpp
return next_page_id_.fetch_add(1);
```

`next_page_id_` 是 `std::atomic<page_id_t>`，单调递增分配全新 page_id。不做任何 IO —— 页是「逻辑上存在」，第一次 `ReadPage/WritePage` 时才真正落到 frame。

### CheckedWritePage / CheckedReadPage（P1 的核心）

两者逻辑几乎完全对称（只有最后返回的 guard 类型不同），都分**三种情况**：

**Case A —— 页已在buffer pool 中（命中）**

```cpp
auto frame_id = page_table_[page_id];
frame->pin_count_.fetch_add(1);
replacer_->RecordAccess(frame_id, page_id, access_type);
replacer_->SetEvictable(frame_id, false);
lk.unlock();                                   // 先放 bpm_latch_
return WritePageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_, disk_scheduler_);
```

无需任何 IO。**必须先 `lk.unlock()` 再构造 guard**，因为 guard 构造函数内部会去抢 `frame_->rwlatch_`，如果此时还捏着 `bpm_latch_`，一旦被别的线程阻塞就会连带堵死整个 BPM。

**Case B —— 未命中，但有空闲 frame**

```cpp
auto frame_id = free_frames_.front(); free_frames_.pop_front();
frame->Reset();                                // 清零数据 + pin_count + is_dirty
frame->page_id_ = page_id;
page_table_[page_id] = frame_id;               // 建映射
frame->pin_count_.store(1);
replacer_->RecordAccess(...); replacer_->SetEvictable(frame_id, false);
// ★ 全程不放 bpm_latch_，直接做磁盘读，同步等完成
```

**Case C —— 未命中且无空闲 frame（最复杂）**

```cpp
auto victim = replacer_->Evict();
if (!victim.has_value()) { return std::nullopt; }   // 真的 OOM

if (frame->is_dirty_) {
  auto old_page_id = frame->page_id_;
  frame->is_dirty_ = false;   // 先清标记，避免并发重复写回
  ... Schedule(write old_page_id) ; future.get() ...   // ★ 不放 bpm_latch_
}
page_table_.erase(frame->page_id_);   // 删旧映射
frame->Reset();
frame->page_id_ = page_id;
page_table_[page_id] = frame_id;      // 建新映射
frame->pin_count_.store(1);
replacer_->RecordAccess(...); replacer_->SetEvictable(frame_id, false);
... Schedule(read page_id) ; future.get() ...        // ★ 不放 bpm_latch_
return WritePageGuard(...);
```

四个实现要点：

1. **锁的类型选`std::unique_lock` 而不是 `std::lock_guard`**。Case A 需要在构造 guard 之前 `unlock()`。
2. **`SetEvictable(frame_id, false)` 要放在 `RecordAccess` 之后**。`RecordAccess` 才刚把条目插进 `alive_map_`，顺序反了会抛「frame_id not found」。
3. **只有 Case A 可以放锁**。命中路径不做 IO，且必须在构造 guard 之前 `lk.unlock()`，否则捏着 `bpm_latch_` 去抢页面锁会连带堵死整个 BPM。
4. **Case B / Case C 全程持有 `bpm_latch_`，换页 IO 也在锁内做。** 见下面的说明——这里最初为了「IO 不占锁」中途放开了锁，是一个会静默丢数据的 bug。

> ⚠️ **换页竞态：本项目踩过的最隐蔽的一个坑**
>
> 最初 Case B / Case C 为了不让磁盘 IO 占着全局锁，在 IO 前后做了 `lk.unlock()` / `lk.lock()`。
> 这留下两个窗口：
>
> **窗口1（Case C 脏页写回期间放锁）**：此时 frame 已被 `Evict()` 从 replacer 摘掉、
> `pin_count` 是 0，但 `page_table_` 里 `old_page_id → frame_id` 的映射**还在**。
> 别的线程请求 `old_page_id` 会命中 `page_table_`、拿到这个 frame 的 guard 并开始读写；
> 我们回来后 `frame->Reset()` 把数据清零并换成新页，**对方的写入被静默丢弃**。
>
> **窗口 2（Case B / C 读入新页前放锁）**：`page_table_[page_id]` 已经装好了，
> 但 frame 里还是旧数据 / 全零，而我们**尚未持有 `frame->rwlatch_`**（guard 是 IO 之后才构造的）。
> 别的线程命中 `page_table_` 后能直接读到未初始化的内容。
>
> 症状：`b_plus_tree_concurrent_test` 的 `InsertTest2` / `MixTest1` **并发插入随机丢 key**，
> 而单线程、以及缓冲池开大到不触发淘汰时都完全正常。定位方法见1.7 的「定位手法」。
>
> 修复：换页全程不放 `bpm_latch_`。代价是这条慢路径被串行化。
> `DiskScheduler` 的后台线程不碰 `bpm_latch_`，所以锁内 `future.get()` 不会死锁。

`WritePage` / `ReadPage` 是上面两个函数的 unwrap 包装，`nullopt` 时打印信息并 `std::abort()`。

### DeletePage

```cpp
if (页不在 page_table_) {  DeallocatePage(page_id); return true; }   // 只在磁盘上
if (frame->pin_count_.load() > 0) { return false; }                  // 有人在用，删不掉
page_table_.erase(it);
replacer_->Remove(frame_id);      // 从 replacer 摘除（不进 ghost）
frame->Reset();
free_frames_.push_back(frame_id); // 归还到空闲链表
bpm_latch_->unlock();             // 放锁后再做磁盘 deallocate
disk_scheduler_->DeallocatePage(page_id);
```

关键：**删页不需要写回脏数据**（都要删了写回没意义），frame 要归还 `free_frames_`，且 `DeallocatePage` 在放锁之后。

### FlushPage 系列

| 函数 | 是否持 `bpm_latch_` | 说明 |
|---|---|---|
| `FlushPageUnsafe` | 否 | 不加锁版本，供`FlushAllPagesUnsafe` 调用 |
| `FlushPage` | 是（`lock_guard`） | 加锁版本 |
| `FlushAllPagesUnsafe` | 先加锁**快照** page_id 列表，放锁后逐个 flush | 避免遍历时 `page_table_` 被并发修改 |
| `FlushAllPages` | 同上，逐个调 `FlushPage` | |

单页 flush 的共同模式：`is_dirty_` 为真才发写请求，且**先清 `is_dirty_` 再发请求**。

「先快照再逐个 flush」的写法避开了「一边遍历 `page_table_` 一边做 IO 导致迭代器失效」的坑。

### GetPinCount

`page_table_` 里没有 → `std::nullopt`；有→ `frames_[frame_id]->pin_count_.load()`。因为 `pin_count_` 是原子量，读它本身不需要 frame 锁。

---

## 1.6 P1 并发与加锁约定

本项目全局遵守的锁层级（**从外到内，不可逆序**）：

```
bpm_latch_  (std::mutex，保护 page_table_ / free_frames_ / frame 元数据)
    ↓
frame_->rwlatch_  (std::shared_mutex，保护页面数据)
```

- `ArcReplacer::latch_` 是叶子锁，在持有 `bpm_latch_` 时获取（`RecordAccess`/`SetEvictable`/`Evict`/`Remove`），不再向下嵌套。
- `PageGuard::Drop()` 是「持 `bpm_latch_` → 归还 pin → 放`rwlatch_`」。这与 BPM 的获取顺序同向，不构成环，理由见 1.4 的说明。
- **只有 Case A（命中，无 IO）会在构造 guard 前放开 `bpm_latch_`。** Case B / Case C 的换页 IO 在锁内完成，这是正确性要求，不是疏漏。

两条支撑正确性的不变式，值得单独记住：

1. **`pin_count == 0` ⇒ 该frame 的 `rwlatch_` 已经空闲。**（由 `Drop()` 的新顺序保证）
   正因为如此，Case B / Case C 拿到的 frame 一定没人持有页面锁，
   所以「持 `bpm_latch_` 时构造 guard」不会真的阻塞。
2. **一个 frame 只要还能通过 `page_table_` 被找到，它的内容就是完整可用的。**
   （由「换页 IO 在锁内完成」保证）

---

## 1.7 P1 遗留缺口与风险

###缺口

1. **`ReadPageGuard::Flush()` 是空实现**（`page_guard.cpp`）。
   当前依赖「读页不会脏」这一假设。但脏页是可能在 `WritePageGuard` 释放后仍留在 frame 里的，此时另一个线程拿 `ReadPageGuard` 再调 `Flush()` 就会静默什么都不做。若 Gradescope 有对应用例，需要补上「发写请求 + 清 `is_dirty_`」的逻辑（注意共享锁下清 `is_dirty_` 需要额外考虑同步）。

2. **`src/buffer/lru_k_replacer.cpp` 完全未实现**。若课程/评测要求提交 LRU-K 版本，需要另行补齐；当前 BPM 只依赖 `ArcReplacer`。

3. **换页被串行化了**。修掉换页竞态的代价是所有 eviction IO 串行执行。
   功能测试全过，但 leaderboard 的吞吐会受影响。要恢复并发度需要引入「页面 IO 进行中」的状态机：
   给 frame 加一个 `io_in_progress_` 标记 + 条件变量，让请求同一页的线程等待而不是看到半成品，
   这样 IO 才能重新挪出 `bpm_latch_`。属于比较大的改造，目前没做。

### 已修复（保留记录，避免重复踩坑）

1. ~~**Case C 释放锁做写回 IO 存在竞态窗口**~~ → 已修，见 1.5 的「换页竞态」说明。
   这条当初是作为「风险点，建议复核」记下的，后来果然就是并发丢 key 的真凶。
2. ~~**`PageGuard::Drop()` 先放页面锁再减 pin**~~ → 已修，见 1.4。

### 定位手法（值得复用）

并发丢数据这类 bug 很难从代码上看出来，这次是靠**两个对照实验**定位的，方法可以复用：

1. **单线程 vs 多线程**：写一个探针，用单线程复现多线程的插入顺序（跨步、交替、顺序），
   并用 `IsTreeValid()` 校验结构。单线程全对 → 排除算法逻辑，锁定并发。
2. **小缓冲池 vs 大缓冲池**：同样的多线程负载，`bpm_size=50`（频繁淘汰）会丢 key，
   `bpm_size=5000`（几乎不淘汰）完全正常 → 直接把范围收缩到淘汰路径。

第2 步是决定性的一击：它把「B+Tree 加锁协议有问题」和「缓冲池换页有问题」这两个
看起来都说得通的猜想干净地区分开了。

### 风险点（建议复核）

1. **`ArcReplacer::Size()` 没有加 `latch_`**。
   其他所有接口都用 `lock_guard` 保护了 `curr_size_`，唯独 `Size()` 裸读，在 TSan 下会被报成data race。加一行 `std::lock_guard` 即可。

2. **`FrameStatus::it_` 的类型双关（type punning）**。
   `it_` 声明为 `std::list<frame_id_t>::iterator`，但在 `Dieout()` 中被赋值为 `list_ghost_.begin()`（类型是 `std::list<page_id_t>::iterator`）。
   目前能编译通过纯粹因为 `frame_id_t` 和 `page_id_t` **都是 `int32_t`**（见 `common/config.h:46-48`），两个模板实例化成了同一个类型。这是**依赖巧合的写法**，一旦哪天 `page_id_t` 改成 `int64_t` 就会直接编译失败。建议用 `std::variant` 或拆成两个成员。

3. **`GetPinCount` 在 `unlock()` 之后才解引用迭代器**。
   ```cpp
   bpm_latch_->unlock();
   return frames_[it->second]->pin_count_.load();   // it 可能已失效
   ```
   如果另一个线程在这两行之间执行了 `DeletePage`，`it` 就是悬垂迭代器。应该在放锁**之前**把 `it->second` 取出到局部变量。

4. **`Evict()` 的判定用 `mru_.size() >= mru_target_size_`**，论文里是 `|T1| >= max(1, p)`。`mru_target_size_` 为 0 时行为略有差异，通常不影响正确性但会影响命中率（leaderboard 相关）。

---

# Project 2 — B+Tree Index

## 2.1 整体架构

```
BPlusTreeHeaderPage           只存 root_page_id_（root 会变，需要一个固定入口）
        │
        ▼
BPlusTreeInternalPage         key_array_[1..size-1] 有效，page_id_array_[0..size-1] 全有效
        │                （key[0] 是占位；value[0] 是"最左孩子"）
        ▼
BPlusTreeLeafPage             key_array_ / rid_array_ 一一对应
        │  next_page_id_ ──►  next_page_id_ ──►  ...（叶子层单向链表，支撑范围扫描）
        ▼
IndexIterator                沿叶子链表推进，跳过 tombstone
```

三个页类型共享基类 `BPlusTreePage`（`page_type_ / size_ / max_size_`）。

内部页的下标约定是 P2 最容易出错的地方，本实现统一遵守：

- **`KeyAt(0)` 无效**，所有内部页二分查找 `left` 从 **1** 开始。
- **`ValueAt(0)` 有效**，代表「所有 < 第一个有效 key 的那棵子树」，所以 `child_idx` 初值为 **0**。

---

## 2.2 页面层辅助方法

### `src/storage/page/b_plus_tree_page.cpp`

| 方法 | 实现 |
|---|---|
| `IsLeafPage()` | `page_type_ == IndexPageType::LEAF_PAGE` |
| `SetPageType` / `GetSize` / `SetSize` / `GetMaxSize` / `SetMaxSize` | 直接读写成员 |
| `ChangeSizeBy(int)` | `size_ += amount`（增删都走它，避免手写±1 出错） |
| `GetMinSize()` | 叶子 `max/2`；内部 `(max+1)/2` |

### `src/storage/page/b_plus_tree_internal_page.cpp`

| 方法 | 说明 |
|---|---|
| `Init(max_size)` | 置 `INTERNAL_PAGE`类型、`max_size`、`size = 0` |
| `KeyAt` / `SetKeyAt` / `ValueAt` | 数组读写 |
| `SetValueAt(index, value)` | **自行新增**，分裂/合并时需要单独改 child 指针 |
| `ValueIndex(value)` | 线性扫描找 `page_id_array_` 中某个 child 的下标，分裂/合并定位父槽位时用|
| `RemoveAt(index)` | **自行新增**，key 和 value 一起左移，`ChangeSizeBy(-1)` |

### `src/storage/page/b_plus_tree_leaf_page.cpp`

| 方法 | 说明 |
|---|---|
| `Init(max_size)` | 置 `LEAF_PAGE`、`size = 0`、`max_size`、`next_page_id_ = INVALID_PAGE_ID`、**`num_tombstones_ = 0`** |
| `GetNextPageId` / `SetNextPageId` | 叶子链表指针 |
| `KeyAt` / `ValueAt` | 返回值拷贝 |
| `KeyAtRef` / `ValueAtRef` | 返回 `const&`，供 `IndexIterator::operator*` 返回引用对 |
| `SetKeyValueAt(index, key, value)` | **自行新增**，原地覆写一对 KV，**不含** tombstone 记账（split / 复活时用） |
| `InsertAt(index, key, value)` | **自行新增**，右移腾位 + `ChangeSizeBy(1)` + **同步修正 tombstone 下标** |
| `RemoveAt(index)` | **自行新增**，左移 + `ChangeSizeBy(-1)` + **同步修正 tombstone 下标** |
| `GetTombstones()` | 框架给定，把 `tombstones_[]` 里的下标翻译成 key 列表 |
| tombstone 接口一组 | **自行新增**，见 2.7 |

> 区分 `SetKeyValueAt` 和 `InsertAt` 很重要：前者是「裸写」，后者带记账。
> split 时先用 `SetKeyValueAt` 批量重排entry、`SetSize()` 定尺寸，最后才`SetTombstoneIndexes()`
> 统一重建记账；日常单条插入则一律走 `InsertAt`。混用会让 tombstone 下标错位。

---

## 2.3 查找 GetValue

```
1. ReadPage(header_page_id_) → root_page_id_
   root == INVALID_PAGE_ID → return false（空树）
2. 螃蟹式下降（先锁孩子再放父亲）：
   - IsLeafPage()? → 叶子内二分找 key
       找不到 → false
       找到   → IsTombstoned(found)? 视为不存在 → false
       否则   → result->push_back(ValueAt(found)); return true
   - 否则是内部页 → ChildIndex() 二分找孩子，锁住它，再放开当前页
```

内部页的二分已经抽成`ChildIndex()`（原来在 `GetValue` / `Insert` ×2 / `Remove` / `Begin(key)` 里重复了 5 遍）：

```cpp
auto ChildIndex(const InternalPage *internal, const KeyType &key) const -> int {
  int child_idx = 0;                     // 默认走最左孩子
  int left = 1, right = internal->GetSize() - 1;   // key[0] 无效，从 1 开始
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(internal->KeyAt(mid), key) <= 0) { child_idx = mid; left = mid + 1; }
    else                { right = mid - 1; }
  }
  return child_idx;
}
```

语义：找**最后一个 `key[i] <= 目标 key`** 的 `i`，因为内部页的 `key[i]` 是「子树 i 的最小 key」。

### 螃蟹加锁（latch crabbing）—— 这里最初写错了

> ⚠️笔记的第一版写的是「下降过程中 guard 是循环体内的局部变量，随循环迭代自然析构，
> **实现了螃蟹加锁的效果**」。这是**错的**。看清楚作用域：
>
> ```cpp
> while (true) {
>   auto guard = bpm_->ReadPage(current_page_id);   // 拿子节点锁
>   ...
>   current_page_id = internal->ValueAt(child_idx);
> }   // ← guard 在这里析构，父锁才被释放
> ```
>
> `guard` 是**循环体内**的局部变量，每轮迭代结束就析构。真实顺序是
> **「先放父锁 → 再拿子锁」**，中间存在一个不持任何锁的空窗。
> 正确的 crabbing 必须是「先拿子锁 → 再放父锁」，需要同时持有两个 guard。
>
> `GetValue` 当时侥幸没出事，是因为它把header 的读锁一直持到函数返回
> （函数作用域局部变量，从没`Drop()`），而所有结构性写操作都要 header 写锁，
> 于是 header 锁意外充当了一把全局读写锁。但**乐观Insert 用了同样的下降模式却主动放掉了 header 读锁**，
> 漏洞就从那里漏出来了。

现在的写法用 `std::optional` 显式控制释放时机：

```cpp
std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
page_id_t cur_id = protector->As<BPlusTreeHeaderPage>()->root_page_id_;
if (cur_id == INVALID_PAGE_ID) { return false; }

std::optional<ReadPageGuard> cur = bpm_->ReadPage(cur_id);
protector.reset();          // 拿到 root 读锁之后，才放开 header

while (true) {
  if (cur->As<BPlusTreePage>()->IsLeafPage()) { ...查找... }
  auto internal = cur->As<InternalPage>();
  cur_id = internal->ValueAt(ChildIndex(internal, key));
  auto child = bpm_->ReadPage(cur_id);   // ① 先锁孩子
  cur = std::move(child);                // ② 移动赋值内部先 Drop() 父亲，再接管孩子
}
```

`cur = std::move(child)` 依赖 `ReadPageGuard::operator=(&&)` 的实现顺序：先 `Drop()` 自己（父亲），
再接管 `that`（孩子）。而 `child` 早在赋值之前就已经加锁完成 —— 这正是 crabbing 要的顺序。

`Begin()` / `Begin(key)` 也改成了同样的模式。

---

## 2.4 插入 Insert 与 InsertIntoParent

### 两条路径

**路径 1：乐观路径（optimistic）**

用 `CrabDownToLeaf()` 螃蟹式下降（全程读锁），到叶子时升级成写锁，只写**一个页**：

```cpp
page_id_t leaf_page_id; bool leaf_is_root;
auto leaf_guard_opt = CrabDownToLeaf(key, &leaf_page_id, &leaf_is_root);
if (leaf_guard_opt.has_value()) {
  auto leaf = leaf_guard_opt->template AsMut<LeafPage>();
  int insert_pos = 0;
  int found = FindKeyInLeaf(leaf, key, &insert_pos);
  if (found >= 0) {
    if (leaf->IsTombstoned(found)) {          // tombstone 复活
      leaf->ClearTombstoneAt(found);
      leaf->SetKeyValueAt(found, key, value);
      return true;
    }
    return false;                             // 真重复键
  }
  if (leaf->GetSize() < leaf_max_size_) {
    leaf->InsertAt(insert_pos, key, value);   // 未满，直接插
    return true;
  }
  // key 不存在 且 页满 → 必须分裂，落到悲观路径
}
// ★ 这个作用域必须在拿 header 写锁之前结束，把所有读锁放干净
```

三点值得注意：

1. **页满时也要先看key 是否已存在**。第一版的乐观路径只在 `size < max` 时才记下叶子页号，
   于是「页已满 + key 是 tombstone」这种本来不需要分裂的情况被白白推到了悲观路径。
2. **必须用 `{ }` 把乐观路径圈起来**，或者显式 `reset()`。
   `std::shared_mutex`不支持锁升级，同一线程持着 header 读锁再去要 header 写锁会**自己锁死自己**
   （`EDEADLK`，`std::shared_mutex::lock()` 抛 `std::system_error` → `terminate`）。
   这正是 `MixTest` 最初挂死的直接原因。
3. 好处：绝大多数插入不会分裂，这条路径只对一个页加写锁，`header_page` 只读一下就放，并发度远高于悲观路径。

**路径 2：悲观路径（pessimistic / fallback）**

只有确实要分裂（或树为空要建根）时才进入：

```cpp
Context ctx;
ctx.header_page_ = bpm_->WritePage(header_page_id_);   // 全程持有 header 写锁
page_id_t root_page_id = header_w_page->root_page_id_;

if (root_page_id == INVALID_PAGE_ID) {
  // 空树建根。此刻已独占 header 写锁，不存在竞争，无需双重检查
  ...建一个只有一个 KV 的叶子当根...
  return true;
}
ctx.root_page_id_ = root_page_id;
// 用 WritePage 一路下降，每一层的 guard 都 push 进 ctx.write_set_
```

> 💡 **空树建根从「双重检查」简化成了「在写锁内直接做」。**
> 第一版是「先读锁看到空 → 再拿写锁 DCLP 复查」，而且**复查失败时忘了刷新 `root_page_id`**，
> 竞争失败的线程会带着 `INVALID_PAGE_ID` 继续往下走，去 `ReadPage(-1)`。
> 现在把建根挪进悲观路径，此时已独占 header 写锁，读到的值就是最终值，
> 整个 DCLP 连同它的 bug 一起消失了 —— 这类「把竞态消灭在结构上，而不是补丁式修补」通常更可靠。

`ctx.write_set_` 是一个「从根到叶父链的 guard 栈」。之所以要用它而不是局部变量：分裂需要向上递归修改父节点，`write_set_.back()` 就是当前父节点，处理完 `pop_back()` 继续往上，天然形成递归结构（这段推理在源码注释里也记了）。

拿到写锁后**重新检查一次** `FindKeyInLeaf` 和 `GetSize() < leaf_max_size_`（乐观路径判断和加锁之间状态可能已变）。

### CrabDownToLeaf：乐观路径的公共下降逻辑

`Insert` 和 `Remove` 的乐观路径共用它。核心是一个**不变式**：

> 循环中始终持有 `protector` —— 目标页**父亲**的读锁（根页的父亲就是 header page）。

为什么这一把锁就够？因为「分裂 / 借位 / 合并 / 删页 / 换根」这些**改变一个页在树中位置**的操作，
无一例外都要先写锁它的父亲。protector 在手，就没有任何线程能把我们脚下的页搬走。

```cpp
auto CrabDownToLeaf(const KeyType &key, page_id_t *leaf_page_id, bool *leaf_is_root)
    -> std::optional<WritePageGuard> {
  std::optional<ReadPageGuard> protector = bpm_->ReadPage(header_page_id_);
  page_id_t cur_id = protector->As<BPlusTreeHeaderPage>()->root_page_id_;
  if (cur_id == INVALID_PAGE_ID) { return std::nullopt; }   // 空树

  bool is_root = true;
  std::optional<ReadPageGuard> cur = bpm_->ReadPage(cur_id);

  while (!cur->As<BPlusTreePage>()->IsLeafPage()) {
    auto internal = cur->As<InternalPage>();
    page_id_t child_id = internal->ValueAt(ChildIndex(internal, key));
    auto child = bpm_->ReadPage(child_id);   // ① 先锁孩子
    protector = std::move(cur);              // ② 父亲升级为 protector（旧 protector 在此释放）
    cur = std::move(child);
    cur_id = child_id; is_root = false;
  }

  // ③ shared_mutex 不支持锁升级 → 必须先放叶子读锁，再拿叶子写锁。
  //    这个空窗是安全的：protector 还在手上，没人能分裂/删掉这个叶子。
  cur.reset();
  std::optional<WritePageGuard> leaf_guard = bpm_->WritePage(cur_id);
  protector.reset();

  *leaf_page_id = cur_id; *leaf_is_root = is_root;
  return leaf_guard;
}
```

三个容易踩的点：

- **②的两行顺序不能换**。`protector = std::move(cur)` 会先释放旧 protector（祖父），
  此时我们手上是「父亲 + 孩子」两把锁，从没出现空窗。
  若换成 `cur = std::move(child); protector = std::move(cur);`，第一行就把父亲的锁 `Drop()` 掉了
  （空窗回来了），而且 `protector` 会变成 `cur` 自己 —— 自己保护自己，毫无意义。
- **③ 必须先 `cur.reset()`**。持读锁再要同一页的写锁 = 自锁死。
- **`leaf_is_root` 要输出出去**。根叶子豁免 underflow 约束，但被删空时要改 header，
  两种情况的「安全」判据不同。

#### protector 是一个「角色」，不是某个固定的页

它的定义是：

```
protector = 「想把 cur 从树里挪走，就必须先写锁的那个页」
```

对照 B+Tree 里所有会**改变一个页在树中位置**的操作，没有例外：

| 操作 | 必须先写锁谁 |
|---|---|
| 分裂 `cur` | `cur` 的父亲（要往父亲里插 (key, 新页)） |
| 从兄弟借位到 `cur` | `cur` 的父亲（要改分隔 key） |
| 把 `cur` 合并掉 / 删掉 | `cur` 的父亲（`parent->RemoveAt`） |
| `cur` 是根，要换根 | header page（要改 `root_page_id_`） |

最后一行让这个角色**统一**了：根页的「父亲」就是 header page，不需要写特例
——所以根叶子情况下循环一次都不执行，`protector` 自然就停在 header guard 上。

「父亲升级为 protector」升级的是**职责**，不是锁的模式（全程都是读锁）。

#### 逐步的锁状态追踪

树为 `header → R(根,内部页) → I(内部页) → L(叶子)`：

| 步骤 | protector | cur | 实际持有的锁 |
|---|---|---|---|
| 初始 | header | — | header |
| `cur = ReadPage(R)` | header | R | header + R |
| 第 1 轮 ① `child = ReadPage(I)` | header | R | header + R + **I** |
| 第 1 轮 ② `protector = move(cur)` | **R** | (空壳) | ~~header~~ R + I |
| 第 1 轮 ③ `cur = move(child)` | R | I | R + I |
| 第 2 轮 ① `child = ReadPage(L)` | R | I | R + I + **L** |
| 第 2 轮 ② `protector = move(cur)` | **I** | (空壳) | ~~R~~ I + L |
| 第 2 轮 ③ `cur = move(child)` | I | L | I + L |
| 退出循环 | I | L | I + L |
| `cur.reset()` | I | — | **只剩 I** ← 关键时刻 |
| `WritePage(L)` | I | — | I(读) + L(**写**) |
| `protector.reset()` | — | — | L(写) |

规律：**循环里每一轮 `protector` 始终是 `cur` 的父亲**，每下降一层交接一次。
第 ② 步会短暂同时持有 3 把锁，然后降回 2 把。

#### 为什么下降过程中非要留着父亲

单看下降，其实**不需要** protector —— `GetValue` 的循环里就没留：

```cpp
auto child = bpm_->ReadPage(current_page_id);
cur = std::move(child);      // 移动赋值先Drop() 父亲，再接管孩子 → crabbing 已成立
```

`CrabDownToLeaf` 多留一把，唯一原因是它多了一步**读锁 → 写锁的升级**。
`std::shared_mutex` 不支持升级，只能「先放叶子读锁，再拿叶子写锁」，
这中间对叶子**什么锁都没有**。如果此时手上再没别的锁：

- 并发 `Remove` 可以把这个叶子合并掉、`DeletePage` 掉 → 我们随后写进一个已回收/被复用的 frame
- 或者叶子还在但被分裂过了，这个 key 已经不属于它 → 插到错误的页上，叶子层有序性静默损坏

**protector 就是为了在这个升级空窗里救命的。** 因为到达叶子之前不知道谁会是叶子的父亲，
只能每一层都把「父亲」这个位置占住。

---

### 乐观 vs 悲观：protector 与 `ctx.write_set_` 的分工

>这一节对 `Insert` 和 `Remove` 都适用。

初学最容易产生的疑问是：**「有了 protector，是不是就不用维护 `ctx.write_set_` 那条根到叶的父链了？」**
不是。`ctx.write_set_` 一点没减少，它在悲观路径里被用了十几处。两条路径**并存**，
因为这两个东西解决的是**完全不同的两个问题**：

| | 要解决的问题 | 需要的锁 |
|---|---|---|
| **保护**（defense） | 别人不能把我脚下的页**挪走** | **读**锁，只需要**父亲**一把 |
| **修改可达性**（offense） | 我要亲手**改**祖先节点，就必须**握着它们的写锁** | **写**锁，需要**整条链** |

`protector` 只解决第一件，`ctx.write_set_` 解决第二件。
**乐观路径的前提就是「我保证一个内部页都不改」**，所以第二件事对它根本不存在。

#### 乐观路径为什么真的不改内部页

它的每一个出口都只碰**一个叶子**：

| 出口 | 改了什么 |
|---|---|
| 重复键 | 什么都不改，`return false` |
| tombstone 复活 | 叶子里一个 value + 一条 tombstone 记录 |
| 未满插入 /安全删除 | 只动这个叶子的数组，`size` 不越界，不改 `next_page_id_` |
| **判据不成立** | **立刻放弃，退回悲观路径** |

最后一条是关键：**乐观路径的判据就是「这次操作不会传播到父节点」**
（插入看 `size < max`，删除看 `size - 1 >= GetMinSize()`）。
判据不成立就不走，所以它永远不需要访问父节点。

#### 悲观路径为什么必须攥住整条链

因为**分裂/合并会链式向上传播，而且传播多远事先不知道**。

走一遍最坏情况，树 `header → R → I → L`，插入导致 L 满了：

```
① 分裂 L → L / L_new，要往I 里插一条 (push_up_key, L_new)
   → 必须持有 I 的写锁                ← write_set_ 栈顶

② 结果 I 也满了 → 分裂 I → I / I_new，要往 R 里插一条
   → 必须持有 R 的写锁                    ← pop 掉 I，栈顶变成 R

③ 结果 R 也满了 → R 是根 → 新建根，要改 header 的 root_page_id_
   → 必须持有 header 的写锁                ← ctx.header_page_
```

**② 和 ③ 是在 ① 已经开始改数据之后才知道要做的。**
如果只留父亲，走到 ② 就没救了：祖父 R 的锁早放了，要改它只能重新 `WritePage(R)`，于是

1. **加锁顺序反了** —— 我们持着 I 的写锁去要 R 的写锁，而所有正常下降都是 R → I 方向。
   两个方向相反的线程一撞就是死锁。
2. **中途状态可能已变** —— 放开 R 的那一瞬间别人可能已经把 R 分裂了，
   `I` 在 R 里的槽位（`ValueIndex`）都不一样了。

所以只能一路下降时就把写锁全部攥住，宁可牺牲并发度。

`Remove` 的传播链更长：合并完叶子后父节点少一个 child，可能也underflow，
要继续向上借位/合并，一直可能传到根收缩。代码里那个
`while (parent->GetSize() < internal_min && !ctx.write_set_.empty())`
就是「沿着 `write_set_` 一层层往上爬」——**没有整条链，这个循环根本写不出来**。

#### 对比总表

| | 乐观路径 | 悲观路径 |
|---|---|---|
| 持有什么 | `protector`（父亲**读**锁）+ 叶子**写**锁 | header **写**锁 + `write_set_`（整条链**写**锁）+ 叶子写锁 |
| 锁的数量 | 2 | 树高 + 2 |
| 目的 | 保护（别人别动我） | 修改可达性（我要改祖先） |
| 会改内部页吗 | **不会**（这是它成立的前提） | 会，可能一直改到根 |
| 并发度 | 高，不同子树互不干扰 | 极低，header 写锁把所有写者串行化 |
| 页面写次数 | **1**（这正是 `OptimisticDeleteTest` 要的） | 树高 + 1 |
| 何时使用 | 判据成立：不满 / 不 underflow / key 已存在 | 判据不成立：要分裂 / 会 underflow / 建根 / 换根 |

#### 两者的衔接：放手重做，不是补锁续跑

乐观失败时**不是**「补上写锁继续」，而是彻底释放、从header 重新下降一遍：

```cpp
{
  ...乐观路径...
  // leaf_guard_opt 在此析构。必须在拿 header 写锁之前把所有读锁放干净，
  // 否则同一线程「读锁 header → 写锁 header」会自己把自己锁死
}
Context ctx;
ctx.header_page_ = bpm_->WritePage(header_page_id_);   //重新开始
```

那个 `{ }` 作用域是**必需**的（`shared_mutex` 不支持升级）。
悲观路径到叶子后还会**重新做一次判定**，因为乐观探测和重新加锁之间状态可能已变。

代价是白走一趟下降。`InsertTest2`（`leaf_max=3`，几乎每次插入都分裂）跑 20 秒，
主要开销就在「乐观失败 → 悲观重做」的双倍下降上。

> **一句话记住**：
> `protector` 是**防守**用的，一把父亲的读锁就能挡住所有想挪动叶子的人；
> `ctx.write_set_` 是**进攻**用的，你要亲手改哪些页就得握着哪些页的写锁，
> 而分裂/合并会一路传播到根，所以必须攥住整条链。
> 乐观路径能只用 2 把锁，唯一原因是它**主动限定自己只改一个叶子**，
> 一旦发现要动内部页就立刻认输退场。

---

### 叶子分裂

```
1. total = size + 1，split = total / 2
2. 新 key 的位置直接复用上面 FindKeyInLeaf 的 insert_pos（不再重复二分）
3. 三段式填入临时 vector：
   [0, new_insert_pos)  ← 原叶子
   [new_insert_pos]     ← 新 KV
   [new_insert_pos+1, total) ← 原叶子剩余（下标 +1）
4. NewPage() + Init()，原叶子截断为前 split 个，新叶子装后 (total - split) 个
5. tombstone 按 key 落到哪一页分派过去，且保持 FIFO 相对顺序（详见 2.7）
6. 修链表： new_leaf->next = old_leaf->next;  old_leaf->next = new_page_id;
7. push_up_key = new_leaf->KeyAt(0)，调InsertIntoParent(leaf_page_id, push_up_key, new_page_id, ctx)
```

用 `std::vector<KeyType>` / `std::vector<ValueType>` 做临时数组（而不是 VLA，VLA 不是标准 C++）。

叶子分裂上推的是**新叶子的第一个 key 的副本**——叶子层数据不丢，这是 B+Tree 与 B-Tree 的区别。

### InsertIntoParent：三种情况递归

```cpp
void InsertIntoParent(page_id_t old_page_id, const KeyType &push_up_key,
                      page_id_t new_page_id, Context &ctx);
```

**情况 1：old 就是根 —— 新建根，树高 +1**

```cpp
if (ctx.IsRootPage(old_page_id)) {
  new_root->Init(internal_max_size_);
  new_root->SetValueAt(0, old_page_id);      // 左孩子
  new_root->SetValueAt(1, new_page_id);      // 右孩子
  new_root->SetKeyAt(1, push_up_key);        // key[0] 留空
  new_root->SetSize(2);
  ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
  ctx.root_page_id_ = new_root_id;
  return;
}
```

`SetSize(2)` 而非 1：内部页的 size 计的是 **value（child 指针）的个数**，两个孩子 = size 2，其中有效 key 只有 1 个。

**情况 2：父节点未满 —— 直接插入**

```cpp
int idx = parent->ValueIndex(old_page_id);   // 用 old_page_id 定位父槽位
for (int i = parent->GetSize() - 1; i > idx; i--) {   // [idx+1, size) 右移
  parent->SetKeyAt(i + 1, parent->KeyAt(i));
  parent->SetValueAt(i + 1, parent->ValueAt(i));
}
parent->SetKeyAt(idx + 1, push_up_key);
parent->SetValueAt(idx + 1, new_page_id);
parent->ChangeSizeBy(1);
```

**必须用 `old_page_id` 在父节点中定位**（而不是拿 key 去二分），因为新记录必须插在 `old_page_id` 槽位的**紧右侧**。这一点在源码注释里专门解释了。

**情况 3：父节点也满 —— 内部页分裂 + 递归向上**

```
1. total = parent->GetSize() + 1，临时数组 key_tmp / val_tmp
   拷贝 [0, idx] → 插入 (push_up_key, new_page_id) 于 idx+1 → 拷贝 [idx+1, size)
2. split = total / 2，new_push_up_key = key_tmp[split]      ← 这个 key 会"上移"而非"复制"
3. 原父节点截断为 [0, split)（i > 0 才写 key，因为 key[0] 无效）
4. NewPage() 建右半页：
     new_internal->SetValueAt(0, val_tmp[split]);   ← 关键！右页最左孩子接住 val_tmp[split]
     然后从 key[1]/value[1] 开始填 [split+1, total)
5. old_internal_page_id = ctx.write_set_.back().GetPageId();
   ctx.write_set_.pop_back();                ← 弹出这一层
   InsertIntoParent(old_internal_page_id, new_push_up_key, new_internal_page_id, ctx);  ← 递归
```

第 4 步是内部页分裂最容易写错的地方：因为 `key_tmp[split]` 被**上推走了**（内部页的中间 key 是"移动"不是"复制"），它对应的 child `val_tmp[split]` 无处安放，必须成为右页的 `value[0]`（最左孩子）。

---

## 2.5 删除 Remove

`Remove` 是 P2 最长的一段（约 380 行），分乐观 / 悲观两条路径。
两条路径的分工、以及「为什么乐观路径不需要 `ctx.write_set_`」，见 2.4 的
[乐观 vs 悲观：protector 与 `ctx.write_set_` 的分工](#乐观-vs-悲观protector-与-ctxwrite_set_-的分工)。

### 乐观路径：一次删除只写一个页

绝大多数删除既不借位也不合并，更不需要改父节点和 header。这条路径用 `CrabDownToLeaf()`
读锁下降，只对目标叶子加一次写锁：

```cpp
auto leaf_guard_opt = CrabDownToLeaf(key, &opt_leaf_page_id, &leaf_is_root);
if (!leaf_guard_opt.has_value()) { return; }          // 空树

int found = FindKeyInLeaf(opt_leaf, key, &opt_insert_pos);
if (found == -1) { return; }                          // key 不存在
if (opt_leaf->IsTombstoned(found)) { return; }        // 已逻辑删除，幂等

// tombstone 缓冲区还有空位 → 纯逻辑删除，物理结构完全不变
if (TombCapacity() > 0 && opt_leaf->GetNumTombstones() < TombCapacity()) {
  opt_leaf->AddTombstone(found);
  return;
}

// 需要真正物理删掉一条，先判断会不会破坏最小占用约束
bool safe = leaf_is_root ? (opt_leaf->GetSize() - 1 > 0)                // 根叶子豁免 underflow，
                                // 但被删空要改 header
                         : (opt_leaf->GetSize() - 1 >= opt_leaf->GetMinSize());
if (safe) { ...直接删... return; }
// 会 underflow（或要改 header）→ 释放所有锁，退化到悲观路径重做
```

>💡 **为什么这条路径是必须的（不只是优化）**：
> `b_plus_tree_delete_test.DISABLED_OptimisticDeleteTest` 会数页面加锁次数
> （`TracedBufferPoolManager::GetReads/GetWrites` 统计的是 `ReadPage`/`WritePage`
> **调用次数**，不是磁盘 IO）：
>
> ```cpp
> EXPECT_GT(new_reads - base_reads, 0);      // 要有读页
> EXPECT_EQ(new_writes - base_writes, 1);    // 只能写 1 个页
> ```
>
> 只有悲观路径时是 5 次写 / 0 次读（header + 整条路径全用`WritePage`）；
> 加上乐观路径后变成 1 次写（只写叶子）/ 若干次读（header + 各层内部页），测试才过。

### 悲观路径：下降阶段

```cpp
Context ctx;
ctx.header_page_ = bpm_->WritePage(header_page_id_);    // 全程持header 写锁
root_page_id = header_page->root_page_id_;
if (root_page_id == INVALID_PAGE_ID) { return; }
// WritePage 一路下降，父路径存ctx.write_set_，叶子 guard 单独存leaf_guard
```

### 定位与删除（tombstone 优先）

```cpp
// 1. 叶子内二分找 key，找不到 → return
// 2. IsTombstoned(found)? 已被逻辑删除 → return
// 3. tombstone 缓冲区有空位 → AddTombstone，结构不变，直接 return
// 4. 缓冲区满 → 淘汰队首（最老）那条，把它「兑现」成物理删除腾出位置，
//    再把当前 key 记为新tombstone。此时 size 减 1，才可能 underflow
// 5.容量为 0（NumTombs <= 0）→ 退化成纯物理删除
```

详细的 tombstone 规则见 2.7。

### 情况 1：叶子就是根

```cpp
if (ctx.write_set_.empty()) {          // 没有父节点 → 叶子是根
  if (leaf->GetSize() == 0) {          // 根允许 underflow，但空了要清掉
    header->root_page_id_ = INVALID_PAGE_ID;
    leaf_guard.Drop();                 // 必须先 Drop 再 DeletePage
    bpm_->DeletePage(leaf_page_id);
  }
  return;
}
```

`leaf_guard.Drop()` 必须在 `DeletePage` 之前——否则 `pin_count > 0`，`DeletePage` 会返回 false。
（这条约束在并发下还依赖 `Drop()` 的正确顺序，见 1.4。）

### 情况 2：叶子层 underflow 处理

```cpp
int leaf_min = leaf->GetMinSize();                  // 统一用 GetMinSize()，不再自己算
if (leaf->GetSize() >= leaf_min) { return; }        // 没下溢，结束

int idx = parent->ValueIndex(leaf_page_id);         // 当前叶子在父节点中的槽位
```

> 💡 原来这里写的是 `(leaf_max_size_ + 1) / 2`，而 `BPlusTreePage::GetMinSize()` 对叶子返回 `max / 2`。
> `max` 为奇数时两者差 1（如 max=5：`2` vs `3`）。测试里直接用 `GetMinSize()`做断言
> （`EXPECT_EQ(2, GetMinSize())`、`EXPECT_GE(GetSize(), GetMinSize())`），所以口径必须统一到 `GetMinSize()`。

按标准的四步优先级：

**2a. 向左兄弟借（redistribute）** —— `idx > 0 && left_sib->GetSize() > leaf_min`

```
把左兄弟最后一个 KV 移到当前叶子最前面（当前叶子整体右移一格）
更新父节点分割key：parent->SetKeyAt(idx, leaf->KeyAt(0))
```

**2b. 向右兄弟借** —— `idx < parent->GetSize() - 1 && right_sib->GetSize() > leaf_min`

```
把右兄弟第一个 KV 追加到当前叶子末尾
right_sib->RemoveAt(0)
更新父节点分割 key：parent->SetKeyAt(idx + 1, right_sib->KeyAt(0))   ← 用 RemoveAt 之后的新 KeyAt(0)
```

**2c. 与左兄弟合并** —— `idx > 0`

```
当前叶子的全部 KV 追加到左兄弟末尾
left_sib->SetNextPageId(leaf->GetNextPageId())      ← 修叶子链表
parent->RemoveAt(idx)                               ← 父节点删掉当前叶子的槽位
leaf_guard.Drop(); DeletePage(leaf_page_id)← 删掉的是"当前叶子"
```

**2d. 与右兄弟合并** —— `idx == 0`（没有左兄弟）

```
page_id_t right_page_id = parent->ValueAt(idx + 1);   ← 先记下页号！
{
  右兄弟的全部 KV 追加到当前叶子末尾
  leaf->SetNextPageId(right_sib->GetNextPageId())
  parent->RemoveAt(idx + 1)
  right_guard.Drop();
}
DeletePage(right_page_id)← 删掉的是"右兄弟"
```

2d 的关键细节：这里被删的是**右兄弟**而不是当前叶子，而`parent->RemoveAt(idx+1)` 之后就再也拿不到那个 page_id 了，所以**必须在 RemoveAt 之前把 `right_page_id` 存到局部变量**。源码注释里也标了这一点。

### 情况 3：内部节点 underflow 递归向上

叶子层合并会让父节点少一个 child，父节点可能也underflow，用**while 循环**（而非递归）向上处理：

```cpp
int internal_min = (internal_max_size_ + 1) / 2;
WritePageGuard current_guard = std::move(ctx.write_set_.back());   // 让 guard 持有当前页
ctx.write_set_.pop_back();
page_id_t child_id = current_guard.GetPageId();
parent = current_guard.AsMut<InternalPage>();

while (parent->GetSize() < internal_min && !ctx.write_set_.empty()) {
  auto &grand_guard = ctx.write_set_.back();
  auto grand = grand_guard.AsMut<InternalPage>();
  int pidx = grand->ValueIndex(child_id);
  // 3a 向左兄弟借 → break
  // 3b 向右兄弟借 → break
  // 3c/3d 合并
  child_id = grand_guard.GetPageId();
  current_guard = std::move(grand_guard);      // 上移一层
  ctx.write_set_.pop_back();
  parent = current_guard.AsMut<InternalPage>();
}
```

这里有个重要的生命周期处理：**当前正在处理的内部页不能只留裸指针 `parent`**，必须让`current_guard` 持有它（源码注释：「当前正在处理的 internal page 不能只留裸指针，要让 guard 持有它」）。因为 `ctx.write_set_.pop_back()` 会析构 guard、释放页锁，裸指针立刻悬垂。

内部节点的借位与叶子不同，是**三方旋转**（左兄弟 /祖父分隔 key / 当前节点）：

```cpp
// 3a 向左兄弟借
parent 整体右移一格
parent->SetKeyAt(1, grand->KeyAt(pidx));                   // 祖父分隔 key 下推到 parent
parent->SetValueAt(0, ls->ValueAt(ls->GetSize() - 1));// 左兄弟最右孩子过来当最左孩子
grand->SetKeyAt(pidx, ls->KeyAt(ls->GetSize() - 1));       // 左兄弟最右 key 上提到祖父
ls->ChangeSizeBy(-1);
```

合并同理，要先把祖父的分隔 key **下推**填进合并处的空位，再拼接剩余 key/value：

```cpp
// 3c 与左兄弟合并
ls->SetKeyAt(ls->GetSize(), grand->KeyAt(pidx));      // 分隔 key 下推
ls->SetValueAt(ls->GetSize(), parent->ValueAt(0));    // parent 的最左孩子接上
ls->ChangeSizeBy(1);
for (int i = 1; i < parent->GetSize(); i++) { ...拼接剩余... }
grand->RemoveAt(pidx);
current_guard.Drop(); DeletePage(child_id);
```

### 情况 4：根收缩，树高 -1

```cpp
if (ctx.write_set_.empty() && parent->GetSize() == 1) {
  page_id_t old_root_page_id = current_guard.GetPageId();
  header->root_page_id_ = parent->ValueAt(0);     // 唯一的孩子成为新根
  ctx.root_page_id_ = parent->ValueAt(0);
  current_guard.Drop();
  DeletePage(old_root_page_id);
}
```

内部页只剩 1 个 child（0 个有效 key）时它就没有意义了，直接让唯一孩子上位。

---

## 2.6迭代器 IndexIterator

文件：`src/storage/index/index_iterator.cpp`

### 状态

```cpp
std::shared_ptr<TracedBufferPoolManager> bpm_;
ReadPageGuard guard_;      // 当前停留的叶子页（持读锁）
page_id_t page_id_;        // INVALID_PAGE_ID 表示 end
int index_;                // 页内下标
KeyComparator comparator_; // tombstone 比较用
```

`IsEnd()` 的判定就是 `page_id_ == INVALID_PAGE_ID`。

### 核心：AdvanceToNextVisible（自行新增）

这是本实现的设计重点——把「跨页」和「跳 tombstone」两件事统一收进一个函数，构造函数和 `operator++` 都调用它，保证**迭代器任何时刻都停在一个合法可见的 entry 上**（或者是 end）。

```cpp
while (true) {
  if (page_id_ == INVALID_PAGE_ID) { return; }              // 已是 end
  auto leaf = guard_.As<LeafPage>();

  if (index_ >= leaf->GetSize()) {                          // 当前页扫完了
    page_id_t next = leaf->GetNextPageId();
    if (next == INVALID_PAGE_ID) {                          // 没有下一页→ 变成 end
      guard_.Drop(); page_id_ = INVALID_PAGE_ID; index_ = 0; return;
    }
    guard_ = bpm_->ReadPage(next);                          // 跳到下一页（旧 guard 被移动赋值释放）
    page_id_ = next; index_ = 0;
    continue;
  }

  // 在页内范围内，但可能是 tombstone。
  // 按「下标」判定，不做 key 比较：tombstones_ 里存的本来就是 key_array_ 的下标
  if (IsCurrentEntryDeleted(leaf)) { index_++; continue; }

  return;      // 停在合法可见位置
}
```

`IsCurrentEntryDeleted()` 在头文件里**声明了却一直没定义**（因为没人调用，显式实例化也只会生成声明，
不会报链接错误，所以一直没被发现）。现在把它实现成 `leaf->IsTombstoned(index_)`，
省掉 comparator 调用，也不依赖 key 的唯一性。

用 `while` 循环而不是单次判断，是因为可能需要**连续多次**推进（源码注释里列了三种情形）：
1. 越过当前页末尾要跳页；
2. 跳页后第一个元素又是 tombstone；
3. 甚至可能连续跨过多个空页 / 全 tombstone 页。

### 其余接口

```cpp
IndexIterator(bpm, guard, page_id, index, comparator) { AdvanceToNextVisible(); }  // 构造即归一化
auto operator*() { return {leaf->KeyAtRef(index_), leaf->ValueAtRef(index_)}; }    // 返回引用对
auto operator++() { if (!IsEnd()) { index_++; AdvanceToNextVisible(); } return *this; }
```

构造函数里就调 `AdvanceToNextVisible()`，好处是 `Begin(key)` 里的 `lower_bound` 允许返回 `index == leaf->GetSize()`（key 比该页所有 key 都大），构造时会自动跳到下一页，调用方不用特殊处理。

### 三个入口

| 方法 | 实现 |
|---|---|
| `Begin()` | 读 header 拿 root；root 无效 → `End()`；否则一路 `ValueAt(0)` 走到最左叶子，`index = 0` |
| `Begin(key)` | 二分下降到目标叶子，页内做 **lower_bound**（`left < right` 模板，找第一个 `>= key`） |
| `End()` | `INDEXITERATOR_TYPE(bpm_,ReadPageGuard{}, INVALID_PAGE_ID, 0, comparator_)` |

`End()` 用**默认构造的 `ReadPageGuard`**（`is_valid_ = false`），所以它析构时 `Drop()` 直接返回，不会误减pin count。

---

## 2.7 Tombstone 懒删除

叶子页里有一块**容量固定为 `NumTombs` 的 tombstone 缓冲区**，用来做逻辑删除。
课程 writeup 不在仓库里，下面的语义是**从 `b_plus_tree_tombstone_test` 的 4 个用例反推**出来的。

### 先修一个致命的类型 bug

`b_plus_tree.h` 里的类型别名漏传了 `NumTombs`：

```cpp
using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;   // ← NumTombs 丢了
```

而 `NumTombs` 默认值是 `0`（`b_plus_tree_page.h:28`），于是 `LEAF_PAGE_TOMB_CNT = 0`，
`tombstones_[0]` 是零长数组。也就是说 **`BPlusTree<..., 2>` 内部一直在用「0 个 tombstone 槽」
的页面布局，而测试用「2 个槽」的布局去读同一块内存** —— 连`LEAF_PAGE_SLOT_CNT` 都不一样。
对比 `index_iterator.h:64` 是写对了的，只有树本身漏了。这个不修，后面一切都是空谈。

### 三条从测试反推出的规则

**规则 1：FIFO 队列，`tombstones_[0]` 最老，存的是「下标」不是 key。**

`GetTombstones()` 的实现是 `key_array_[tombstones_[i]]`。这个设计决定了
**任何移动 entry 的操作都必须同步修正下标** —— 这是整个实现最容易错的地方。
所以把记账收进页面层，让上层不用操心：

```cpp
void RemoveAt(int index) {
  ClearTombstoneAt(index);                                // 指向被删 entry 的记录要删掉
  for (i) if (tombstones_[i] > index) tombstones_[i]--;      // 后面的整体左移
  ...搬 entry...
}
void InsertAt(int index, key, value) {
  ...右移 entry...
  for (i) if (tombstones_[i] >= index) tombstones_[i]++;     // 后面的整体右移
  ...
}
```

`AddTombstone()` 用 `num_tombstones_ >= TombCapacity()` 做守卫 ——
容量为 0 时 `0 >= 0` 成立直接返回 false，顺带避免了对零长数组的越界写。

来源：`TombstoneBasicTest` 的「处理顺序」段。连删 3 个 key（容量 2），期望剩 `[d1, d2]`
且 `d0` 变成物理删除 → 缓冲区满时**淘汰队首并把它兑现成物理删除**。

**规则 2：split 时按 key 落到哪一页分派，保持 FIFO 相对顺序。**

```cpp
for (i in旧页 tombstone) {
  t = TombKeyIndexAt(i);
  if (t >= new_insert_pos) { t++; }      // 新 key 插进来会把后面的顶开一格
  if (t < split) { left_tombs.push(t); }
  else           { right_tombs.push(t - split); }
}
```

遍历顺序即 FIFO 顺序，所以相对新旧关系天然保留。
注意 `SetTombstoneIndexes()` 要在 entry 都搬完、`SetSize()` 定下来**之后**再调用
（它内部有 `tombstones_[i] < GetSize()` 的断言）。

**规则 3：merge 时「目标页的 tombstone 排在 FIFO 前面」，超容量则从队首兑现。**

`TombstoneCoalesceTest` 期望：左页存活 → tombstone 是右页的 `[6,3]`；右页存活 → 是左页的 `[1,2]`。
唯一能同时满足两个分支的解释就是：

```
合并后 FIFO = 目标页自己的（较老，排前面） + 被吸收页的（下标 += base）
然后从队首淘汰到装得下 —— 于是目标页自己的 tombstone 先被兑现掉
```

```cpp
void FlushTombstonesToFit(LeafPage *page, std::vector<size_t> &tombs) {
  page->ClearTombstones();        // 记账权临时收到外部 vector，避免和 RemoveAt 的自动修正打架
  while (tombs.size() > LeafPage::TombCapacity()) {
    size_t victim = tombs.front(); tombs.erase(tombs.begin());
    page->RemoveAt(victim);
    for (auto &t : tombs) { if (t > victim) { t--; } }
  }
  page->SetTombstoneIndexes(tombs);
}
```

### 其余两条约定

**Insert 命中 tombstone = 复活**：撤掉 tombstone 并**覆盖 value**，返回 true（不是重复键）。
`TombstoneBasicTest` 会检查复活后 `GetValue`拿到的是**新** RID。

**borrow 不借 tombstone 条目**：

```cpp
if (left_sib->GetSize() > leaf_min && !left_sib->IsTombstoned(last)) { ...借... }
```

借一个不可见的 entry过来根本缓解不了 underflow，还要跨页搬 tombstone 记账。
加上这个条件后，`TombstoneBorrowTest` 自动落到 merge 分支，结果正好是期望的 `[2]`。

### 「删空的树」不是空树

`TombstoneBasicTest` 最后把 17 个 key 全删掉，然后断言：

```cpp
EXPECT_GT(tot_tombs, ((num_keys - 1) / 4) * 2);   // 页面没有被物理清空
EXPECT_LT(tot_tombs, num_keys);
EXPECT_EQ(tree.Begin().IsEnd(), true);           // 但迭代器要表现为空
```

这个行为是自然涌现的：一旦某个叶子的所有 entry 都被 tombstone 了，
后续删除都会命中「已 tombstone → 直接 return」，页面就不再收缩。
当 `容量 <= min` 时叶子最终稳定在 `size == min` 且全部 tombstone，永远不会 underflow。

---

## 2.8 P2 遗留缺口与风险

### 缺口

1. **merge 后的二次 underflow 未处理**。
   极端情况下 `size_左 + size_右 - 容量` 仍可能小于 `min`（需要 `容量 > min` 才会发生）。
   4 个 tombstone 测试里 `容量 <= min`，都不触发。要彻底解决得把叶子层的 underflow 也改成循环。

2. **悲观路径不做「安全节点提前释放」**。
   `ctx.write_set_` 只增不减，所以一次操作会 pin 住 `header + 整条根到叶路径 + 兄弟页`。
   探针里试过 `internal_max=2` 的退化配置（fanout 只有 1~2，树极深），
   会直接 `CheckedWritePage failed to bring in page` 而 abort。正常参数下没问题，但这是已知边界。
   正确做法是下降时若某内部页「安全」（插入看 `size < max`，删除看 `size > min`），
   说明修改不会传播到它之上，立刻 `ctx.write_set_.clear()` 释放所有祖先。

3. **迭代器与借位的加锁方向相反**。
   迭代器沿叶子链表**左 → 右**推进（`guard_ = bpm_->ReadPage(next)` 先锁下一页再放当前页）；
   而 `Remove` 向左兄弟借位/合并时是**当前页 → 左兄弟**，即右 → 左。
   理论上构成环。目前没触发，是因为所有结构性写操作都被 header 写锁串行化了，
   同一时刻只有一个写者；但这是**靠外层串行化兜住的**，不是加锁协议本身正确。

### 已修复（保留记录）

1. ~~**tombstone 懒删除完全没实现**~~ → 已实现，见 2.7。
2. ~~**`Insert` 乐观路径 TOCTOU**~~ → 已修：真 crabbing + `CrabDownToLeaf`。
3. ~~**`Remove` 每次都抢 header 写锁**~~ → 已修：加了乐观路径。
4. ~~**`GetMinSize()` 与 `Remove()` 最小尺寸口径不一致**~~ → 已统一到 `GetMinSize()`。
5. ~~**`GetRootPageId()` 用了 `WritePage`**~~ → 已改成 `ReadPage`。
6. ~~**空树 DCLP 失败后没刷新 `root_page_id`**~~ → 已消除（建根挪进悲观路径的写锁内）。
7. ~~**`IndexIterator::IsCurrentEntryDeleted` 声明未定义**~~ → 已实现。

### 风险点（建议复核）

1. **tombstone 检查是 O(NumTombs) 线性扫描**，在 `GetValue` / `Remove` / 每次迭代器推进时都做。
   `NumTombs` 很小所以现在无所谓，但如果容量调大会成为热点。

2. **`Remove` 的借位/合并没有校验兄弟页类型**。`AsMut<LeafPage>()` 是无检查的 reinterpret，依赖「同一层的兄弟必然同类型」这一不变式。正确前提下没问题，但调试期若树结构被破坏，会表现为难以定位的内存乱码而非断言失败。

3. **乐观路径失败时会整体重做一遍下降**。`leaf_max_size` 很小（比如 3）时几乎每次插入都分裂，
   于是「读锁下降 + 写锁下降」双倍开销。`InsertTest2`（1000 key × 50 轮，leaf_max=3）
   跑 20 秒左右，主要开销在这里。可以考虑在乐观路径就判断「叶子已满」并跳过后续检查。

---

# 附：文件改动清单

## Project 1

| 文件 | 完成度 | 内容 |
|---|---|---|
| `src/storage/disk/disk_scheduler.cpp` | ✅ | 后台线程、请求队列、读写分派、毒丸退出 |
| `src/buffer/arc_replacer.cpp` | ✅ |ARC 全套：`Evict` / `RecordAccess` / `SetEvictable` / `Remove` / `Size` + 自建 `Dieout` |
| `src/include/buffer/arc_replacer.h` | ✅ | 四列表 + 两哈希索引 + `FrameStatus`（含链表迭代器）+ `Dieout` 声明 |
| `src/storage/page/page_guard.cpp` | ⚠️ | 读/写 guard 的构造、移动语义、`WritePageGuard::Flush`；**`Drop()` 顺序已修正**（先归还 pin 再放页锁）；`ReadPageGuard::Flush()` 仍为空 |
| `src/buffer/buffer_pool_manager.cpp` | ✅ | `NewPage` / `DeletePage` / `CheckedRead(Write)Page` 三情况 / `Flush*` 四函数 / `GetPinCount`；**换页竞态已修**（Case B/C 全程持锁） |
| `src/buffer/lru_k_replacer.cpp` | ❌ | 未实现（本实现走 ARC，此文件被绕过） |

## Project 2

| 文件 | 完成度 | 内容 |
|---|---|---|
| `src/include/storage/index/b_plus_tree.h` | ✅ | **修`LeafPage` 别名漏传 `NumTombs`**；新增 `FindKeyInLeaf` / `ChildIndex` / `CrabDownToLeaf` / `FlushTombstonesToFit` 声明 |
| `src/storage/page/b_plus_tree_page.cpp` | ✅ | 类型/尺寸的全部 getter/setter + `GetMinSize` |
| `src/storage/page/b_plus_tree_internal_page.cpp` | ✅ | `Init` / `KeyAt` / `SetKeyAt` / `ValueAt` / `ValueIndex` + 新增 `SetValueAt` `RemoveAt` |
| `src/include/storage/page/b_plus_tree_leaf_page.h` | ✅ | 新增整套 tombstone 接口 + `InsertAt` 声明 |
| `src/storage/page/b_plus_tree_leaf_page.cpp` | ✅ | 基础方法 + **tombstone FIFO 全套**；`RemoveAt` / `InsertAt` 自带下标修正 |
| `src/storage/index/b_plus_tree.cpp` | ✅ | `IsEmpty` / `GetValue` / `Insert` / `InsertIntoParent` / `Remove` / `Begin` / `Begin(key)` / `End` / `GetRootPageId`；**真 crabbing + Remove 乐观路径 + tombstone 全流程** |
| `src/storage/index/index_iterator.cpp` | ✅ | 构造 / `IsEnd` / `operator*` / `operator++` + `AdvanceToNextVisible` + 补齐 `IsCurrentEntryDeleted` |

## 框架文件的移植性修复（与作业逻辑无关，建议单独成 commit）

| 文件 | 内容 |
|---|---|
| `src/include/common/util/string_util.h` | 补 `#include <cstdint>`（`uint64_t`） |
| `src/include/primer/orset_driver.h` | 补 `#include <cstdint>`（`int64_t` / `uint32_t`） |

>环境的工具链从 gcc-toolset-14 升到 15后，libstdc++ 15 收紧了传递包含，
> `<string>` / `<vector>` / `<memory>` 不再顺带带出 `<cstdint>`。
> 报错会级联成`out-of-line definition ... does not match any declaration`，容易误判。
> 排错用 `ninja -k 0`（遇错继续）一次收齐全部根因，不要用默认的 fail-fast。

## 自行新增的辅助设施（非框架要求）

| 名称 | 作用 |
|---|---|
| `ArcReplacer::Dieout()` | 收拢「淘汰一个 frame 到ghost 列表」的 5 步公共逻辑 |
| `FrameStatus::it_` | 缓存链表位置迭代器，让 `list::erase` 降到 O(1) |
| `BPlusTree::ChildIndex()` | 内部页二分抽成一处（原来重复 5 遍） |
| `BPlusTree::FindKeyInLeaf()` | 叶子二分 + 输出 `insert_pos`，查重/复活/插入/分裂共用 |
| `BPlusTree::CrabDownToLeaf()` | 乐观路径的螃蟹式下降，返回叶子写锁 |
| `BPlusTree::InsertIntoParent()` | 递归处理分裂上推（新建根 / 父未满 / 父也分裂） |
| `BPlusTree::FlushTombstonesToFit()` | 合并后把超容量的 tombstone 兑现成物理删除 |
| `InternalPage::SetValueAt` / `RemoveAt` | 分裂合并时单独改 child 指针 / 删槽位 |
| `LeafPage::InsertAt` / `RemoveAt` | KV 移位并同步修正 tombstone 下标 |
| `LeafPage` tombstone 接口 | `TombCapacity` / `GetNumTombstones` / `TombKeyIndexAt` / `IsTombstoned` / `AddTombstone` / `PopOldestTombstone` / `ClearTombstoneAt` / `ClearTombstones` / `SetTombstoneIndexes` |
| `IndexIterator::AdvanceToNextVisible()` | 统一处理跨页与 tombstone 跳过，保证迭代器不变式 |
| `build_support/verify_p1_p2.sh` | 一键编译 + 跑全部 P1/P2 测试（`--clean` / `p1` / `p2`） |

---

## 测试现状

构建命令（**必须用 clang**，系统 gcc 没有 libasan）：

```bash
CC=clang CXX=clang++ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja -j $(nproc) build-tests
```

跑测试必须加 `--gtest_also_run_disabled_tests`（BusTub 用例默认带 `DISABLED_` 前缀）。

| P1 | 结果 | | P2 | 结果 |
|---|---|---|---|---|
| `disk_manager_test` | 4/4 ✅ | | `b_plus_tree_insert_test` | 4/4 ✅ |
| `disk_scheduler_test` | 1/1 ✅ | | `b_plus_tree_delete_test` | 3/3 ✅ |
| `arc_replacer_test` | 2/2 ✅ | | `b_plus_tree_tombstone_test` | 4/4 ✅ |
| `page_guard_test` | 2/2 ✅ | | `b_plus_tree_sequential_scale_test` | 1/1 ✅ |
| `buffer_pool_manager_test` | 7/7 ✅ | | `b_plus_tree_concurrent_test` | 6/6 ✅ |
| **小计** | **16/16** | | **小计** | **18/18** |

并发测试耗时：`InsertTest1` 3s、`InsertTest2` 20s、`DeleteTest1/2` <1s、`MixTest1` 18s、`MixTest2` 5s。
全程 AddressSanitizer 开启。`DeleteTest1`曾经 flaky，额外连跑 5 次确认稳定。

---

## 下一步 TODO（P1/P2 范围内）

- [ ] 补 `ReadPageGuard::Flush()`
- [ ] `ArcReplacer::Size()` 加 `latch_`
- [ ] 修 `GetPinCount()` 中放锁后解引用迭代器的问题
- [ ] `FrameStatus::it_` 的类型双关改为显式方案
- [ ] （性能）给 frame 加 `io_in_progress_` 标记 + 条件变量，把换页 IO 重新挪出 `bpm_latch_`
- [ ] （性能/健壮性）悲观路径加「安全节点提前释放」，解决深树 pin 满abort
- [ ] 处理 merge 后的二次 underflow（`容量 > min` 时才会触发）
- [ ] 统一迭代器与借位的加锁方向，去掉对 header 写锁串行化的隐式依赖
- [ ] （可选）`lru_k_replacer.cpp` —— 若评测要求 LRU-K 版本
