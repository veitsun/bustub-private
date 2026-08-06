# BusTub P1 / P2 实现笔记

> 记录 Project 1（Buffer Pool Manager）与 Project 2（B+Tree Index）已完成的全部工作：
> 涉及文件、设计思路、关键实现细节、以及遗留缺口与已知风险。
>
> 代码基线：`master`，`src/` 目录。

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
  - [2.3 查找 GetValue](#23-查找-getvalue)
  - [2.4 插入 Insert 与 InsertIntoParent](#24-插入-insert-与-insertintoparent)
  - [2.5 删除 Remove](#25-删除-remove)
  - [2.6 迭代器 IndexIterator](#26-迭代器-indexiterator)
  - [2.7 P2 遗留缺口与风险](#27-p2-遗留缺口与风险)
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

  page_lock_.unlock();            // ① 先放页面锁

  {
    std::lock_guard<std::mutex> lk(*bpm_latch_);   // ② 再拿 bpm_latch_
    frame_->pin_count_--;
    if (frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
}
```

**顺序不能反。** BPM 的 `CheckedReadPage/CheckedWritePage` 是「先持 `bpm_latch_`，再构造 guard 去拿页面锁」，如果 `Drop()` 反过来「先持 `bpm_latch_` 再放页面锁」，两条路径的加锁顺序相反 → 死锁。

`pin_count_` 归零时才通知 replacer 该frame 变为可淘汰，这也是 `curr_size_` 保持正确的关键。

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
lk.unlock();                                   // 放锁再做 IO
// 发起磁盘读，同步等完成
```

**Case C —— 未命中且无空闲 frame（最复杂）**

```cpp
auto victim = replacer_->Evict();
if (!victim.has_value()) { return std::nullopt; }   // 真的 OOM

if (frame->is_dirty_) {
  auto old_page_id = frame->page_id_;
  frame->is_dirty_ = false;   // 先清标记，避免并发重复写回
  lk.unlock();                //↓ 放锁做写回 IO
  ... Schedule(write) ; future.get() ...
  lk.lock();                  //   ↑ 重新拿锁
}
page_table_.erase(frame->page_id_);   // 删旧映射
frame->Reset();
frame->page_id_ = page_id;
page_table_[page_id] = frame_id;      // 建新映射
frame->pin_count_.store(1);
replacer_->RecordAccess(...); replacer_->SetEvictable(frame_id, false);
lk.unlock();
... Schedule(read) ; future.get() ...
return WritePageGuard(...);
```

三个实现要点：

1. **锁的类型选`std::unique_lock` 而不是 `std::lock_guard`**。做 IO 前要能中途 `unlock()`，IO 后要能 `lock()` 回来，`lock_guard` 做不到（源码注释里专门写了这一点）。
2. **`SetEvictable(frame_id, false)` 要放在 `RecordAccess` 之后**。`RecordAccess` 才刚把条目插进 `alive_map_`，顺序反了会抛「frame_id not found」。
3. 所有磁盘 IO 都在**放掉 `bpm_latch_` 之后**做，`future.get()` 同步等待。否则整个缓冲池会被一次磁盘 IO 串行化。

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
- `PageGuard::Drop()` 严格「先放`rwlatch_`，再拿 `bpm_latch_`」——因为反向持有会与 BPM 的获取顺序构成环。
- 所有磁盘 IO 一律在**不持任何锁**的状态下进行。

---

## 1.7 P1 遗留缺口与风险

###缺口

1. **`ReadPageGuard::Flush()` 是空实现**（`page_guard.cpp:149`）。
   当前依赖「读页不会脏」这一假设。但脏页是可能在 `WritePageGuard` 释放后仍留在 frame 里的，此时另一个线程拿 `ReadPageGuard` 再调 `Flush()` 就会静默什么都不做。若 Gradescope 有对应用例，需要补上「发写请求 + 清 `is_dirty_`」的逻辑（注意共享锁下清 `is_dirty_` 需要额外考虑同步）。

2. **`src/buffer/lru_k_replacer.cpp` 完全未实现**。若课程/评测要求提交 LRU-K 版本，需要另行补齐；当前 BPM 只依赖 `ArcReplacer`。

### 风险点（建议复核）

1. **`ArcReplacer::Size()` 没有加 `latch_`**（`arc_replacer.cpp:370`）。
   其他所有接口都用 `lock_guard` 保护了 `curr_size_`，唯独 `Size()` 裸读，在 TSan 下会被报成data race。加一行 `std::lock_guard` 即可。

2. **`FrameStatus::it_` 的类型双关（type punning）**。
   `it_` 声明为 `std::list<frame_id_t>::iterator`，但在 `Dieout()` 中被赋值为 `list_ghost_.begin()`（类型是 `std::list<page_id_t>::iterator`）。
   目前能编译通过纯粹因为 `frame_id_t` 和 `page_id_t` **都是 `int32_t`**（见 `common/config.h:46-48`），两个模板实例化成了同一个类型。这是**依赖巧合的写法**，一旦哪天 `page_id_t` 改成 `int64_t` 就会直接编译失败。建议用 `std::variant` 或拆成两个成员。

3. **`GetPinCount` 在 `unlock()` 之后才解引用迭代器**（`buffer_pool_manager.cpp:708-710`）。
   ```cpp
   bpm_latch_->unlock();
   return frames_[it->second]->pin_count_.load();   // it 可能已失效
   ```
   如果另一个线程在这两行之间执行了 `DeletePage`，`it` 就是悬垂迭代器。应该在放锁**之前**把 `it->second` 取出到局部变量。

4. **`Evict()` 的判定用 `mru_.size() >= mru_target_size_`**，论文里是 `|T1| >= max(1, p)`。`mru_target_size_` 为 0 时行为略有差异，通常不影响正确性但会影响命中率（leaderboard 相关）。

5. **Case C 中释放锁做写回 IO 存在竞态窗口**：`lk.unlock()` 到 `lk.lock()` 之间，被淘汰的 frame 已不在 replacer 里（`Evict` 已摘除）但 `page_table_` 里的旧映射还在。此时若另一线程请求那个旧 page_id，会命中 `page_table_` 拿到一个正在被写回/即将被复用的 frame。这是本实现的一个潜在正确性隐患，若遇到并发测试失败应优先排查此处。

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
| `SetKeyValueAt(index, key, value)` | **自行新增**，key/value 成对写入，插入时的移位操作全靠它 |
| `RemoveAt(index)` | **自行新增**，key/value 一起左移 + `ChangeSizeBy(-1)` |
| `GetTombstones()` | 框架给定，把 `tombstones_[]` 里的下标翻译成 key 列表 |

---

## 2.3 查找 GetValue

```
1. ReadPage(header_page_id_) → root_page_id_
   root == INVALID_PAGE_ID → return false（空树）
2. 循环下降：
   - IsLeafPage()? → 叶子内二分找 key
       找不到 → false
       找到   → 遍历 GetTombstones()，若命中 tombstone 则视为不存在 → false
       否则   → result->push_back(ValueAt(found)); return true
   - 否则是内部页 → 二分找「最后一个 KeyAt(mid) <= key」的 mid 作为 child_idx
                    current_page_id = ValueAt(child_idx)
```

内部页的二分模板（全文复用了 4 次，`GetValue` / `Insert` 两处 / `Remove` / `Begin(key)`）：

```cpp
int child_idx = 0;                     // 默认走最左孩子
int left = 1, right = size - 1;        // key[0] 无效，从 1 开始
while (left <= right) {
  int mid = left + (right - left) / 2;
  if (comparator_(internal->KeyAt(mid), key) <= 0) { child_idx = mid; left = mid + 1; }
  else                { right = mid - 1; }
}
current_page_id = internal->ValueAt(child_idx);
```

语义：找**最后一个 `key[i] <= 目标 key`** 的 `i`，因为内部页的 `key[i]` 是「子树 i 的最小 key」。

`GetValue` 全程只用 `ReadPage`（共享锁），下降过程中guard 是局部变量，随循环迭代自然析构，实现了「螃蟹加锁（crabbing）」的效果。

---

## 2.4 插入 Insert 与 InsertIntoParent

### 三条路径

**路径 0：空树**

```cpp
if (root_page_id == INVALID_PAGE_ID) {
  auto header_w_guard = bpm_->WritePage(header_page_id_);   // 此时才升级为写锁
  if (header_w_page->root_page_id_ == INVALID_PAGE_ID) {    // 双重检查（DCLP）
    new_page_id = bpm_->NewPage();
    root_leaf->Init(leaf_max_size_);
    root_leaf->SetKeyValueAt(0, key, value);
    root_leaf->SetSize(1);
    header_w_page->root_page_id_ = new_page_id;
    return true;
  }
}
```

先用读锁看到「空」，再拿写锁**重新确认一次**，防止两个线程同时建根。

**路径 1：乐观路径（optimistic）**

用 `ReadPage` 一路下降到叶子，只判断一件事：`leaf->GetSize() < leaf_max_size_`（不会分裂）。
如果成立，记下 `target_leaf_page_id`，退出循环后单独 `WritePage(target_leaf_page_id)` 做插入：

```cpp
// 二分找插入位置，顺便查重复 key
int insert_pos = leaf->GetSize();
while (left <= right) {
  int cmp = comparator_(leaf->KeyAt(mid), key);
  if (cmp == 0) { return false; }                 // 唯一键，重复直接失败
  if (cmp < 0)  { left = mid + 1; }
  else          { insert_pos = mid; right = mid - 1; }
}
// 从后往前移位腾出 insert_pos
for (int i = leaf->GetSize(); i > insert_pos; i--) {
  leaf->SetKeyValueAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
}
leaf->SetKeyValueAt(insert_pos, key, value);
leaf->ChangeSizeBy(1);
```

好处：绝大多数插入不会分裂，这条路径只对**一个页**加写锁，`header_page` 完全不碰，并发度远高于悲观路径。

**路径 2：悲观路径（pessimistic / fallback）**

只有乐观路径发现叶子已满时才进入：

```cpp
Context ctx;
ctx.header_page_ = bpm_->WritePage(header_page_id_);   // 全程持有 header 写锁
ctx.root_page_id_ = root_page_id;
// 用 WritePage 一路下降，每一层的 guard 都 push 进 ctx.write_set_
while (true) {
  auto guard = bpm_->WritePage(current_page_id);
  if (page->IsLeafPage()) { leaf_guard = std::move(guard); break; }
  ...二分找child...
  ctx.write_set_.push_back(std::move(guard));// 保留父路径
}
```

`ctx.write_set_` 是一个「从根到叶父链的 guard 栈」。之所以要用它而不是局部变量：分裂需要向上递归修改父节点，`write_set_.back()` 就是当前父节点，处理完 `pop_back()` 继续往上，天然形成递归结构（这段推理在源码注释里也记了）。

拿到写锁后**重新检查一次** `leaf->GetSize() < leaf_max_size_`（乐观路径判断和加锁之间状态可能已变），仍然没满就走普通插入。

### 叶子分裂

```
1. total = size + 1，split = total / 2
2. 二分找新 key 在原叶子中的位置 new_insert_pos（顺便查重复 → return false）
3. 三段式填入临时 vector：
   [0, new_insert_pos)  ← 原叶子
   [new_insert_pos]     ← 新 KV
   [new_insert_pos+1, total) ← 原叶子剩余（下标 +1）
4. NewPage() + Init()，原叶子截断为前 split 个，新叶子装后 (total - split) 个
5. 修链表： new_leaf->next = old_leaf->next;  old_leaf->next = new_page_id;
6. push_up_key = new_leaf->KeyAt(0)，调InsertIntoParent(leaf_page_id, push_up_key, new_page_id, ctx)
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

`Remove` 是 P2 最长的一段（约 300 行），走的是完整的悲观路径。

### 下降阶段

```cpp
Context ctx;
ctx.header_page_ = bpm_->WritePage(header_page_id_);    // 全程持header 写锁
root_page_id = header_page->root_page_id_;
if (root_page_id == INVALID_PAGE_ID) { return; }
// WritePage 一路下降，父路径存ctx.write_set_，叶子 guard 单独存leaf_guard
```

### 定位与删除

```cpp
// 1. 叶子内二分找 key，找不到 → return
// 2. 检查 GetTombstones()，已被逻辑删除 → return
// 3. leaf->RemoveAt(found)← 物理删除
```

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

### 情况 2：叶子层 underflow 处理

```cpp
int leaf_min = (leaf_max_size_ + 1) / 2;
if (leaf->GetSize() >= leaf_min) { return; }        // 没下溢，结束

int idx = parent->ValueIndex(leaf_page_id);         // 当前叶子在父节点中的槽位
```

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

  // 在页内范围内，但可能是 tombstone
  KeyType current_key = leaf->KeyAt(index_);
  bool deleted = false;
  for (const auto &tomb_key : leaf->GetTombstones()) {
    if (comparator_(tomb_key, current_key) == 0) { deleted = true; break; }
  }
  if (deleted) { index_++; continue; }

  return;      // 停在合法可见位置
}
```

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

## 2.7 P2 遗留缺口与风险

### 缺口

1. **Tombstone 懒删除机制没有实现**（`b_plus_tree.cpp:704` 注释「优先 tombstone 懒删除，先不管这个」）。

   现状是一个**半成品状态**：
   - 读路径的 tombstone 过滤**已经写好了**：`GetValue()` 会查 `GetTombstones()`，`IndexIterator::AdvanceToNextVisible()` 会跳过 tombstone，`Remove()` 也会检查「是否已被 tombstone」。
   - 但**没有任何代码往`tombstones_[]` 里写**。`LeafPage::Init()` 把 `num_tombstones_` 置0 之后，这个值永远是 0。
   - 所以 `Remove()` 走的是纯物理删除 + 借位/合并路径，上面那些过滤逻辑**永远不会被触发**（是死代码）。

   要补的话，需要在 `b_plus_tree_leaf_page.cpp` 里新增「写入 tombstone」的方法（记录被删 key 的下标到 `tombstones_[]`、维护 `num_tombstones_`），并在 `Remove()` 里优先走懒删除、达到阈值后再触发真正的物理压缩。

2. **`GetMinSize()` 与 `Remove()` 里的最小尺寸不一致**。
   - `BPlusTreePage::GetMinSize()`：叶子返回 `GetMaxSize() / 2`
   - `Remove()` 里：`int leaf_min = (leaf_max_size_ + 1) / 2;`

   `max` 为奇数时两者差1（如 max=5：`5/2 = 2` vs `(5+1)/2 = 3`）。目前 `Remove()` 没有调 `GetMinSize()`，所以行为是自洽的，但两个来源并存容易在后续修改时踩坑。建议统一到`GetMinSize()`。

### 风险点（建议复核）

1. **`Insert` 乐观路径存在 TOCTOU 竞态**。
   下降时用 `ReadPage` 判断 `leaf->GetSize() < leaf_max_size_`，然后**释放读锁**、退出循环、再 `WritePage(target_leaf_page_id)`。这两步之间叶子可能被其他线程插满甚至分裂掉。
   代码在拿到写锁后**重新检查了一次** `GetSize() < leaf_max_size_`（`b_plus_tree.cpp:268`），所以不会写溢出，会退化到悲观路径——这部分是安全的。
   但更隐蔽的问题是：期间叶子若发生**分裂**，`target_leaf_page_id` 可能已不再是该 key 应该去的那个叶子，插入就会落到错误的页上，破坏有序性。这是并发测试下需要重点验证的地方。

2. **`Remove()` 对每次删除都抢 `header_page` 的写锁**（`b_plus_tree.cpp:629`）。
   这会把所有 Remove 完全串行化。函数开头的注释其实提到了「正确的实现下只有目标叶子需要写一次」，但实现并没有做乐观路径。若要提升并发度（leaderboard），应该像 `Insert` 一样加一条乐观路径：先用读锁探测「删除后不会 underflow」，再只锁目标叶子。

3. **`GetRootPageId()` 用了 `WritePage`**（`b_plus_tree.cpp:1060`）。
   只是读一个字段却拿了排他锁，应改为 `ReadPage`。

4. **tombstone 检查是 O(num_tombstones) 线性扫描**，且在 `GetValue` / `Remove` / 每次迭代器推进时都做。如果后续真的实现了tombstone，这里会成为热点。

5. **`Remove` 的借位/合并没有校验兄弟页类型**。`AsMut<LeafPage>()` 是无检查的 reinterpret，依赖「同一层的兄弟必然同类型」这一不变式。正确前提下没问题，但调试期若树结构被破坏，会表现为难以定位的内存乱码而非断言失败。

---

# 附：文件改动清单

## Project 1

| 文件 | 行数 | 完成度 | 内容 |
|---|---|---|---|
| `src/storage/disk/disk_scheduler.cpp` | 79 | ✅ | 后台线程、请求队列、读写分派、毒丸退出 |
| `src/buffer/arc_replacer.cpp` | 376 | ✅ |ARC 全套：`Evict` / `RecordAccess` / `SetEvictable` / `Remove` / `Size` + 自建 `Dieout` |
| `src/include/buffer/arc_replacer.h` | 95 | ✅ | 四列表 + 两哈希索引 + `FrameStatus`（含链表迭代器）+ `Dieout` 声明 |
| `src/storage/page/page_guard.cpp` | 381 | ⚠️ | 读/写 guard 的构造、移动语义、`Drop`、`WritePageGuard::Flush`；**`ReadPageGuard::Flush()` 为空** |
| `src/buffer/buffer_pool_manager.cpp` | 716 | ✅ | `NewPage` / `DeletePage` / `CheckedRead(Write)Page` 三情况 / `Flush*` 四函数 / `GetPinCount` |
| `src/buffer/lru_k_replacer.cpp` | 107 | ❌ | 未实现（本实现走 ARC，此文件被绕过） |

## Project 2

| 文件 | 行数 | 完成度 | 内容 |
|---|---|---|---|
| `src/storage/page/b_plus_tree_page.cpp` | 80 | ✅ | 类型/尺寸的全部 getter/setter + `GetMinSize` |
| `src/storage/page/b_plus_tree_internal_page.cpp` | 112 | ✅ | `Init` / `KeyAt` / `SetKeyAt` / `ValueAt` / `ValueIndex` + 新增 `SetValueAt` `RemoveAt` |
| `src/storage/page/b_plus_tree_leaf_page.cpp` | 135 | ⚠️ | `Init` / 链表指针 / `KeyAt(Ref)` / `ValueAt(Ref)` + 新增 `SetKeyValueAt` `RemoveAt`；**无tombstone 写入方法** |
| `src/storage/index/b_plus_tree.cpp` | 1081 | ⚠️ | `IsEmpty` / `GetValue` / `Insert`（乐观+悲观+分裂）/ `InsertIntoParent`（递归）/ `Remove`（借位+合并+根收缩）/ `Begin` / `Begin(key)` / `End` / `GetRootPageId`；**tombstone 懒删除未做** |
| `src/storage/index/index_iterator.cpp` | 143 | ✅ | 构造 / `IsEnd` / `operator*` / `operator++` + 自建 `AdvanceToNextVisible`（跨页 + 跳 tombstone） |

## 自行新增的辅助设施（非框架要求）

| 名称 | 位置 | 作用 |
|---|---|---|
| `ArcReplacer::Dieout()` | `arc_replacer.h:91` | 收拢「淘汰一个 frame 到ghost 列表」的 5 步公共逻辑 |
| `FrameStatus::it_` | `arc_replacer.h:37` | 缓存链表位置迭代器，让 `list::erase` 降到 O(1) |
| `BPlusTree::InsertIntoParent()` | `b_plus_tree.cpp:491` | 递归处理分裂上推（新建根 / 父未满 / 父也分裂） |
| `InternalPage::SetValueAt` / `RemoveAt` | `b_plus_tree_internal_page.cpp` | 分裂合并时单独改 child 指针 / 删槽位 |
| `LeafPage::SetKeyValueAt` / `RemoveAt` | `b_plus_tree_leaf_page.cpp` | KV 成对写入 / 删除并左移 |
| `IndexIterator::AdvanceToNextVisible()` | `index_iterator.cpp:67` | 统一处理跨页与 tombstone 跳过，保证迭代器不变式 |

---

## 下一步 TODO（P1/P2 范围内）

- [ ] 补 `ReadPageGuard::Flush()`
- [ ] `ArcReplacer::Size()` 加 `latch_`
- [ ] 修 `GetPinCount()` 中放锁后解引用迭代器的问题
- [ ] `FrameStatus::it_` 的类型双关改为显式方案
- [ ] 实现叶子页 tombstone 写入 + `Remove` 懒删除路径
- [ ] 统一 `GetMinSize()` 与 `Remove()` 的最小尺寸口径
- [ ] `GetRootPageId()` 改用 `ReadPage`
- [ ] （可选）给 `Remove()` 加乐观路径，去掉 header 写锁的全局串行化
- [ ] （可选）复核 `Insert` 乐观路径在并发分裂下的正确性
