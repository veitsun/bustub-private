# BusTub P4 实现笔记 — Concurrency Control (MVCC)

> Project 4（并发控制）的概念地基、任务清单与实现记录。
>
> 代码基线：`master`（P1/P2 已完成；P3 中 SeqScan/Insert/Delete/Update 已按「非事务版」写完，
> 其余 P3 算子——聚合/连接/排序/TopN/外排序/窗口函数——**对 P4 主线不是必需的**，见下文说明）。
> 相关笔记：[`NOTES-P1-P2.md`](./NOTES-P1-P2.md) · [`NOTES-P3.md`](./NOTES-P3.md)
>
> **本文档是活文档**：前半部分是开工前必须搞清楚的概念（按重要性排序），
> 后半部分是实现记录与踩坑记录，边做边往里填。

---

## 目录

**第一部分：概念地基（★ 越多越重要）**

- [0. P4 是什么 · 一句话定位](#0-p4-是什么--一句话定位)
- [1. ★★★ 版本链：Base Tuple + UndoLog 链表](#1--版本链base-tuple--undolog-链表)
- [2. ★★★ 时间戳体系：临时时间戳 vs 提交时间戳](#2--时间戳体系临时时间戳-vs-提交时间戳)
- [3. ★★★ ReconstructTuple / CollectUndoLogs —— MVCC 读路径的核心](#3--reconstructtuple--collectundologs--mvcc-读路径的核心)
- [4. ★★☆ Watermark ——谁的日志能被回收](#4--watermark--谁的日志能被回收)
- [5. ★★☆ 写写冲突与 TAINTED 状态](#5--写写冲突与-tainted-状态)
- [6. ★★☆ Commit 流程：commit_mutex_ + write set](#6--commit-流程commit_mutex--write-set)
- [7. ★☆☆ 索引与 MVCC 的矛盾：索引项永不删除](#7--索引与-mvcc-的矛盾索引项永不删除)
- [8. ★☆☆ 原子读写：UpdateTupleAndUndoLink / GetTupleAndUndoLink](#8--原子读写updatetupleandundolink--gettupleandundolink)

**第二部分：任务与进度** → [任务清单](#任务清单与进度表) · [推进顺序](#建议推进顺序) · [构建与测试](#构建与测试)

**第三部分：实现记录** → [实现记录](#实现记录) · [踩坑记录](#踩坑记录) · [遗留问题](#遗留问题)

---

# 第一部分：概念地基

## 0. P4 是什么 · 一句话定位

> **给每一行数据挂一条「历史版本链」，让多个事务互不打扰地同时读写同一张表。**

P1 解决「页怎么搬」，P2 解决「怎么按key 查」，P3 让系统能「执行 SQL」，
但P3 里所有事务其实都是「裸写」——`TupleMeta.ts_` 永远填 0，没有隔离性。
**P4 把时间戳这个维度加进来，用乐观多版本并发控制（MVOCC）实现快照隔离（Snapshot Isolation）。**

```
本项目采用"delta table"存储模型：
  TableHeap 里只存"最新版本"（base tuple + base meta）
      │
      │ base_meta.ts_ 是「最后修改这行的时间戳（或临时时间戳）」
      ▼
  version_info_[page_id][slot] → UndoLink { prev_txn_, prev_log_idx_ }
      │
      ▼
  txn->undo_logs_[idx] = UndoLog { is_deleted_, modified_fields_, tuple(旧值的部分列), ts_, prev_version_ }
      │
      ▼ 继续沿 prev_version_ 往前，直到 UndoLink 无效 → 链表终点
  ...
```

**核心心智模型**：每行数据不是「一个值」，而是「一条时间线」。
不同事务用不同的「观察时刻」（`read_ts_`）看这条时间线，各自看到不同的版本——这就是快照隔离。

**bug 特征**：和 P3 一样很少是「崩」，但比 P3 更隐蔽——
两个事务并发跑，**单线程测试全过，一并发就错**（丢更新、幻读、脏读）。
→ 调试工具从 `EXPLAIN` 换成 **`TxnMgrDbg`**（自己实现的版本链打印器），
官方助教明确说「先写这个函数，才有资格来问问题」。

---

## 1. ★★★ 版本链：Base Tuple + UndoLog 链表

```56:84:src/include/concurrency/transaction.h
struct UndoLink {
  /* Previous version can be found in which txn */
  txn_id_t prev_txn_{INVALID_TXN_ID};
  /* The log index of the previous version in `prev_txn_` */
  int prev_log_idx_{0};
  auto IsValid() const -> bool { return prev_txn_ != INVALID_TXN_ID; }
};

struct UndoLog {
  bool is_deleted_;
  std::vector<bool> modified_fields_;
  Tuple tuple_;
  timestamp_t ts_{INVALID_TS};
  UndoLink prev_version_{};
};
```

**三层结构，缺一不可地组合使用**：

| 层 | 存在哪 | 内容 |
|---|---|---|
| Base tuple | `TableHeap`（page里） | 当前最新值+ `TupleMeta{ts_, is_deleted_}` |
| Version link | `TransactionManager::version_info_` | `RID → UndoLink`（指向第一条 undo log） |
| Undo log | `txn->undo_logs_`（每个事务自己的vector，**只能 append**） | 该次修改前的"旧样子" + 指向更早一条的链 |

**为什么 `UndoLog` 只存"被修改的列"（`modified_fields_`）而不是整行？**
省空间。`Update SET age=age+1` 只有 `age` 列变了，undo log 只记 `age` 的旧值，
其余列在重建时直接从**更新版本**里"借"过来（回放算法见第3 节）。

**为什么 `undo_logs_` 只能 append，不能删除中间元素？**
因为别处存的是 `(txn_id, index)` 这种"指针"（`UndoLink`），
数组一旦发生删除/重排，所有指向它的链接全部失效。GC 时只能整条txn 一起清。

---

## 2. ★★★ 时间戳体系：临时时间戳 vs 提交时间戳

```103:108:src/include/concurrency/transaction.h
inline auto GetTransactionId() const -> txn_id_t { return txn_id_; }
inline auto GetTransactionIdHumanReadable() const -> txn_id_t { return txn_id_ ^ TXN_START_ID; }
inline auto GetTransactionTempTs() const -> timestamp_t { return txn_id_; }
```

```35:53:src/include/common/config.h（关键常量，需要另行确认实际行号）
const txn_id_t TXN_START_ID = 1LL << 62;
```

事务未提交时**没有真正的时间戳**，只能用「临时时间戳」占位：

```
txn_id_        = TXN_START_ID + n            （n 从 0 递增，最高位是 1）
临时时间戳      = txn_id_ 本身                （对外表现成一个"极大"的 timestamp_t）
GetTransactionIdHumanReadable() = txn_id_ ^ TXN_START_ID→ 打印日志用，得到人类可读的 n
```

**读到一条 `ts_ >= TXN_START_ID` 的 base tuple/undo log 意味着什么？**
→ 这一版本是**某个尚未提交的事务**写的临时占位值，
不能直接当成"已提交的历史版本"使用，必须结合"这是不是我自己这个事务"来判断：

```
若 base_meta.ts_ == 自己的临时时间戳  → 我自己刚写的，直接能看见，无需走undo log
若 base_meta.ts_ 是别的临时时间戳     → 别人未提交的脏数据，我看不到，必须往undo log 更早版本找
若 base_meta.ts_ <= read_ts_（正常数字） → 已提交，且在我开始读之前提交，可见
若 base_meta.ts_ >  read_ts_（正常数字） → 已提交，但在我开始读之后才提交，对我不可见，往更早版本找
```

**Task #1 的两个函数**：

- `TransactionManager::Begin`：分配 `read_ts_ = last_commit_ts_`（当前系统已提交的最高时间戳），
  然后 `running_txns_.AddTxn(read_ts_)` 登记进watermark。
- `TransactionManager::Commit`：在 `commit_mutex_` 保护下，`commit_ts_ =++last_commit_ts_`
  （单调递增，且**全局只有一个事务能在提交**——这也是为什么要单独用一把 `commit_mutex_`）。

---

## 3. ★★★ReconstructTuple / CollectUndoLogs —— MVCC 读路径的核心

这两个函数是 `execution_common.cpp` 里最重要的两块，**几乎所有 P4 执行器都要调用它们**。

### `ReconstructTuple`：从新到旧回放，拼出某个历史版本

```50:51:src/include/execution/execution_common.h
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;
```

语义：`undo_logs` 是"从最新到最旧"排好的一串增量，**依次全部应用**（不看时间戳，调用方已经筛好了）。

```
起点 = base_tuple（当前最新值）
for log in undo_logs（数组下标 0 → N-1，即"最新的修改先被撤销"）:
    for 每个 modified_fields_[i] == true 的列 i:
        当前值[i] = log.tuple_ 里对应位置的值      // log.tuple_ 只含被改列，按顺序取
    若 log.is_deleted_ == true:
        当前"删除标记" = true（但仍继续应用后面的 log，因为可能被"复活"）
最终：如果一路回放到最后，删除标记为 true →返回 std::nullopt
      否则返回拼出来的 Tuple
```

**易错点**：`log.tuple_` 内部的列是"紧凑排列"的（只含`modified_fields_` 为 true 的那些列，
用一个只含这些列的临时 schema 构造），不能直接按原 schema 下标去取——
需要自己维护一个"从紧凑 tuple 里取第 j 个值"的游标（这也是为什么官方提示要写
`GetUndoLogSchema` 辅助函数）。

### `CollectUndoLogs`：从新到旧走链表，收集"我能看见"的那一段

```53:54:src/include/execution/execution_common.h
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>>;
```

三种情况（官方文档原话，是本函数最难的部分）：

1. **base tuple 本身对我可见**（已提交 且 `ts_ <= read_ts_`，或 `ts_`恰好是我自己的临时时间戳）
   → 不需要任何 undo log，直接返回 `{}`（空 vector，配合 base_tuple 直接可用）。
2. **base tuple 不可见**（被别的未提交事务改过，或提交时间在我 `read_ts_` 之后）
   → 沿 `undo_link` 顺着 `txn_mgr->GetUndoLog(link)` 一条条往前走，
     每条都检查 `log.ts_`，直到找到第一条 `ts_ <= read_ts_` 的，
     **连同它一起**加入结果并停止（这条以及更早的不需要，因为它已经可见了，
     但它本身必须被应用来"抹平"更新版本带来的差异）。
3. **走到链表尽头都没找到可见版本** → 说明这一行对我来说"还不存在"，返回 `std::nullopt`。

```cpp
//骨架（示意，不是最终实现）
if (IsVisible(base_meta.ts_, txn)) return {};
std::vector<UndoLog> logs;
auto link = undo_link;
while (link.has_value() && link->IsValid()) {
  auto log = txn_mgr->GetUndoLog(*link);
  logs.push_back(log);
  if (log.ts_ <= txn->GetReadTs()) return logs;   // 找到即停
  link = log.prev_version_;
}
return std::nullopt;  // 链表走完还不可见 → 这行对我不存在
```

> 🔥 **可见性判断要单独抽一个 helper**（官方叫它类似 `IsVisible`/`WalkUndoLogs`），
> 因为 SeqScan / IndexScan / Update / Delete 全部要用同一套判断逻辑，
> 写重复代码是 P4 最容易出的"逻辑不一致"bug 来源。

---

## 4. ★★☆ Watermark —— 谁的日志能被回收

```20:53:src/include/concurrency/watermark.h
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}
  auto AddTxn(timestamp_t read_ts) -> void;
  auto RemoveTxn(timestamp_t read_ts) -> void;
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }
  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) return commit_ts_;
    return watermark_;
  }
  timestamp_t commit_ts_;
  timestamp_t watermark_;
  std::unordered_map<timestamp_t, int> current_reads_;
};
```

**一句话**：`watermark_` = 当前所有「正在运行事务」里最小的 `read_ts_`。
比这个时间戳更早、且已经"不可能再被任何人看到"的 undo log，才能被 GC 安全删除。

**为什么要求 O(log N)？** 如果每次 `AddTxn`/`RemoveTxn` 都扫一遍 `current_reads_` 找最小值，是 O(N)。
官方要求用类似「多重集 + 最小堆/有序结构」的做法把插入删除都做到 O(log N)：

```
current_reads_: unordered_map<timestamp_t, int>   // 计数：这个 read_ts 有几个活跃事务在用
配合一个 multiset<timestamp_t>（或类似结构）维护有序性，取最小值O(log N) / O(1)

AddTxn(ts):    current_reads_[ts]++; 若从 0→1，插入有序结构；更新 watermark_ = 有序结构最小值
RemoveTxn(ts): current_reads_[ts]--; 若从 1→0，从有序结构删除该 ts；再更新 watermark_
```

**踩坑提示**：多个事务可能拿到**相同的** `read_ts_`（比如连续两次 `Begin` 之间没有任何`Commit`），
所以不能用 `set<timestamp_t>` 直接存（去重了），必须像上面一样先计数再决定要不要真正插入/删除有序结构。

---

## 5. ★★☆ 写写冲突与 TAINTED 状态

MVOCC 是**乐观**的：写的时候不加锁，但写之前必须检查「这行当前的最新版本，我是否有权覆盖」。

```
写写冲突定义：
  base_meta.ts_（这行最后一次被修改的时间戳）既不是我自己的临时时间戳，
  又>我的 read_ts_（即：在我读到这行之后，有另一个已提交/未提交的事务改过它）
  → 冲突！
```

发生冲突时：

```44:44:src/include/concurrency/transaction.h（TransactionState 定义）
enum class TransactionState { RUNNING = 0, TAINTED, COMMITTED = 100, ABORTED };
```

```cpp
txn->SetTainted();                // 状态机：RUNNING → TAINTED（其余状态转换会 std::terminate，很严格）
throw ExecutionException("write-write conflict");
```

TAINTED 事务**不能再做任何读写**，最终只能被 `Abort`（不能 `Commit`）——
这是官方特意设计的「一旦检测到不安全，立刻掐死」策略，避免脏读继续传播。

Update / Delete 执行器的核心骨架因此变成：

```
loop 从子节点批量拉 tuple：
  for每个待修改的 rid：
    (base_meta, base_tuple, undo_link) = GetTupleAndUndoLink(txn_mgr, table_heap, rid)   // 原子读
    若 base_meta.ts_ 冲突（见上）：SetTainted() + throw
    若这是"我自己这个事务第一次改这一行"：
        新 undo log = GenerateNewUndoLog(...)   // 记录 base_tuple 相对 new_tuple 的差异
        追加进 txn->undo_logs_，得到新 UndoLink
    否则（我已经改过这一行，undo_link 已经指向"我自己写的"那条日志）：
        更新 undo log = GenerateUpdatedUndoLog(...)   // 合并差异，而不是叠加新的一条
        原地 ModifyUndoLog 替换
    UpdateTupleAndUndoLink(...)   // 原子写：base tuple 换成新值(ts_=我的临时时间戳) + undo link 指过去
    AppendWriteSet(table_oid, rid)   // 记入 write set，供 Commit 时统一"钉上"真正的 commit_ts_
```

---

## 6. ★★☆ Commit 流程：commit_mutex_ + write set

```65:93:src/concurrency/transaction_manager.cpp
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  // TODO(P4): acquire commit ts!
  if (txn->state_ != TransactionState::RUNNING) throw Exception("txn not in running state");
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) { commit_lck.unlock(); Abort(txn); return false; }
  }
  // TODO(P4): Implement the commit logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(P4): set commit timestamp + update last committed timestamp here.
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}
```

**为什么要「全局只有一个事务能提交」（`commit_mutex_`）？**
因为 commit_ts_ 必须严格单调递增且不能有并发的两个事务拿到同一个值——
这是快照隔离正确性的地基（"提交顺序即时间戳顺序"）。

**Commit 要做的事（按顺序）**：

```
1. commit_ts_ = ++last_commit_ts_                ← 分配真正的时间戳
2. 遍历 txn->GetWriteSets() 里的每个 (table_oid, rid)：
     把 base tuple 的 TupleMeta.ts_ 从"我的临时时间戳"改写成 commit_ts_
     （数据内容不用动，只是把"占位时间戳"换成"真时间戳"）
3. txn->commit_ts_ = commit_ts_
4. running_txns_.UpdateCommitTs(commit_ts_) 再 RemoveTxn(read_ts_)   ← 顺序不能反，否则 watermark 算错
5. txn->state_ = COMMITTED
```

**`AppendWriteSet` 的作用**在这里完全体现：Commit 时不用重新扫表找"我改过哪些行"，
直接遍历这个 O(改动行数) 的集合即可。

---

## 7. ★☆☆ 索引与 MVCC 的矛盾：索引项永不删除

P3 里Delete/Update 会同步 `idx->index_->DeleteEntry(...)`。**P4 里不能再这样做**：

```
问题：事务 A 删除了一行并提交，但事务 B 的快照时间戳早于 A 的提交，B 应该仍能通过索引查到这行。
若索引项被物理删除 → B 查索引直接查不到，破坏了快照隔离。
```

**解决方案（官方要求）**：

- 索引里的一个 key 一旦插入，**永不删除**（除非发生"插入到一个已删除 tuple 位置"的复用场景，
  见下）。索引条目里存的 RID 指向 base tuple，可见性判断完全交给"回表后走版本链"这一步处理。
- Insert 时如果发现索引里已经有这个 key（`ScanKey` 命中）：
  - 若该 RID 对应的 base tuple 是**已删除**且**对所有人不可见**（早于 watermark）→ 可以复用这个 slot，
    走"原地插入新版本"逻辑（`UpdateTupleAndUndoLink`），不新建索引项。
  - 若该 RID 对应的行**仍然可能被别的事务看见**，或本身就没被删 → **唯一性冲突**，`SetTainted()` + throw。
- 涉及主键列的 Update，语义上要拆成「先删旧 key 对应行（逻辑删除 + 打undo log）+ 再插入新 key」，
  不能像P3 那样简单原地改。

---

## 8. ★☆☆ 原子读写：UpdateTupleAndUndoLink / GetTupleAndUndoLink

```126:162:src/concurrency/transaction_manager_impl.cpp
auto UpdateTupleAndUndoLink(TransactionManager *txn_mgr, RID rid, std::optional<UndoLink> undo_link,
                            TableHeap *table_heap, Transaction *txn, const TupleMeta &meta, const Tuple &tuple,
                            std::function<bool(...)> &&check) -> bool {
  auto page_write_guard = table_heap->AcquireTablePageWriteLock(rid);
  auto page = page_write_guard.AsMut<TablePage>();
  auto [base_meta, base_tuple] = page->GetTuple(rid);
  if (check != nullptr && !check(base_meta, base_tuple, rid, undo_link)) return false;
  if (meta != base_meta || !IsTupleContentEqual(tuple, base_tuple)) {
    table_heap->UpdateTupleInPlaceWithLockAcquired(meta, tuple, rid, page);
  }
  txn_mgr->UpdateUndoLink(rid, undo_link);
  return true;
}
```

**这是「本文件不用改」的框架代码，但必须理解它给的保证**：

- 持有 page 的写锁期间，`check` 回调可以安全地"读到最新值再决定要不要写"——
  这正是写写冲突检测天然要用的钩子（把冲突判断塞进 `check` 里，一次锁内完成"读-判断-写"）。
- `GetTupleAndUndoLink` 同理，用读锁把"读 base tuple + 读 undo link"这两步在原子性上绑在一起，
  避免"读到 tuple 后，link 被另一个线程改了"这种竞态。

**执行器写代码时的铁律**：永远通过这两个函数访问 table heap + version_info_，
**不要**分两步自己手写（`GetTuple()` 再单独 `GetUndoLink()`），否则并发下会读到不一致的组合。

---

# 第二部分：任务与进度

## 任务清单与进度表

### Task #1 — 时间戳（Timestamps）

| 文件/函数 | 状态 | 要点 |
|---|---|---|
| `TransactionManager::Begin` | ☐ | 分配 `read_ts_ = last_commit_ts_`，`AddTxn` 登记 watermark |
| `TransactionManager::Commit`（时间戳部分） | ☐ | `commit_mutex_` 下 `commit_ts_ = ++last_commit_ts_` |
| `Watermark::AddTxn` / `RemoveTxn` | ☐ | O(log N)，注意重复 `read_ts_` 的计数语义 |

通过标准：`txn_timestamp_test`（把 `DISABLED_` 前缀去掉后跑）。

### Task #2 — 存储格式与顺序扫描

| 文件/函数 | 状态 | 要点 |
|---|---|---|
| `ReconstructTuple` | ☐ | 从新到旧回放 undo log，命中删除标记返回 `std::nullopt` |
| `CollectUndoLogs` | ☐ | 判可见性 + 沿链收集，三种情况见概念第 3 节 |
| `SeqScanExecutor`（重写） | ☐ | 每行都要走 `GetTupleAndUndoLink` →可见性判断 → 必要时 `CollectUndoLogs` + `ReconstructTuple` |

通过标准：`txn_scan_test`。

### Task #3 — MVCC 执行器

| 文件/函数 | 状态 | 要点 |
|---|---|---|
| `InsertExecutor`（重写） | ☐ | 新 tuple 的 `TupleMeta.ts_` 设为**临时时间戳**；`AppendWriteSet` |
| `TransactionManager::Commit`（写入部分） | ☐ | 遍历 write set，把 base tuple 的 `ts_` 从临时值改成真正 `commit_ts_` |
| `GenerateNewUndoLog` | ☐ | 首次修改：整理`modified_fields_` + 旧值 + `prev_version_` |
| `GenerateUpdatedUndoLog` | ☐ | 二次修改：与已有 undo log **合并**差异，不是叠加 |
| `TxnMgrDbg` | ☐ | **强烈建议先写**，打印版本链，调试利器 |
| `UpdateExecutor` / `DeleteExecutor`（重写） | ☐ | 写写冲突检测 → `SetTainted()` + throw；Update 需先缓冲全部子节点结果（pipeline breaker） |
| `TransactionManager::GarbageCollection` | ☐ | Stop-the-world：基于 `GetWatermark()` 清理不再可见的 undo log |

通过标准：`txn_executor_test`。

### Task #4 — 主键索引

| 文件/函数 | 状态 | 要点 |
|---|---|---|
| 4.1 索引插入 | ☐ | 并发插入同一 key 检测唯一性冲突→ `SetTainted()`；完成即**85 分** |
| 4.2 `IndexScanExecutor`（重写） + 索引删除/更新 | ☐ | 索引项永不物理删除；"插入到已删除 tuple 位置"的复用场景 |
| 4.3 主键更新| ☐ | 拆成「删旧 key + 插新 key」两步 |

通过标准：`txn_index_test`、`txn_index_concurrent_test`。完成整个 Task 4 即 **100 分**。

### ★Bonus Task #1 — Abort（可选）

| 内容 | 状态 | 要点 |
|---|---|---|
| `TransactionManager::Abort` | ☐ | 方案一：直接把 tuple 恢复原值（简单，版本链多一条冗余）；方案二：原子改undo link 跳过自己（更省空间，可立即从 txn_map 摘除） |

要求支持多线程并行 abort，**不能用全局锁**。

### ★Bonus Task #2 — Serializable Verification（可选）

| 内容 | 状态 | 要点 |
|---|---|---|
| `Transaction::AppendScanPredicate` 已给字段 | ✅ | 只需在 SeqScan/IndexScan 里调用它记录读集 |
| `TransactionManager::VerifyTxn` | ☐ | OCC 后向验证：检查读集是否与"提交时间在自己 read_ts_ 之后的事务"的写集相交 |

通过标准：`txn_abort_serializable_test`。

---

## 建议推进顺序

```
第 0 步读懂 UndoLink / UndoLog / TupleMeta 三层结构，画一遍版本链的图    ← 别跳过
        ↓
第 1 步  Task #1 时间戳 + Watermark（最独立，纯逻辑，先用 txn_timestamp_test 打底）
        ↓
第 2 步  execution_common.cpp：ReconstructTuple → CollectUndoLogs
        ↓
第 3 步  重写 SeqScanExecutor（只读，最先能验证可见性逻辑）  ★ txn_scan_test
        ↓
第 4 步  TxnMgrDbg（花30 分钟写好，后面全靠它调试）
        ↓
第 5 步  InsertExecutor + Commit 的写入逻辑（打通"写-提交"最短路径）
        ↓
第 6 步  GenerateNewUndoLog / GenerateUpdatedUndoLog
        ↓
第 7 步  UpdateExecutor / DeleteExecutor（写写冲突检测）        ★ txn_executor_test
        ↓
第 8 步  GarbageCollection
        ↓
第 9 步  索引：Insert 唯一性冲突 → IndexScan 重写 → 索引 Delete/Update → 主键更新
        ★ txn_index_test / txn_index_concurrent_test
        ↓
第 10 步（可选）Bonus: Abort → Serializable Verification
```

**关键判断**：P4 和 P3 不同，**深度优先**更合适——先把"一条 tuple 完整的读写生命周期"跑通
（Begin → Insert → Commit → 另一个事务 Scan 看到），再逐个补边界情况（冲突、GC、索引）。

---

## 构建与测试

### P4 用 gtest，且默认全部 `DISABLED_`

```16:29:test/txn/txn_scan_test.cpp（示例）
TEST(TxnScanTest, DISABLED_TupleReconstructTest) {  // NOLINT
```

**必须先把要跑的测试名里的 `DISABLED_` 删掉**（这是 bustub 的常规套路，避免脚手架代码跑起来直接崩）。

### 命令

```bash
# Debug + AddressSanitizer（同 P3，必须用 clang）
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# 编译并跑单个测试目标（目标名 = 文件名去掉 .cpp）
make txn_timestamp_test -j$(nproc) && ./test/txn_timestamp_test
make txn_scan_test -j$(nproc) && ./test/txn_scan_test
make txn_executor_test -j$(nproc) && ./test/txn_executor_test
make txn_index_test -j$(nproc) && ./test/txn_index_test
make txn_index_concurrent_test -j$(nproc) && ./test/txn_index_concurrent_test
make txn_abort_serializable_test -j$(nproc) && ./test/txn_abort_serializable_test   # Bonus

# 也可以用 ctest跑指定名字（去掉 DISABLED_ 后的 test 名）
ctest -R TxnScanTest --output-on-failure
```

### `TxnMgrDbg` 是P4 最重要的调试工具

官方给的示例输出格式：

```
debug_hook: before verify scan
RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  txn8@0 (2, _, _) ts=1
RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  txn5@0 <del> ts=2
  txn3@0 (4, <NULL>, <NULL>) ts=1
```

第一行是 base tuple + 它的 `ts_`（如果是临时时间戳就打印成 `txnN`），
后面每行是这条 RID 的版本链，从最新的 undo log 往旧打印。
**建议在每个测试用例的关键节点手动调用它**，肉眼比对链表是否符合预期，
比单步调试快得多——官方助教在office hour 也是先看这个输出。

---

# 第三部分：实现记录

## 实现记录

### 进度

尚未开始（本笔记创建于开工前，用于梳理概念）。

### 1. 时间戳与 Watermark

（占位，实现后补充：`Begin`/`Commit` 时间戳分配细节、`Watermark` 内部数据结构选择与复杂度证明）

### 2. ReconstructTuple / CollectUndoLogs

（占位，实现后补充：紧凑 tuple 的取值游标写法、可见性判断 helper 的最终签名）

### 3. MVCC 执行器改造

（占位，实现后补充：Insert/Update/Delete 的写写冲突检测代码、`TxnMgrDbg` 实现）

### 4. 索引 MVCC

（占位，实现后补充：唯一性冲突判断、"复用已删除 slot"逻辑、主键更新拆分）

---

## 踩坑记录

### 坑 1：<一句话症状>

（占位）

---

## 遗留问题

- Bonus Task #1（Abort）与 Bonus Task #2（Serializable Verification）暂未开始。
- `lock_manager.cpp` / `test/concurrency/*.disabled` 是旧版基于 2PL 的 Project4 遗留代码，
  当前 MVCC 版本**不需要**实现它（`AddEdge`/`RemoveEdge`/`HasCycle` 均为空实现，测试文件已 `.disabled`）。

---

# 附：P3 是否必须先做完？

**不是硬性前提，但 P1/P2 必须正确。** 结论与依据：

1. **P1、P2 的正确性直接影响 P4**：`TableHeap`、`B+Tree` 索引、`BufferPoolManager` 都是 P4 的地基，
   这两部分的 bug 会直接表现成 P4 的诡异失败。
2. **P3 的访问类执行器（SeqScan/Insert/Delete/Update/IndexScan）会在 P4 里整体重写**——
   官方文档原话："access method executors need to be rewritten for MVCC"，
   所以哪怕这些文件在 P3 阶段没写完/写得简陋，也不影响开始 P4
   （反正要按第二部分任务清单重新写一遍）。
3. **P3 的聚合/连接/排序/TopN/外排序/窗口函数**（Task2~4）**只有在做 leaderboard 加分测试时才需要**，
   核心的 P4 评分测试（`txn_*_test` 系列）只涉及 scan / insert / update / delete / index，
   完全不依赖 join、aggregation、sort 等算子。
4. 因此实际可行的路径是：**确保 P1/P2 全绿→ 直接开始 P4**，
   P3 里没完成的聚合/连接/排序部分可以留到最后，只在追加 leaderboard 分数时再补。

一句话："P3 不是 P4 的前置任务，P1/P2 才是。P3 的访问类执行器在 P4 里反正要重写。"
