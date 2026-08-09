# BusTub P3 实现笔记 — Query Execution

> Project 3（查询执行）的概念地基、任务清单与实现记录。
>
> 代码基线：`master`（P1/P2 已完成，P1 16/16、P2 18/18 全绿）。
> 相关笔记：[`NOTES-P1-P2.md`](./NOTES-P1-P2.md)
>
> **本文档是活文档**：前半部分是开工前必须搞清楚的概念（按重要性排序），
> 后半部分是实现记录与踩坑记录，边做边往里填。

---

## 目录

**第一部分：概念地基（★ 越多越重要）**

- [0. P3 是什么 · 一句话定位](#0-p3-是什么--一句话定位)
- [1. ★★★ 批量 Next 接口 —— 今年最大的坑](#1--批量-next-接口--今年最大的坑)
- [2. ★★★ Tuple 与 Schema —— 所有「结果错但不崩」的根源](#2--tuple-与-schema--所有结果错但不崩的根源)
- [3. ★★☆ 火山模型与数据流](#3--火山模型与数据流)
- [4. ★★☆ 流式算子 vs Pipeline Breaker](#4--流式算子-vs-pipeline-breaker)
- [5. ★★☆ 索引查找：拿 key 查 RID，再回表](#5--索引查找拿-key-查-rid再回表)
- [6. ★☆☆ ExecutorContext —— P3 与 P1/P2 的接缝](#6--executorcontext--p3-与-p1p2-的接缝)
- [7. ★☆☆ TupleMeta 与逻辑删除](#7--tuplemeta-与逻辑删除)

**第二部分：任务与进度** → [任务清单](#任务清单与进度表) · [推进顺序](#建议推进顺序) · [构建与测试](#构建与测试)

**第三部分：实现记录** → [实现记录](#实现记录) · [踩坑记录](#踩坑记录) · [遗留问题](#遗留问题)

---

# 第一部分：概念地基

## 0. P3 是什么 · 一句话定位

> **一台把「计划树」变成「数据流」的机器。**

P1 解决「页在内存和磁盘之间怎么搬」，P2 解决「怎么按 key 快速找到一行」。
**P3 把这两层接起来，让系统真的能执行 SQL。**

```
SQL 字符串
   ├─ ① Binder      字符串 → BoundStatement（表名变 oid，列名变下标）   框架给定
   ├─ ② Planner     BoundStatement → AbstractPlanNode 树                框架给定
   ├─ ③ Optimizer   计划树 → 更好的计划树                          ★ P3（4 条规则）
   ▼
   ④ ExecutionEngine ──► ExecutorFactory 递归建 executor 树        ★ P3（主体 15 个）
   │                PollExecutor 反复摇根节点
   ├──► ⑤ Catalog        查表 / 索引元信息
   ├──► ⑥ TableHeap      读写数据行（RID 寻址）
   ├──► ⑦ BPlusTree      索引查找                 ← 我的 P2
   └──► ⑧ BufferPoolMgr  页面换入换出              ← 我的 P1
```

**为什么这层不能省**

| 作用 | 说明 |
|---|---|
| 声明式→ 过程式 | SQL 说「我要 x>5 的行」，P3 决定「怎么一行行找出来」 |
| **内存与数据量解耦** | 1TB 的表用 100MB 内存扫完。这是执行引擎存在的根本理由 |
| 算子任意组合 | 接口统一 → `Limit(Sort(Agg(Filter(SeqScan))))` 随意嵌套，加算子不改别人 |
| **支持提前终止** | `LIMIT 3` 拿够就不再向下要。见 [pull 模型](#为什么是-pull-而不是-push) |

**bug 特征**：P3 的 bug 很少是「崩」，多半是「结果不对」——
executor 之间只传 tuple 拷贝，写错不会内存越界，只会算出错误答案。
→ **ASan 帮不上忙，`EXPLAIN` 才是主武器。**

---

## 1. ★★★ 批量 Next 接口 —— 今年最大的坑

```50:51:src/include/execution/executors/abstract_executor.h
  virtual auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool = 0;
```

**不是经典 BusTub 的「一次一 tuple」，而是一次返回一批（`BUSTUB_BATCH_SIZE`）。**

> ⚠️ **网上绝大多数 15-445 P3 题解都是单 tuple 版本，直接照抄会写不对。**

### Executor 不是函数，是有状态的迭代器

`Next()` 会被**反复调用**，两次调用之间「我上次干到哪了」必须由 executor
**自己存成成员变量**。可以想成一台**手摇发电机**：摇一下出一批电，它必须记住摇到哪个角度。

### 阻抗匹配问题

```
子节点给我一批 1000 个
    ↓ 我过滤/变换后可能：
  只剩 200 个   → 不够一批，得再向子节点要
  变成 2000 个  → 超了一批，得记住位置分两次吐
```

### 官方给了完整参考实现：`filter_executor.cpp`

**入门 P3 最该逐行读的文件。** 抄走三个模式：

**模式 1** 入口先`clear()` 两个输出 vector（调用方会复用同一个 vector）

**模式 2** `child_offset_` 断点续传

```51:52:src/execution/filter_executor.cpp
    if (child_offset_ != 0) {
      for (size_t i = child_offset_; i < child_tuples_.size(); ++i) {
```

**模式 3** 三个 return 点对应三种终止情况

```69:77:src/execution/filter_executor.cpp
    // If no more tuples and output batch is empty, return false
    if (!status && tuple_batch->empty()) {
      return false;
    }

    // If no more tuples but output batch is not empty, return true
    if (!status && !tuple_batch->empty()) {
      return true;
    }
```

> 🔥 **第二个 return 最容易漏**：子节点说「没了」，但自己还攥着没吐出去的数据 ——
> 必须 `return true`。写成 `return false` 会**静默丢掉最后一批**，
> 症状是「结果少了几行」，且**只在数据量不是 `batch_size` 整数倍时出现**。

攒满一批时记录断点：

```87:97:src/execution/filter_executor.cpp
        if (tuple_batch->size() >= batch_size) {
          // If we have filled the output batch but not yet reached the end of the current child batch, update the
          // offset and return
          if (i + 1 < child_tuples_.size()) {
            child_offset_ = i + 1;
          } else {
            child_offset_ = 0;
          }

          return true;
        }
```

其他给定的完整实现：`projection_executor.cpp`、`values_executor.cpp`、`mock_scan_executor.cpp`。

---

## 2. ★★★ Tuple 与 Schema —— 所有「结果错但不崩」的根源

> **Tuple 是一串没有自我描述能力的字节。Schema 是解读这串字节的说明书。**
> 两者必须成对使用。

### Tuple 只有两个成员

```111:116:src/include/storage/table/tuple.h
 private:
  auto GetDataPtr(const Schema *schema, uint32_t column_idx) const -> const char *;

  RID rid_{};  // if pointing to the table heap, the rid is valid
  std::vector<char> data_;
```

- `data_` —— 序列化后的裸字节，**不知道自己有几列、每列什么类型**
- `rid_` —— 这行在表堆里的物理地址 `{page_id, slot_num}`

**值语义，拷贝即深拷贝**（`data_` 是 `std::vector<char>`）：

```74:78:src/include/storage/table/tuple.h
  // assign operator, deep copy
  auto operator=(const Tuple &other) -> Tuple & = default;

  // move assignment
  auto operator=(Tuple &&other) noexcept -> Tuple & = default;
```

→ **这是「executor 之间传数据副本、不传页面指针」的技术原因**，
所以 SeqScan 读完一页可以马上放锁走人，不用把页锁攥到查询结束。

### Column 的关键字段是 `column_offset_`

```120:130:src/include/catalog/column.h
  /** Column name. */
  std::string column_name_;

  /** Column value's type. */
  TypeId column_type_;

  /** The size of the column. */
  uint32_t length_;

  /** Column offset in the tuple. */
  uint32_t column_offset_{0};
```

`column_offset_` = **这列数据在 tuple 字节数组里从第几个字节开始**。
不是手写的，是 `Schema` 构造时累加算出来的（`src/catalog/schema.cpp:23-36`）。

定长/变长判定：

```86:87:src/include/catalog/column.h
  /** @return true if column is inlined, false otherwise */
  auto IsInlined() const -> bool { return column_type_ != TypeId::VARCHAR && column_type_ != TypeId::VECTOR; }
```

### 完整的字节布局例子

```sql
CREATE TABLE t1(a int, b varchar(20), c int);   -- 插入 (7, "hello", 42)
```

| 列 | 类型 | IsInlined | `length_` | `column_offset_` |
|---|---|---|---|---|
| a | INTEGER | ✅ | 4 | **0** |
| b | VARCHAR(20) | ❌ | 20 | **4** ← 只存 4 字节偏移量 |
| c | INTEGER | ✅ | 4 | **8** |

`GetInlinedStorageSize()` = 4 + 4 + 4 = **12**（b 在定长区只占 4 字节，不是 20）

```
data_ 实际字节（21 字节）：

字节:  0  1  2  3 │ 4  5  6  7 │ 8  9 10 11 │12 13 14 15│16 17 18 19 20
      ┌───────────┬────────────┬────────────┬──────────┬─────────────┐
      │  a = 7    │ b 的偏移=12│   c = 42   │"hello"长5│  h e l l o  │
      └───────────┴────────────┴────────────┴──────────┴─────────────┘
      └──── 定长区 12 字节（Schema 决定）────┘└─── 变长区（追加在尾部）───┘
```

```75:87:src/storage/table/tuple.cpp
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
      // Serialize varchar value, in place (size+data).
      values[i].SerializeTo(data_.data() + offset);
```

**定长列原地存值；变长列在定长区存「指路牌」，真实数据追加在尾部。**

### 取值：三步走

```80:86:src/storage/table/tuple.cpp
auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}
```

① 问 Schema 要类型 → ② 问 Schema 要地址 → ③ 按类型反序列化成 `Value`

```107:111:src/storage/table/tuple.cpp
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  return (data_.data() + offset);
```

定长一次寻址；变长两次寻址（先读偏移量，再跳过去）。

### 🔥 传错 Schema 的后果

```cpp
// 正确：schema = (a int, b varchar, c int)，第 2 列偏移 8
tuple.GetValue(&correct_schema, 2);   // 读 data_[8..11] → 42  ✅

// 传错：schema = (x int, y int)，第 1 列偏移 4
tuple.GetValue(&wrong_schema, 1);     // 读 data_[4..7] → 12（b 的偏移量！）  ❌
```

**不崩、不报错，静默返回荒谬的数字。这是 P3 最高频、最难查的 bug。**

### Schema 一路在变，所以每层都要 `GetOutputSchema()`

```
SeqScan     输出 (a int, b varchar, c int)   吐: [7][12][42]["hello"]
   ▼
Filter      输出 (a int, b varchar, c int)   不变，只筛行
   ▼
Projection  输出 (c int)                     变了！吐: [42]
   ▼
Aggregation 输出 (count_star int, c int)     又变了！吐: [3][42]
```

### 🔑 铁律

```cpp
// 求值来自【子节点】的 tuple → 必须用【子节点】的 schema
expr->Evaluate(&tuple, child_executor_->GetOutputSchema());   // ✅

// 用自己的输出 schema 解读子节点的 tuple → 静默读错列
expr->Evaluate(&tuple, plan_->OutputSchema());                 // ❌ 除非两者恰好一致
```

> **判断标准：这个 tuple 是谁生产的，就用谁的 schema。**

框架示范：

```56:56:src/execution/filter_executor.cpp
        auto value = filter_expr->Evaluate(&tuple, child_executor_->GetOutputSchema());
```

### 反向：从 Value 造 Tuple

```65:65:src/include/storage/table/tuple.h
  Tuple(std::vector<Value> values, const Schema *schema);
```

`Projection` / `Aggregation` / `Insert` 输出结果都用它。
**有断言 `values.size() == schema->GetColumnCount()`**，值的个数必须和 schema 列数一致。

### 四个概念的关系

| 概念 | 是什么 | 数量 |
|---|---|---|
| `Schema` | 字节布局说明书（类型 + 偏移） | 每个算子输出一份 |
| `Column` | 一列的元信息，关键是 `column_offset_` | 每列一个 |
| `Tuple` | 裸字节 + RID，**无自我描述能力** | 每行一个 |
| `Value` | 反序列化后的运行时值，带类型，能比较/运算 | 求值时临时产生 |

```
Schema ──►「说明书」：第 i 列什么类型、在第几字节
              │
Tuple.data_ ──┤ 按说明书解读
              ▼
           Value ← 运行时值，表达式求值都在这一层
```

---

## 3. ★★☆ 火山模型与数据流

### 每个 executor 只拿到三样东西

```cpp
XxxExecutor(ExecutorContext *exec_ctx,   // ① 环境：通往整个系统的唯一入口
            const XxxPlanNode *plan,      // ② 说明书：我该干什么（编译期算好）
            std::unique_ptr<AbstractExecutor> &&child_executor)  // ③ 上游：数据从哪来
```

- `plan` 是**编译期算好的说明书**：输出 schema、谓词、聚合列表、join key……
  **executor 只是照说明书干活的工人，不做决策**（决策在优化器）。
- `child_executor` 是 `unique_ptr` —— 父节点**独占拥有**子节点，析构树只需析构根。

### 控制流向下，数据流向上

```
                     ┌─── 控制流（Next 调用）向下 ───┐
   PollExecutor                                    │
      │ Next(batch=1000)                           ▼
   Limit ──────────► Sort ──────► Agg ──────► Filter ──────► SeqScan
      ◄──────────────────────────────────────────────────────────┘
                     └─── 数据流（Tuple 拷贝）向上 ───┘
```

```95:104:src/include/execution/execution_engine.h
  static void PollExecutor(AbstractExecutor *executor, const AbstractPlanNodeRef &plan,
                           std::vector<Tuple> *result_set) {
    std::vector<RID> rids{};
    std::vector<Tuple> tuples{};
    while (executor->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
      if (result_set != nullptr) {
        result_set->insert(result_set->end(), tuples.begin(), tuples.end());
      }
    }
  }
```

### 为什么是 pull 而不是 push

```sql
SELECT * FROM huge_table LIMIT 3;
```

- **push**（自底向上推）：SeqScan 把 1 亿行往上推，Limit 只能一边收一边丢。白干。
- **pull**（自顶向下拉）：Limit 拿到 3 行后**就不再调 `Next()`**，SeqScan 自然停在第 3 行。

> **「谁需要，谁去要」—— pull 模型的价值，也是提前终止能力的来源。**

### 数据物理上住在哪

```
① 磁盘文件  →  DiskManager::ReadPage  →  字节读进 buffer pool 的某个 frame
② buffer pool frame（P1）
   SeqScan 通过 TableIterator 持有 ReadPageGuard，数据是 slotted page 裸字节
③ TableHeap::GetTuple(rid) 返回 std::pair<TupleMeta, Tuple>
   ★ 返回【值拷贝】！所以 guard 释放后 tuple 依然有效
④ Tuple 在 executor 之间传递（tuple_batch 里是一堆 Tuple 拷贝）
   ★ 传数据副本，不传页面指针 → 执行期间不需一直握页锁
⑤ expr->Evaluate(&tuple, schema) → Value
```

**第 ③ 步的值语义是整个设计的关键。** 代价是内存拷贝，收益是并发度。

### 为什么 `Next()` 还要输出 `rid_batch`

`DELETE` / `UPDATE` 需要知道**这行物理上在哪**：

```
child(SeqScan) 吐出 (tuple, rid)
     ↓ DeleteExecutor 不关心 tuple 内容，要的是 rid
TableHeap::UpdateTupleMeta({ts, is_deleted=true}, rid)
（索引则要用 tuple 算出 key 来删）
```

上层算子（Agg/Sort）不需要 RID，但**写路径离不开它**。

---

## 4. ★★☆ 流式算子 vs Pipeline Breaker

**这个区分直接决定你的代码写在 `Init()` 还是 `Next()` 里。**

| 类型 | 算子 | 干活位置 | 内存 |
|---|---|---|---|
| **流式** | SeqScan、IndexScan、Filter、Projection、Limit、NLJ、NestedIndexJoin | `Next()` | O(一批) |
| **阻塞** | Sort、Aggregation、TopN、HashJoin build 侧、ExternalMergeSort、WindowFunction | `Init()` | O(数据量) 或 O(N) |

**为什么聚合必须阻塞？** `SUM(x)` 在看到最后一行之前算不出来，不可能边读边吐。

框架把结构暗示在头文件里：

```191:195:src/include/execution/executors/aggregation_executor.h
  /** Simple aggregation hash table */
  // TODO(Student): Uncomment SimpleAggregationHashTable aht_;

  /** Simple aggregation hash table iterator */
  // TODO(Student): Uncomment SimpleAggregationHashTable::Iterator aht_iterator_;
```

一个哈希表 + 一个指向它的迭代器 = **「`Init()` 建表，`Next()` 用迭代器吐」**：

```cpp
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {   // 彻底抽干
    for (auto &t : tuples) {
      aht_.InsertCombine(MakeAggregateKey(&t), MakeAggregateValue(&t));
    }
  }
  aht_iterator_ = aht_.Begin();   // Next() 从这里开始吐
}
```

`MakeAggregateKey` / `MakeAggregateValue` 框架已给：

```166:182:src/include/execution/executors/aggregation_executor.h
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }
```

要填的只是 `CombineAggregateValues` 的 `switch`（五种聚合类型各怎么合并）。
初始值也给了：`CountStar` 从 0 开始，其余从 NULL 开始（`aggregation_executor.h:45-63`）。

---

## 5. ★★☆索引查找：拿 key 查 RID，再回表

> **拿 key tuple 查，查出 RID。索引里不存数据行，只存地址。**

### 三次形态转换

```
① 完整数据行 Tuple      (a=7, b="hello", c=42)   schema = (a,b,c)
        │  Tuple::KeyFromTuple(schema, key_schema, key_attrs)
        ▼
② key tuple(c=42)                   schema = (c int)   ← Index 接口收这个
        │  GenericKey<8>::SetFromKey(key_tuple)
        ▼
③ GenericKey<8>         [42,0,0,0,0,0,0,0]       ← B+Tree 真正比较的东西（定长！）
```

`CREATE INDEX idx_c ON t1(c)` 产生两组关键元数据：

- **`key_attrs_ = {2}`** —— 被索引列在**原表**里的下标
- **`key_schema_ = (c int)`** —— 从原表 schema 按 `key_attrs_` 抠出的子 schema

```53:53:src/include/storage/index/index.h
    key_schema_ = std::make_shared<Schema>(Schema::CopySchema(tuple_schema, key_attrs_));
```

`KeyFromTuple` 三个参数各司其职：`schema`=怎么读、`key_attrs`=读哪几列、`key_schema`=怎么写。

变长 → 定长（`BPlusTreeIndex` 这层薄封装的全部工作）：

```32:36:src/include/storage/index/generic_key.h
  inline void SetFromKey(const Tuple &tuple) {
    // initialize to 0
    memset(data_, 0, KeySize);
    memcpy(data_, tuple.GetData(), tuple.GetLength());
  }
```

### Index 的三个接口，输出全是 RID

```179:185:src/include/storage/index/index.h
  /**
   * Search the index for the provided key.
   * @param key The index key
   * @param result The collection of RIDs that is populated with results of the search
   * @param transaction The transaction context
   */
  virtual void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) = 0;
```

`ValueType` 永远是 `RID`，只有 `KeyType` 在变：

```65:69:src/storage/index/b_plus_tree_index.cpp
template class BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>;
```

### 读路径：IndexScan 的数据流

```
① 从 plan 拿常量 42，构造 key tuple
② index_->ScanKey(key, &rids, txn)
     └─► SetFromKey → GenericKey<8> → BPlusTree::GetValue()   ← 我的 P2
③ ★ 回表：for (rid : rids) TableHeap::GetTuple(rid)           ← 我的 P1
     └─► 必须跳过 meta.is_deleted_
```

**回表是必须的**：索引里只有 `c=42` 和一个地址，`a`/`b` 的值不在索引里。

| | SeqScan | IndexScan |
|---|---|---|
| 怎么定位 | `MakeIterator()` 从头到尾 | key → 索引 → RID |
| 100 万行找 1 行 | 读 ~1 万页 | 读 ~5 页（树高 + 回表） |

这就是 `OptimizeSeqScanAsIndexScan` 的全部价值。

### 写路径：为什么必须同步更新索引

```
新行 tuple = (a=7, b="hi", c=42)
  ├─► ① TableHeap::InsertTuple(meta, tuple) → 返回 rid
  └─► ② 对该表【每一个】索引：
          key = tuple.KeyFromTuple(表schema, idx->key_schema_, idx->index_->GetKeyAttrs())
          idx->index_->InsertEntry(key, rid, txn)
```

现成示范可抄：

```254:254:src/include/catalog/catalog.h
      index->InsertEntry(tuple.KeyFromTuple(schema, key_schema, key_attrs), tuple.GetRid(), txn);
```

**两个参数来自不同地方**：`key` 从数据内容抽，`rid` 从堆的插入结果来。
→ **顺序不能反：必须先插堆拿到 RID，才能插索引。**

> 🔥 **P3 最典型的 bug：只插堆没插索引。**
> 症状隐蔽 —— `SELECT *`（走 SeqScan）能查到，`WHERE c = 42`（走 IndexScan）查不到，
> 而且要等优化器转成 IndexScan 后才暴露。

`UPDATE` 更要小心：**RID 会变**（本质是 delete + insert），
索引必须**先 `DeleteEntry(旧key, 旧rid)` 再 `InsertEntry(新key, 新rid)`**。

### 一张图

```
   SeqScan 路径                IndexScan 路径
        │                       key tuple（只含被索引列）
        │                                │
        │                          GenericKey<N>
        │                                │
        │                    BPlusTree::GetValue()  ← P2
        │                                │
        │                             ┌─ RID ─┐
        └──────► TableHeap ◄──────────┘回表  │
                    │                         │
              RID 寻址 {page_id, slot_num}      │
                    │                         │
              BufferPoolManager ← P1           │
                    │                         │
              完整 Tuple (a,b,c) ──────────────┘
```

---

## 6. ★☆☆ ExecutorContext —— P3 与 P1/P2 的接缝

Executor 想访问任何系统资源，**唯一入口就是 `exec_ctx_`**：

```62:66:src/include/execution/executor_context.h
  /** @return the catalog */
  auto GetCatalog() -> Catalog * { return catalog_; }

  /** @return the buffer pool manager */
  auto GetBufferPoolManager() -> BufferPoolManager * { return bpm_; }
```

- `GetCatalog()` → `TableInfo` / `IndexInfo` → **我的 P2 的 B+Tree**
- `GetBufferPoolManager()` → **我的 P1 的 ARC 缓冲池**（外排序自己申请页时用）

常用组合：

```cpp
auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
```

### 框架埋的检查：NLJ 右子节点必须每次Init

```78:86:src/include/execution/execution_engine.h
  void PerformChecks(ExecutorContext *exec_ctx) {
    for (const auto &[left_executor, right_executor] : exec_ctx->GetNLJCheckExecutorSet()) {
      auto casted_left_executor = dynamic_cast<const InitCheckExecutor *>(left_executor);
      auto casted_right_executor = dynamic_cast<const InitCheckExecutor *>(right_executor);
      BUSTUB_ASSERT(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount(),
                    "nlj check failed, are you initializing the right executor every time when there is a left tuple? "
                    "(off-by-one is okay)");
    }
  }
```

---

## 7. ★☆☆ TupleMeta 与逻辑删除

```30:41:src/include/storage/table/tuple.h
struct TupleMeta {
  /** the ts / txn_id of this tuple. In project 3, simply set it to 0. */
  timestamp_t ts_;
  /** marks whether this tuple is marked removed from table heap. */
  bool is_deleted_;
```

**它不在 `Tuple` 里面**，由 `TablePage` 单独存在 slot 数组里，所以取数据总是成对返回：

```cpp
auto [meta, tuple] = table_info_->table_->GetTuple(rid);
if (meta.is_deleted_) { continue; }   // ★ 所有扫描算子都必须检查
```

- `ts_` 在 P3 里**一律填 0**（注释明说），P4 才用它做 MVCC
- `is_deleted_` 是 P3 重点：**DELETE 不物理删除，只置这个标记**

> 🔥 忘了检查 `is_deleted_` →「删掉的行还能查出来」。

---

# 第二部分：任务与进度

## 任务清单与进度表

### Task #1 — 访问与修改（5 个）

| 文件 | 状态 | 要点 |
|---|---|---|
| `seq_scan_executor.cpp` | ✅ | `TableHeap::MakeIterator()`；**跳过 `is_deleted_`**；谓词下推后在这里求值（`filter_predicate_` 待补） |
| `insert_executor.cpp` | ☐ | 插堆拿 RID → **同步更新所有索引**；输出「只含受影响行数的一个 tuple」 |
| `delete_executor.cpp` | ☐ | `UpdateTupleMeta` 置 `is_deleted_`；索引也要删 |
| `update_executor.cpp` | ☐ | 本质 delete + insert；**RID 会变**，索引先删后插 |
| `index_scan_executor.cpp` | ☐ | `ScanKey` 拿 RID → **回表** `GetTuple(rid)` |

### Task #2 — 聚合与连接（4 个 + 1 个头文件函数）

| 文件 | 状态 | 算法 |
|---|---|---|
| `aggregation_executor.cpp` | ☐ | 哈希聚合，`Init()` 抽干子节点建表 |
| `aggregation_executor.h::CombineAggregateValues` | ☐ | 五种聚合类型的合并逻辑 |
| `nested_loop_join_executor.cpp` | ☐ | 嵌套循环，注意 LEFT JOIN 语义 + 右子节点每次 Init |
| `nested_index_join_executor.cpp` | ☐ | 外表每行去内表**索引**探测 |
| `hash_join_executor.cpp` | ☐ | build 侧建表，probe 侧探测 |

**坑**：`SELECT COUNT(*) FROM empty_table` 必须返回 `0`（一行），
但 `... GROUP BY x` 要返回空。`p3.06-empty-table.slt` 专测这个。

### Task #3 — 排序、Limit、TopN（4 个 + 公共代码）

| 文件 | 状态 | 要点 |
|---|---|---|
| `execution_common.cpp::TupleComparator` | ☐ | 按 `order_bys_` 逐列比，**严格弱序**（全相等返回 false，否则 `std::sort` UB） |
| `execution_common.cpp::GenerateSortKey` | ☐ | 抽出排序列 |
| `limit_executor.cpp` | ☐ | 最简单，**建议第一个写来热身** |
| `sort_executor.cpp` | ☐ | `Init()` 全排序 |
| `topn_executor.cpp` | ☐ | 容量 N 的堆，别先全排序再截断；**求 top-N 最小值用大顶堆** |
| `topn_per_group_executor.cpp` | ☐ | 每分组一个堆 |

`execution_common.h` 明确标了 P3 只需要前两个：

```45:48:src/include/execution/execution_common.h
/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */
```

### Task #4 — 外排序与窗口函数（难度最高）

| 文件 | 状态 | 要点 |
|---|---|---|
| `storage/page/intermediate_result_page.h` | ☐ | **页面布局要自己设计**，支持变长 tuple |
| `external_merge_sort_executor.h::MergeSortRun::Iterator` | ☐ | 6 处 `UNIMPLEMENTED`：`++` `*` `==` `!=` `Begin` `End` |
| `external_merge_sort_executor.cpp` | ☐ | 只需 **2-way** |
| `window_function_executor.cpp` | ☐ | `RANK() OVER (PARTITION BY ...)`，形态特殊，**放最后** |

```106:108:src/include/execution/executors/external_merge_sort_executor.h
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
```

页面布局完全留白：

```19:26:src/include/storage/page/intermediate_result_page.h
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
```

> 💡 **可复用 P2 经验**：
> - 页面布局设计 → 参考 `TablePage` 的 slotted page（数据从页尾往前长、slot 数组从页头往后长），
>   以及我在 `b_plus_tree_leaf_page` 里做的 tombstone 定长缓冲区布局。
> - `MergeSortRun::Iterator` 要**跨页推进** → 和 P2 的 `IndexIterator::AdvanceToNextVisible()`
>   是同一类问题，可直接借鉴那个「用 while 循环统一处理跨页」的写法。

### Task #5 — 优化器规则（4 条）

| 文件 | 状态 | 说明 |
|---|---|---|
| `optimizer/seqscan_as_indexscan.cpp` | ☐ | 等值谓词 + 存在索引 → IndexScan |
| `optimizer/sort_limit_as_topn.cpp` | ☐ | Sort + Limit → TopN |
| `optimizer/nlj_as_hash_join.cpp` | ☐ | 从谓词里认出等值 join key |
| `optimizer/column_pruning.cpp` | ☐ | 裁掉用不到的列，**还没接进规则链** |
| `optimizer/optimizer_custom_rules.cpp` | ☐ | leaderboard 用，可选 |

当前规则链顺序（`optimizer_custom_rules.cpp:22`）：

```cpp
p = OptimizeMergeProjection(p);
p = OptimizeMergeFilterNLJ(p);
p = OptimizeNLJAsHashJoin(p);      // ← 待写
p = OptimizeOrderByAsIndexScan(p);
p = OptimizeSortLimitAsTopN(p);    // ← 待写
p = OptimizeMergeFilterScan(p);
p = OptimizeSeqScanAsIndexScan(p); // ← 待写
```

**顺序有讲究**（也解释了框架为何这样排）：
- 必须先 `MergeFilterNLJ` 把Filter 塞进 NLJ 谓词，`NLJAsHashJoin` 才能认出等值 join key
- 必须先 `MergeFilterScan`，`SeqScanAsIndexScan` 才看得到 scan 上的过滤条件

规则都是**树 → 树的纯函数**，套路是「先递归优化 children，再匹配当前节点模式」，
照抄已有的 starter rule（`merge_filter_scan.cpp` 等）。

---

## 建议推进顺序

```
第 0 步  逐行读 filter_executor.cpp，吃透批量接口的 3 个模式   ← 别跳过
        ↓
第 1 步  limit_executor（最简单，验证理解了批量协议）
        ↓
第 2 步  seq_scan → insert → delete → update → index_scan
        ★里程碑：bustub-shell 能跑 SQL 了，p3.01~p3.06 过
        ↓
第 3 步  execution_common（TupleComparator + GenerateSortKey）
        → sort → topn → topn_per_group          ★ p3.16 p3.17
        ↓
第 4 步  aggregation（含 CombineAggregateValues）  ★ p3.07~p3.09
        ↓
第 5 步  nested_loop_join → nested_index_join → hash_join  ★ p3.10~p3.15
        ↓
第 6 步  优化器 4 条规则
        ↓
第 7 步  IntermediateResultPage + external_merge_sort（最难，独立）
        ↓
第 8 步  window_function  ★ p3.20，然后 p3.18 p3.19 集成测试
```

**关键判断：先做「广度」再做「深度」。**
先让最简单的 SQL 端到端跑通（第 2 步），再逐个补算子。
因为 **P3 没有单元测试，必须先让 shell 能跑，才有调试手段。**

---

## 构建与测试

### P3 没有 gtest 单测

`test/execution/` 是**空目录**。全靠 SQL 回归测试，共 **26 个** `.slt` 文件：

```
p3.00-primer            p3.07-simple-agg        p3.14-hash-join
p3.01-seqscan           p3.08-group-agg-1       p3.15-multi-way-hash-join
p3.02-insert            p3.09-group-agg-2       p3.16-sort-limit
p3.03-update            p3.10-simple-join       p3.17-topn
p3.04-delete            p3.11-multi-way-join    p3.18-integration-1
p3.05-index-scan-btree  p3.12-repeat-execute    p3.19-integration-2
p3.06-empty-table       p3.13-nested-index-join p3.20-window-function

p3.leaderboard-q1 / q1-index / q1-window / q2 / q3
```

### 命令

```bash
cd build
# ★ 必须用 clang：系统 gcc 没有 libasan，Debug 默认开 -fsanitize=address 会链接失败
# CC=clang CXX=clang++ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..

ninja -j $(nproc) sqllogictest bustub-shell

# 跑单个测试文件
./bin/bustub-sqllogictest ../test/sql/p3.01-seqscan.slt --verbose

# 交互式调试（强烈推荐）
./bin/bustub-shell
```

### `EXPLAIN` 是 P3 最重要的调试工具

```sql
EXPLAIN SELECT * FROM t1 WHERE x > 5;   -- 看优化后的计划树
EXPLAIN (o) SELECT ...;                  -- 只看优化器输出
```

**结果不对时，先确认计划树是不是你想的那样**，再去查 executor 逻辑。
很多「结果错」其实是优化器规则匹配错了。

### 从 P1 继承的性能隐患

为了修换页竞态（见 `NOTES-P1-P2.md` 1.5），eviction IO 被**串行化**了。
P3 的大表测试（`_1m` 后缀）会比参考实现慢。功能不受影响，
但要打 leaderboard 得先补那个 TODO（给 frame 加 `io_in_progress_` 标记 + 条件变量）。

---

# 第三部分：实现记录

## 实现记录

> 每完成一个 executor 在这里记：**设计选择 / 为什么这样写 / 与 P1P2 的接缝 / 对应测试**。
> 参照 `NOTES-P1-P2.md` 的风格 —— **写错过的结论也保留并标注**，比只留正确答案有价值。

### 进度

| 测试 | 状态 | 依赖的 executor |
|---|---|---|
| `p3.00-primer` | ✅ | 全靠框架给定（MockScan + Projection），**开工前就是通的** |
| `p3.01-seqscan` | ✅ | SeqScan |
| `p3.02-insert` | ✅ | Insert + SeqScan 谓词下推 |
| `p3.03`~ `p3.19` | ❌ | 见任务清单 |

---

### 1. SeqScanExecutor ✅

**类型**：流式（在 `Next()` 里干活）
**对应测试**：`p3.01-seqscan.slt`（4 条查询：裸扫描×2、列换序、表达式）
**只改了一个文件**：`src/execution/seq_scan_executor.cpp` + 头文件加两个成员

#### 为什么它必须是第一个写的

不是因为最简单，而是**它是唯一的解锁点**：

- `p3.01`~`p3.06` 全是真实表（`create table`，无 mock）→全部依赖 SeqScan
- `p3.02`~`p3.04` 的 insert/update/delete，子节点也是 SeqScan
- **在它跑通之前，`bustub-shell` 和 `EXPLAIN` 都用不了 → 只能盲写**

> ⚠️我最初的计划是「先写 `limit_executor` 热身」，**这是错的**：
> `SELECT * FROM t1 LIMIT 3` 的子节点是 SeqScan，Limit 写完根本没法验证。
> `p3.16-sort-limit` 虽然用 mock 表，但还需要 Sort。**必须先 SeqScan。**

#### 三个函数的分工：由「调用次数」决定

```
ExecutionEngine::Execute()
   ├─ ① ExecutorFactory::CreateExecutor()  → 构造函数   【1 次】
   ├─ ② executor->Init()                                【多次】
   └─ ③ while (executor->Next(...))                      【很多次】
```

**判断标准：问自己「这个东西需要在 `Init()` 时归零吗？」**

| 成员 | 放哪 | 理由 |
|---|---|---|
| `plan_` | 构造函数 | 永不变 |
| `table_info_`（`shared_ptr<TableInfo>`） |构造函数 | 表元信息整个查询期间不变，`GetTable()` 是哈希查找，放`Next()` 会重复查几千次 |
| `iter_`（`optional<TableIterator>`） | **`Init()`** | **扫描进度，每次重扫都要归零** |

#### 🔑 迭代器为什么绝不能放构造函数

`Init()` 的语义是**「回到开头重新扫」**，而构造函数只调一次。

NLJ 的左右子树各只构造一次（`executor_factory.cpp:60-61`），
但外表每来一行，内表都要从头扫一遍 —— 靠的就是反复调 `right->Init()`。
框架还专门为此加了断言（`execution_engine.h:82-85` `nlj check failed`）。

写在构造函数里 → 第二次 `Init()` 不重置进度 → 内表第二轮返回空 → NLJ 只匹配到第一行。
`p3.12-repeat-execute.slt` 专测这个。

#### `TableIterator` 的约束 → 必须用 `std::optional`

```35:42:src/include/storage/table/table_iterator.h
  DISALLOW_COPY(TableIterator);

  TableIterator(TableHeap *table_heap, RID rid, RID stop_at_rid);


  TableIterator(TableIterator &&) = default;
```

**不可拷贝 + 无默认构造** → `TableIterator iter_;` 编译不过。

```cpp
std::optional<TableIterator> iter_;
// Init(): iter_.emplace(table_info_->table_->MakeIterator());   // emplace 原地构造，不需要拷贝
```

> 💡 **和 P2 完全同一个套路**：move-only 类型 + 延迟初始化 = `std::optional`。
> 对照 P2 `CrabDownToLeaf` 里的 `std::optional<ReadPageGuard> protector`
> 和框架 `Context` 里的 `std::optional<WritePageGuard> header_page_`。

#### `Next()` 的三阶段骨架

**这是所有流式算子的通用骨架**，只有阶段 2 的内容不同：

```cpp
auto SeqScanExecutor::Next(...) -> bool {
  //阶段 1：重置输出（调用方复用同一对 vector）
  tuple_batch->clear();
  rid_batch->clear();

  // 阶段 2：从 iter_ 当前位置继续扫，攒够一批就走
  while (!iter_->IsEnd()) {
    auto [meta, tuple] = iter_->GetTuple();   // TupleMeta 和 Tuple 分开存
    auto rid = iter_->GetRID();
    ++(*iter_);                               // ★ 必须在 continue 之前
    if (meta.is_deleted_) { continue; }       // ★ DELETE 只置标记
    tuple_batch->push_back(tuple);
    rid_batch->push_back(rid);                // ★ 父节点 delete/update 要用
    if (tuple_batch->size() >= batch_size) { return true; }
  }

  // 阶段 3：表扫完了，把手上剩的吐出去
  return !tuple_batch->empty();
}
```

#### 四个「为什么」

**① 为什么开头必须 `clear()`**

调用方的两个 vector 在 `while` **外面**声明，每轮传同一个对象：

```97:102:src/include/execution/execution_engine.h
    std::vector<RID> rids{};
    std::vector<Tuple> tuples{};
    while (executor->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
      if (result_set != nullptr) {
        result_set->insert(result_set->end(), tuples.begin(), tuples.end());
      }
    }
```

不清空 → 第2 批 = 第 1 批 + 新数据 → `result_set` **平方级膨胀**。

**② 为什么 SeqScan 不需要 `child_offset_`**

| | 数据来源 | 断点存在哪 |
|---|---|---|
| `filter_executor` | 子节点**批量**给 1000 个数组 | 必须记数组下标 `child_offset_` |
| `SeqScanExecutor` | 迭代器**一个一个**给 | **`iter_` 自己就是断点** |

`filter_executor` 一次拿 1000 个但输出批只装300 个，剩下 700 个在**中间数组**里，得记下标。
SeqScan 没有这个中间数组。

**③ 为什么 `++` 必须在 `continue` 之前** 🔥

```cpp
if (meta.is_deleted_) { continue; }   // ❌ 没推进就跳回 while
++(*iter_);                            //    永远执行不到
```

遇到已删除行 → `continue` → `while` 重判 → `iter_` 没动 → 取到同一行 → **无限循环，进程卡死**。
`p3.04-delete.slt` 一定造出已删除的行，必踩。

**④ 为什么结尾是 `return !tuple_batch->empty();`** 🔥

等价于 `filter_executor` 的两个 return 合并（`filter_executor.cpp:69-77`）。
语义：**「表扫完了」≠「没数据要给你」**。

在 `p3.01` 上这个错误最致命：

```
test_simple_seq_1 只有 10 行，BUSTUB_BATCH_SIZE 远大于 10
第 1 次 Next()：while 把 10 行全扫完 → 走出循环
                此时 tuple_batch 里正好装着【全部】10 行
  写 return false     → 调用方 while 判假直接退出 → 结果集是空的！
  写 return !empty()  → true → 10 行进结果集 ✅
```

**整个结果集都在这最后一批里。** 后面数据量大的算子里，同样的bug 只在
「总行数不是 `batch_size` 整数倍」时才现形，那时难查得多。

#### 与 P1/P2 的接缝

```
SeqScanExecutor
  └─ exec_ctx_->GetCatalog()->GetTable(oid)   → TableInfo
       └─ table_info_->table_                → TableHeap
            └─ MakeIterator() / GetTuple(rid)  → BufferPoolManager::ReadPage  ← 我的 P1
```

#### 谓词下推：`filter_predicate_` 的处理 ✅

**这不是「要不要做的性能优化」，而是强制的正确性依赖。** 原因在优化器这一步：

```28:47:src/optimizer/merge_filter_scan.cpp
auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  ...
  if (optimized_plan->GetType() == PlanType::Filter) {
    ...
    if (child_plan.GetType() == PlanType::SeqScan) {
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }
  return optimized_plan;
}
```

这条规则是**框架给定、无条件执行**的：不管 `SeqScanExecutor` 有没有实现谓词求值，
它都会把 `Filter(SeqScan)` 两层节点合并成一层 `SeqScan{filter_predicate_=谓词}`，
**独立的 `Filter` 节点从计划树里彻底消失**。

**后果**：合并已经在优化阶段发生，如果 `SeqScanExecutor::Next()` 不读 `filter_predicate_`
去求值，就没有任何算子会执行这个 `WHERE` 了——不是变慢，是条件**直接消失**，
所有行（不管满不满足条件）都会原样吐出去。不崩、不报错，典型的「结果错但不崩」。

**实现**（SeqScan 没有子节点，求值要用自己的 `GetOutputSchema()`）：

```cpp
auto predicate = plan_->filter_predicate_;
while (!iter_->IsEnd()) {
  auto [meta, tuple] = iter_->GetTuple();
  ...
  if (predicate != nullptr) {
    auto value = predicate->Evaluate(&tuple, GetOutputSchema());
    if (value.IsNull() || !value.GetAs<bool>()) { continue; }  // 不满足，跳过这行
  }
  tuple_batch->push_back(tuple);
  ...
}
```

**如何暴露的**：`p3.01` 四条查询都没 `WHERE`（此时 `filter_predicate_ = nullptr`），测不出来；
`p3.02-insert.slt` 里 `insert into t2 select * from t1 where v1 <= 2`
（期望只插 6 行）在漏处理时把全表 10 行都插进去了，因为 `Filter` 节点已经被合并消失，
`WHERE` 相当于形同虚设。


---

## 踩坑记录

> 只记**真正踩过并验证过**的坑，附症状与定位方法。

### 坑 1：漏处理 `filter_predicate_`，`WHERE` 条件被无声吞掉

**现象**：`insert into t2 select * from t1 where v1 <= 2` 期望插入 6 行，实际把全表 10 行都插了进去；
`where v1 != v1`（期望 0 行）实际插入了 10 行。

**根因**：`OptimizeMergeFilterScan`（框架给定）无条件把`Filter(SeqScan)` 合并成
`SeqScan{filter_predicate_=谓词}`，独立的 `Filter` 节点从计划树消失；
`SeqScanExecutor::Next()` 没有读`filter_predicate_` 求值，导致这个谓词没人执行。

**为什么难查**：不崩、不报错；`p3.01` 全是无 `WHERE` 的查询，测不出来；
只有真正带 `WHERE` 的语句才暴露，且现象是「结果多了几行」而非报错。

**修复**：`Next()` 里对每一行 `predicate->Evaluate(&tuple, GetOutputSchema())`，
不满足（`IsNull()` 或 `false`）就 `continue` 跳过（详见上面「谓词下推」小节）。

**可复用的定位手法**：`EXPLAIN` 看优化后的计划树，确认原来的 `Filter` 节点是否已被合并进
`SeqScan`/其他节点；凡是「结果多了/少了几行但没报错」，先怀疑某个过滤条件是不是被优化器
挪了位置之后没人真正执行。

<!-- 模板：
### 坑 N：<一句话症状>
**现象**：
**根因**：
**为什么难查**：
**修复**：
**可复用的定位手法**：
-->

### 预判会踩的坑（来自概念分析，待验证）

| # | 预判 | 症状 |
|---|---|---|
| 1 | `!status && !empty()` 写成 `return false` | 结果少几行，**只在数据量非 `batch_size` 整数倍时出现** |
| 2 | 求值用 `plan_->OutputSchema()` 而非 `child->GetOutputSchema()` | 静默读到错误的列，不崩不报错 |
| 3 | 忘记检查 `meta.is_deleted_` | 删掉的行还能查出来 |
| 4 | 只插堆没插索引 | `SELECT *` 查得到，`WHERE c=42` 查不到 |
| 5 | UPDATE 时索引没「先删后插」 | RID 变了但索引还指向旧地址 → 幽灵行 |
| 6 | 空表 + 无 GROUP BY 的聚合返回空 | `SELECT COUNT(*) FROM empty` 应返回 0而非空集|
| 7 | NLJ 右子节点没有每次 `Init()` | 框架断言 `nlj check failed` |
| 8 | `TupleComparator` 不满足严格弱序 | `std::sort` UB，可能崩也可能乱序 |
| 9 | TopN 堆的方向搞反 | 结果是最大的 N 个而非最小的 N 个 |

---

## 遗留问题

> 已知但暂不处理的，记在这里避免重复发现。

### （待填）
