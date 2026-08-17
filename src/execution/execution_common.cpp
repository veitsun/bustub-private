//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool { return false; }

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  return {};
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {

  // 这个函数的作用是把一行数据“倒带” 到过去某个时刻的样子
  // 历史修改都被“反向记录” 成了 undolog  ， 每条 undo log 不是记录改成了什么， 而是记录改之前是什么样
  // ReconstructTuple 就是拿着这些"改之前的样子"，从最新帧一路倒着回放，把每一列逐步恢复成更早的值，最终得到你要看的那个历史版本。



  // 1. 从 base tuple 提取所有列的值（这些值会随回放逐步被"倒带"成更早的版本）。
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // 2. base 版本的删除标记。
  bool deleted = base_meta.is_deleted_;

  // 3. 从新到旧依次应用每条 undo log：把被改的列"撤销"回旧值，并更新删除标记。
  for (const auto &log : undo_logs) {
    // 构造"只含被改列"的紧凑 schema，用于从 log.tuple_（紧凑存储）按序取值。
    std::vector<uint32_t> modified_cols;
    modified_cols.reserve(schema->GetColumnCount());
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        modified_cols.push_back(i);
      }
    }
    auto undo_schema = Schema::CopySchema(schema, modified_cols);   // 从原始 schema 里挑出“被修改的那几列”， 构造一个新的，只含这些列的紧凑 schema

    // 游标 j 指向 log.tuple_ 里的第 j 个值（紧凑排列，只含被改列）。
    uint32_t j = 0;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        values[i] = log.tuple_.GetValue(&undo_schema, j);
        j++;
      }
    }

    // 每条 log 精确描述"回放到这一步时 tuple 的删除状态"，必须直接赋值（而非 |=），
    // 这样"删除后又插入"（复活）的场景才能正确处理。
    deleted = log.is_deleted_;
  }

  // 4. 回放到最后若仍是删除态，说明该行对调用方"已删除/不存在"。
  if (deleted) {
    return std::nullopt;
  }
  return Tuple(values, schema);
}

// 判断当前这行是否已经被别人抢先该过，导致我这个事务不能安全地写它
auto IsWriteWriteConflict(timestamp_t base_ts, Transaction *txn) -> bool {
  // base_ts 是这行 base tuple 的当前时间戳 （读到的最新版本是谁写的， 什么时候写的）
  // 是我自己已经写过的行（base_ts == 我的临时时间戳）→ 不冲突，继续改。
  if (base_ts == txn->GetTransactionTempTs()) { // 这行是我自己的这个事务刚写的。 不冲突，继续写
    return false;
  }
  // 其他情况（别人的临时时间戳，或已提交但晚于我的 read_ts_）→ 都 > read_ts_，冲突。
  // read_ts_ = 事务 Begin() 那一刻的快照时间点，值 = 当时的 last_commit_ts_（系统已提交的最高时间戳）：
  return base_ts > txn->GetReadTs();  // 别人的临时时间戳全都归进冲突，别人（哪怕没提交）抢先动了这一行，我就应该判别冲突
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto IsVisible(timestamp_t ts, Transaction *txn) -> bool {
  // 情况1：这是我自己的临时时间戳 → 我刚写的，一定可见。
  // 临时时间戳和普通时间戳的区别就只有一点 ， 最高位是 0 就是 普通时间戳，最高位是 1 就是临时时间戳
  if (ts == txn->GetTransactionTempTs()) {
    return true;
  }
  // 情况2：是某个其他事务的临时时间戳（>= TXN_START_ID）→ 别人未提交的脏数据，不可见。
  if (ts >= TXN_START_ID) {
    return false;
  }
  // 情况3：正常已提交时间戳 → 只有 <= 我的 read_ts_（在我开始读之前提交）才可见。
  return ts <= txn->GetReadTs();
}


// 给定一行数据，找出“当前事务 T 能看到那个历史版本” 需要回放哪些 undo log
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  // 情况1：base tuple 本身对当前事务可见 → 无需任何 undo log，直接返回空 vector
  //（配合 base_tuple 直接使用，无需回放历史）。
  if (IsVisible(base_meta.ts_, txn)) {
    return std::vector<UndoLog>{};
  }

  // 情况2/3：base tuple 不可见，沿版本链从新到旧收集 undo log，
  // 直到找到第一条可见的日志（连同它一起收集，用于回放时"抹平"更新版本带来的差异）。
  std::vector<UndoLog> logs;
  auto link = undo_link;  // undo_link 是这行版本链的入口
  while (link.has_value() && link->IsValid()) {
    auto log = txn_mgr->GetUndoLog(*link);  // 取这一节日志
    logs.push_back(log);      // 收集，先收进来再说
    if (IsVisible(log.ts_, txn)) {
      return logs;    // 找到可见版本就停
    }
    link = log.prev_version_;  // 没找到继续往前
  }

  // 走到链表尽头都没找到可见版本 → 该行对当前事务"根本不存在"。
  return std::nullopt;  // 把整条版本链从头走到尾， 没有任何一个版本的 ts_ <= read_ts_ 说明这行是在我开始读之后才被插入的，在我的快照世界里它还没出生，所以返回 nullopt
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // 这个函数的作用是生成一条新的 undo log， 记录这次修改之前，这行的旧样子-- 用于事务第一次修改某一行的场景
  UndoLog log;
  // is_deleted_ = "这次修改之前，这行是否是被删除状态"（base_tuple == nullptr）。
  // 回放时 undo log 的 is_deleted_ 会被直接赋值给 deleted 标记，表示"撤销这次修改后"的删除态。
  log.is_deleted_ = (base_tuple == nullptr);
  log.ts_ = ts;
  log.prev_version_ = prev_version;

  // 收集"这次修改中被改动的列"以及它们的旧值（来自 base_tuple）。
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  std::vector<uint32_t> modified_cols;
  std::vector<Value> old_values;

  if (base_tuple != nullptr) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      // target_tuple == nullptr 表示删除：整行的旧值都要记录。
      // 否则是更新：只记录"发生变化"的列。
      bool changed =
          (target_tuple == nullptr) ||
          !base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i));
      if (changed) {
        modified_fields[i] = true;
        modified_cols.push_back(i);
        old_values.push_back(base_tuple->GetValue(schema, i));
      }
    }
  }

  log.modified_fields_ = std::move(modified_fields);
  // 用紧凑 schema（只含被改列）构造旧值 tuple；若没有改列（如插入到已删除 slot），则得到空 tuple。
  auto undo_schema = Schema::CopySchema(schema, modified_cols);
  log.tuple_ = Tuple(old_values, &undo_schema);
  return log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // 同一个事务第二次修改同一行：把这次"新改的列"合并进已有的 undo log。
  // undo log 的语义始终是"撤销该事务的所有修改，回到第一次修改前的原始值"，
  // 所以只往 tuple 里"加东西"，永不删除已有列。
  UndoLog new_log = log;

  // 1. 更新 modified_fields_：找出这次"新改"的、且之前没被记录过的列。
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (new_log.modified_fields_[i]) {
      continue;
    }
    bool changed = (target_tuple == nullptr) ||
                   (base_tuple != nullptr &&
                    !base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i)));
    if (changed) {
      new_log.modified_fields_[i] = true;
    }
  }

  // 2. 按列下标升序重新构造紧凑 tuple（保证与 modified_fields_ 的顺序一一对应）。
  //    旧 log 里已记录的列取旧值；新改的列取 base_tuple 的值（第一次没改它，所以 base 里的值就是原始值）。
  std::vector<uint32_t> old_cols;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      old_cols.push_back(i);
    }
  }
  auto old_schema = Schema::CopySchema(schema, old_cols);   // 用来从旧 log 的紧凑 tuple_ 里取出旧值

  std::vector<Value> values;
  std::vector<uint32_t> all_cols;
  uint32_t old_j = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (!new_log.modified_fields_[i]) {
      continue;
    }
    all_cols.push_back(i);
    if (log.modified_fields_[i]) {
      values.push_back(log.tuple_.GetValue(&old_schema, old_j));
      old_j++;
    } else {
      values.push_back(base_tuple->GetValue(schema, i));
    }
  }

  auto all_schema = Schema::CopySchema(schema, all_cols);
  new_log.tuple_ = Tuple(values, &all_schema);
  return new_log;
}

namespace {
/** 把时间戳格式化成调试输出：临时时间戳（>= TXN_START_ID）打印成 txnN，普通时间戳打印成数字。 */
auto TsToString(timestamp_t ts) -> std::string {
  if (ts >= TXN_START_ID) {
    return fmt::format("txn{}", ts ^ TXN_START_ID);
  }
  return std::to_string(ts);
}

/** 把 undo log 的紧凑 tuple 展开成调试输出：被修改的列打印值，未修改的列打印 `_`。 */
auto UndoLogTupleToString(const Schema *schema, const UndoLog &log) -> std::string {
  // 收集被改列的下标，构造紧凑 schema。
  std::vector<uint32_t> modified_cols;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      modified_cols.push_back(i);
    }
  }
  auto undo_schema = Schema::CopySchema(schema, modified_cols);

  uint32_t j = 0;  // 游标：指向 log.tuple_ 里的第 j 个值（紧凑排列）
  std::string result = "(";
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (i != 0) {
      result += ", ";
    }
    if (log.modified_fields_[i]) {
      auto value = log.tuple_.GetValue(&undo_schema, j);
      j++;
      result += value.IsNull() ? "<NULL>" : value.ToString();
    } else {
      result += "_";
    }
  }
  result += ")";
  return result;
}
}  // namespace

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  // 把每行数据的“版本链” 肉眼可见地打印出来，快速验证 MVCC 逻辑对不对
  // 验证读/写是否正确： 改完 SeqScan， Update， Delete， 索引后，在关键节点调用 TxnMgrDbg， 肉眼比对版本链是否符合预期--比单步调试快一个数量级
  fmt::println(stderr, "debug_hook: {}", info);

  const auto *schema = &table_info->schema_;

  // 遍历 table heap 的每个 slot，打印 base tuple + 它的版本链。
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto [meta, tuple] = iter.GetTuple();  // 拿到这一行的 base tuple + 元信息
    auto rid = iter.GetRID();   // 拿到这一行的 rid

    // 第一行：base tuple 的 RID、时间戳、删除标记、内容。
    fmt::println(stderr, "RID={}/{} ts={}{} tuple={}", rid.GetPageId(), rid.GetSlotNum(), TsToString(meta.ts_),
                 meta.is_deleted_ ? " <del marker>" : "", tuple.ToString(schema)); // tostring 时间戳如果是临时时间戳，打印成 txnN， 普通时间戳打印数字

    // 后续行：沿 undo link 从新到旧打印版本链。
    // 沿着版本链打印历史
    auto link = txn_mgr->GetUndoLink(rid);   // 取这一行版本链的入口
    while (link.has_value() && link->IsValid()) {   // 还有历史就继续
      auto log_opt = txn_mgr->GetUndoLogOptional(*link);  
      if (!log_opt.has_value()) {
        break;  // 该日志所属事务已被 GC，链到此为止。
      }
      const auto &log = *log_opt;
      auto txn_human = link->prev_txn_ ^ TXN_START_ID;  // 把事务 id 转成 人类可读的 n
      if (log.is_deleted_) {
        fmt::println(stderr, "  txn{}@{} <del> ts={}", txn_human, link->prev_log_idx_, TsToString(log.ts_));
      } else {
        fmt::println(stderr, "  txn{}@{} {} ts={}", txn_human, link->prev_log_idx_,
                     UndoLogTupleToString(schema, log), TsToString(log.ts_));
      }
      link = log.prev_version_;
    }
  }
}

}  // namespace bustub
