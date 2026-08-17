//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // read_ts_ 固定为当前系统已提交的最高时间戳：即"我只能看到在我 Begin 之前
  // 就已经提交的数据"，这正是快照隔离（Snapshot Isolation）的定义。
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);  // 这是全局提交锁

  // 先"预占"一个 commit ts（此时还没有正式写回 last_commit_ts_，仅在本地计算）。
  // 之所以能在校验之前就计算好，是因为 commit_mutex_ 保证了同一时刻只有一个事务
  // 在走 Commit 流程，不会有竞争；即使后面校验失败走 Abort，这个数字也只是被浪费掉，
  // 时间戳只要求单调递增、不要求连续，所以没有正确性问题。
  timestamp_t commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // P4 Task#3: 遍历 write set，把这个事务写过的每一行的 base tuple 时间戳
  // 从"临时时间戳"改写为正式的 commit_ts_，使这些修改对"此后开始"的事务可见。
  // 数据内容不需要动（写的时候已经是新值了），只改时间戳。
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    if (table_info == nullptr) {
      continue;
    }
    for (const auto &rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // 正式落盘：全局时间戳前进，事务自己的 commit_ts_ 也被钉死。
  last_commit_ts_.store(commit_ts);
  txn->commit_ts_.store(commit_ts);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(P4): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  auto watermark = running_txns_.GetWatermark();

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::vector<txn_id_t> to_remove;
  for (auto &[txn_id, txn] : txn_map_) {
    auto state = txn->GetTransactionState();
    // 仍在运行 / 已被污染（TAINTED）的事务不能回收：它们的 read_ts_ 还占着 watermark，
    // 且它们的 undo log 仍可能被别的事务回放需要。
    if (state == TransactionState::RUNNING || state == TransactionState::TAINTED) {
      continue;
    }
    // COMMITTED / ABORTED：无 undo log 的事务直接回收（纯插入/原地修改，历史无需保留）；
    // 有 undo log 的事务，只有当 commit_ts_ < watermark（所有活跃事务都能直接看到它的修改）时才回收。
    if (txn->GetUndoLogNum() == 0 || txn->GetCommitTs() < watermark) {
      to_remove.push_back(txn_id);
    }
  }
  for (auto txn_id : to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
