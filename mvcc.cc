#include "mvcc.h"

#include <iostream>
#include <utility>

namespace mvcc {

Connection Database::CreateConn() {
  auto txn = std::make_shared<Transaction>();
  txn->txn_id = next_txn_id++;
  txn->isolation_level = isolation_level_;
  txn->state = TransactionState::kInProgress;

  // Get all in-process transactions.
  for (const auto& [cur_txn_id, cur_txn] : db_txns) {
    if (cur_txn->state == TransactionState::kInProgress) {
      txn->inprogress_txns.insert(cur_txn_id);
    }
  }

  // Add current transaction into database.
  db_txns.emplace(txn->txn_id, txn);

  Connection conn;
  conn.db = this;
  conn.txn = std::move(txn);

  return conn;
}

bool Database::HasWriteConflict(Transaction* txn1, Transaction* txn2) {
  const auto& write_set1 = txn1->write_set;
  const auto& write_set2 = txn2->write_set;

  for (const auto& cur_key : write_set1) {
    if (write_set2.find(cur_key) != write_set2.end()) {
      return true;
    }
  }

  return false;
}

bool Database::HasReadWriteConflict(Transaction* txn1, Transaction* txn2) {
  // Check read set for [txn1] and write set for [txn2].
  const auto& write_set1 = txn1->write_set;
  const auto& read_set2 = txn2->read_set;

  for (const auto& cur_key : write_set1) {
    if (read_set2.find(cur_key) != read_set2.end()) {
      return true;
    }
  }

  // Check read set for [txn2] and write set for [txn1].
  const auto& read_set1 = txn1->read_set;
  const auto& write_set2 = txn2->write_set;

  for (const auto& cur_key : read_set1) {
    if (write_set2.find(cur_key) != write_set2.end()) {
      return true;
    }
  }

  return false;
}

// Two visible cases:
// 1. The value starts before current transaction, and already committed.
// 2. The value doesn't ends, or ends from an uncommitted transaction.
bool Database::IsVisible(const ValueWrapper& value_wrapper, Transaction* txn) {
  // Case-1: if the value is deleted or overwritten by current transaction.
  if (value_wrapper.end_txn_id == txn->txn_id) {
    return false;
  }

  // Case-2: current transaction writes the value.
  if (value_wrapper.start_txn_id == txn->txn_id) {
    return true;
  }

  // Case-3: value starts before current transaction, and has been committed or
  // didn't end (aka, not overwritten).
  if (value_wrapper.start_txn_id < txn->txn_id
      && db_txns.at(value_wrapper.start_txn_id)->state
      == TransactionState::kCommitted) {
    // Case-3-1: value doesn't get overwritten.
    if (value_wrapper.end_txn_id == kInvalidTxnId) {
      return true;
    }

    // Case-3-2: value gets overwritten by an uncommited transaction.
    if (value_wrapper.end_txn_id != kInvalidTxnId
        && db_txns.at(value_wrapper.end_txn_id)->state
        != TransactionState::kCommitted) {
      return true;
    }
  }

  return false;
}

std::optional<ValueType> Connection::Get(const KeyType& key) {
  auto key_iter = db->storage.find(key);
  if (key_iter == db->storage.end()) {
    return std::nullopt;
  }

  txn->read_set.insert(key);

  const auto& value_wrappers = key_iter->second;
  const auto value_num = value_wrappers.size();
  for (int idx = value_num - 1; idx >= 0; --idx) {
    if (db->IsVisible(value_wrappers[idx], txn.get())) {
      return value_wrappers[idx].value;
    }
  }

  return std::nullopt;
}

void Connection::Set(KeyType key, ValueType value) {
  auto& value_wrappers = db->storage[key];
  const auto value_num = value_wrappers.size();
  for (int idx = value_num - 1; idx >= 0; --idx) {
    // Mark all visible values as finish.
    if (db->IsVisible(value_wrappers[idx], txn.get())) {
      value_wrappers[idx].end_txn_id = txn->txn_id;
    }
  }

  txn->write_set.insert(key);

  ValueWrapper cur_value_wrapper;
  cur_value_wrapper.value = std::move(value);
  cur_value_wrapper.start_txn_id = txn->txn_id;
  cur_value_wrapper.end_txn_id = kInvalidTxnId;
  value_wrappers.emplace_back(std::move(cur_value_wrapper));
}

bool Connection::Delete(const KeyType& key) {
  auto key_iter = db->storage.find(key);
  if (key_iter == db->storage.end()) {
    return false;
  }

  auto& value_wrappers = key_iter->second;
  const auto value_num = value_wrappers.size();
  for (int idx = value_num - 1; idx >= 0; --idx) {
    // Mark all visible values as finish.
    if (db->IsVisible(value_wrappers[idx], txn.get())) {
      value_wrappers[idx].end_txn_id = txn->txn_id;
    }
  }

  txn->write_set.insert(key);
  return true;
}

void Connection::Abort() {
  txn->state = TransactionState::kAborted;
}

bool Connection::Commit() {
  // At commit, check whether current transaction has conflict with in-progress
  // ones at start.
  const auto& inprogress_txns = txn->inprogress_txns;
  for (const TxnId cur_txn_id : inprogress_txns) {
    const auto& another_txn = db->db_txns.at(cur_txn_id);
    // Check conflict for snapshot isolation.
    if (txn->isolation_level == IsolationLevel::kSnapshotIsolation) {
      if (another_txn->state != TransactionState::kCommitted) {
        continue;
      }
      if (db->HasWriteConflict(txn.get(), another_txn.get())) {
        Abort();
        return false;
      }
      continue;
    }

    // Check conflict for serializable isolation.
    if (txn->isolation_level == IsolationLevel::kSerializableIsolation) {
      if (db->HasWriteConflict(txn.get(), another_txn.get())) {
        Abort();
        return false;
      }
      if (db->HasReadWriteConflict(txn.get(), another_txn.get())) {
        return false;
      }
      continue;
    }
  }

  txn->state = TransactionState::kCommitted;
  return true;
}

Connection::~Connection() {
  if (txn->state == TransactionState::kInProgress) {
    Abort();
  }
}

}  // namespace mvcc
