#include "mvcc.h"

#include <utility>

namespace mvcc {

Connection Database::CreateConn() {
  auto txn = std::make_shared<Transaction>();
  txn->txn_id = next_txn_id++;
  txn->state = TransactionState::kInProgress;

  // Add current transaction into database.
  db_txns.emplace(txn->txn_id, txn);

  Connection conn;
  conn.db = this;
  conn.txn = std::move(txn);

  return conn;
}

bool Database::IsVisible(const ValueWrapper& value_wrapper, Transaction* txn) {
  // Case-1.
  if (value_wrapper.start_txn_id > txn->txn_id) {
    return false;
  }

  // Case-2: if start txn is still in-progress, invisible.
  if (txn->inprogress_txns.find(value_wrapper.start_txn_id)
      != txn->inprogress_txns.end()) {
    return false;
  }

  // Case-3.
  if (db_txns.at(value_wrapper.start_txn_id)->state !=
      TransactionState::kCommitted
      && value_wrapper.start_txn_id != txn->txn_id) {
    return false;
  }

  // Case-4: if current value is deleted in the current [txn].
  if (value_wrapper.end_txn_id == txn->txn_id) {
    return false;
  }

  // Case-5: the value is deleted in another committed transaction, which starts
  // before the given [txn].
  TxnId end_txn_id = value_wrapper.end_txn_id;
  if (end_txn_id != kInvalidTxnId
      && end_txn_id < txn->txn_id
      && db_txns.at(end_txn_id)->state != TransactionState::kCommitted
      && txn->inprogress_txns.find(end_txn_id)
      == txn->inprogress_txns.end()) {
    return false;
  }

  return true;
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

}  // namespace mvcc
