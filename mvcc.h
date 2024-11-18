// In-memory implementation for MVCC, for educational purpose.
//
// Attention:
// 1. Assume single-threaded usage.
// 2. Assume key-value data model.
//
// TODO:
// 1. Add different isolation levels, currently only support snapshot isolation.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mvcc {

// A monotonically increasing id.
using TxnId = uint64_t;
// Use 0 to indicate invalid txn-id; starting from txn-id = 1.
constexpr TxnId kInvalidTxnId = 0;

using KeyType = std::string;
using ValueType = std::string;

enum class TransactionState {
  kInvalid,
  kInProgress,
  kCommitted,
  kAborted,
};

struct Transaction {
  TxnId txn_id = kInvalidTxnId;

  // Ongoing transactions whether the current txn starts.
  std::unordered_set<TxnId> inprogress_txns;

  // State for the current transaction.
  TransactionState state = TransactionState::kInvalid;

  // Keys for write.
  std::unordered_set<KeyType> write_set;

  // Keys for read.
  std::unordered_set<KeyType> read_set; 
};

// Definition for multi-version values.
struct ValueWrapper {
  ValueType value;
  TxnId start_txn_id;               // Inclusive.
  TxnId end_txn_id = kInvalidTxnId; // Inclusive.
};

// Forward declaration.
class Database;

// A connection has access to [Database], which represents a single [Transaction].
struct Connection {
 public:
  // Get the value for [key], return `std::nullopt` if doesn't exist.
  std::optional<ValueType> Get(const KeyType& key);

  // Set the given [key] and [value] pair to the database.
  void Set(KeyType key, ValueType value);

 private:
  friend class Database;

  Database* db = nullptr;
  std::shared_ptr<Transaction> txn;
};

// Definition for database.
struct Database {
 public:
  // Create a connection, which represents a transaction.
  Connection CreateConn();

 private:
  friend class Connection;

  // Returns whether the given [value_wrapper] is visible for [txn].
  bool IsVisible(const ValueWrapper& value_wrapper, Transaction* txn);

  // Ongoing transactions.
  // TODO(hjiang): Could prune committed transactions.
  std::map<TxnId, std::shared_ptr<Transaction>> db_txns;
  // Multi-version in-memory storage.
  std::unordered_map<KeyType, std::vector<ValueWrapper>> storage;
  // Next transaction id.
  TxnId next_txn_id = kInvalidTxnId + 1;
};

}  // namespace mvcc
