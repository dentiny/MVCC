// In-memory implementation for MVCC, for educational purpose.
//
// Attention:
// 1. Assume single-threaded usage.
// 2. Assume key-value data model.
//
// TODO:
// 1. Add different isolation levels, currently only support snapshot isolation.
// 2. As of now, we store all transaction in the database, whether it's
// committed, ongoing or aborted; should consider prune expired ones.

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

enum class IsolationLevel {
  kInvalid,
  kReadCommittedIsolation,
  kRepeatableReadIsolation,
  kSnapshotIsolation,
  kSerializableIsolation,
};

enum class TransactionState {
  kInvalid,
  kInProgress,
  kCommitted,
  kAborted,
};

struct Transaction {
  TxnId txn_id = kInvalidTxnId;

  // Isolation level.
  IsolationLevel isolation_level = IsolationLevel::kInvalid;

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
//
// There're two possible time points we would set end txn id:
// 1. A key-value pair is explicitly committed;
// 2. A new value overwrites with the current txn id.
struct ValueWrapper {
  ValueType value;
  TxnId start_txn_id;               // Inclusive.
  TxnId end_txn_id = kInvalidTxnId; // Inclusive.
};

// Forward declaration.
class Database;

// [Connection] represents a single [Transaction].
struct Connection {
 public:
  Connection() = default;

  // Abort transaction if not committed.
  ~Connection();

  // Get the value for [key], return `std::nullopt` if doesn't exist.
  std::optional<ValueType> Get(const KeyType& key);

  // Set the given [key] and [value] pair to the database.
  void Set(KeyType key, ValueType value);

  // Delete the given [key], return whether deletion succeeds or not.
  bool Delete(const KeyType& key);

  // Commit current transaction, whether commit succeeds or not.
  bool Commit();

  // Abort current transaction.
  void Abort();

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

  void SetIsolationLevel(IsolationLevel level) {
    isolation_level_ = level;
  }

 private:
  friend class Connection;

  // Visibility check for read committed isolation level.
  bool IsVisibleForReadCommitted(const ValueWrapper& value_wrapper,
    Transaction* txn);
  // Visibility check for repeatable read, snapshot isolation and serializable
  // isolation.
  bool IsVisibleForRepeatableRead(const ValueWrapper& value_wrapper,
    Transaction* txn);
  // Returns whether the given [value_wrapper] is visible for [txn].
  bool IsVisible(const ValueWrapper& value_wrapper, Transaction* txn);

  // Returns whether two transactions have write conflict.
  bool HasWriteConflict(Transaction* txn1, Transaction* txn2);

  // Returns whether two transactions have read-write conflict.
  bool HasReadWriteConflict(Transaction* txn1, Transaction* txn2);

  // Ongoing transactions.
  // TODO(hjiang): Could prune committed transactions.
  std::map<TxnId, std::shared_ptr<Transaction>> db_txns;
  // Multi-version in-memory storage.
  std::unordered_map<KeyType, std::vector<ValueWrapper>> storage;
  // Next transaction id.
  TxnId next_txn_id = kInvalidTxnId + 1;
  // Isolation level.
  IsolationLevel isolation_level_ = IsolationLevel::kSnapshotIsolation;
};

}  // namespace mvcc
