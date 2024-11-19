// TODO(hjiang): Use `googletest` instead of `assert`.

#include "mvcc.h"

#include <cassert>
#include <iostream>

#define EXPECT_EQ(lhs, rhs) assert((lhs) == (rhs))
#define EXPECT_TRUE(cond) assert((cond))
#define EXPECT_FALSE(cond) assert(!(cond))

namespace mvcc {

namespace {

void AssertHasKeyValue(Database* db, const KeyType& key,
                       const ValueType& expected_value) {
  auto conn = db->CreateConn();
  auto actual_value = conn.Get(key);
  EXPECT_TRUE(actual_value.has_value());
  EXPECT_EQ(*actual_value, expected_value);
}

}  // namespace

// Testing senario: testing key-value pair get and set operation in one
// transaction.
void TestGetAndSetInOneTxn() {
  Database db{};
  db.SetIsolationLevel(IsolationLevel::kSnapshotIsolation);

  // First transaction.
  {
    auto conn = db.CreateConn();

    // Get a non-existent key.
    auto value = conn.Get("key");
    EXPECT_FALSE(value.has_value());

    // Set key-value pair and get later.
    conn.Set("key", "val");
    value = conn.Get("key");
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(*value, "val");

    // Delete key and get again.
    EXPECT_TRUE(conn.Delete("key"));
    value = conn.Get("key");
    EXPECT_FALSE(value.has_value());

    // Commit transaction and get again.
    conn.Set("key", "another-val");
    EXPECT_TRUE(conn.Commit());
  }

  // Second transaction.
  {
    auto conn = db.CreateConn();
    auto value = conn.Get("key");
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(*value, "another-val");
  }
}

// Testing senario: testing multiple interleaving transactions.
void TestMultipleTransactions_SnapshotIsolation() {
  Database db{};
  db.SetIsolationLevel(IsolationLevel::kSnapshotIsolation);

  {
    auto conn = db.CreateConn();
    conn.Set("key", "val");
    EXPECT_TRUE(conn.Commit());
  }
  AssertHasKeyValue(&db, "key", "val");

  auto conn1 = db.CreateConn();
  auto conn2 = db.CreateConn();

  conn1.Set("key", "conn-1");
  // Check conn2.
  auto value = conn2.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "val");
  // Check conn1.
  value = conn1.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "conn-1");

  // Commit conn1 and check.
  conn1.Commit();
  value = conn2.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "conn-1");

  // Delete conn2 and check.
  EXPECT_TRUE(conn2.Delete("key"));
  value = conn2.Get("key");
  EXPECT_FALSE(value.has_value());

  auto conn3 = db.CreateConn();
  value = conn3.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "conn-1");

  // Fails to commit conn2.
  EXPECT_FALSE(conn2.Commit());
}

// Testing senario: testing multiple interleaving transactions.
void TestMultipleTransactions_SerializableIsolation() {
  Database db{};
  db.SetIsolationLevel(IsolationLevel::kSerializableIsolation);

  {
    auto conn = db.CreateConn();
    conn.Set("key", "val");
    EXPECT_TRUE(conn.Commit());
  }
  AssertHasKeyValue(&db, "key", "val");

  // Check both read transactions.
  auto conn1 = db.CreateConn();
  auto conn2 = db.CreateConn();

  // Check conn1 and conn2.
  auto value = conn1.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "val");
  value = conn2.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "val");
  EXPECT_TRUE(conn1.Commit());
  EXPECT_TRUE(conn2.Commit());

  // Check conn1 and conn2 with read-write conflict.
  auto conn3 = db.CreateConn();
  auto conn4 = db.CreateConn();
  // Read operation for conn3.
  value = conn3.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "val");
  // Write operation for conn4.
  conn4.Set("key", "another-val");
  EXPECT_TRUE(conn3.Commit());
  EXPECT_FALSE(conn4.Commit());
}

}  // namespace mvcc

int main(int argc, char** argv) {
  mvcc::TestGetAndSetInOneTxn();
  mvcc::TestMultipleTransactions_SnapshotIsolation();
  mvcc::TestMultipleTransactions_SerializableIsolation();
  return 0;
}
