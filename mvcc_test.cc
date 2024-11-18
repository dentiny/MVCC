// TODO(hjiang): Use `googletest` instead of `assert`.

#include "mvcc.h"

#include <cassert>

#define EXPECT_EQ(lhs, rhs) assert((lhs) == (rhs))
#define EXPECT_TRUE(cond) assert((cond))
#define EXPECT_FALSE(cond) assert(!(cond))

namespace mvcc {

// Testing senario: testing key-value pair get and set operation in one
// transaction.
void TestGetAndSetInOneTxn() {
  Database db{};
  auto conn = db.CreateConn();

  // Get a non-existent key.
  auto value = conn.Get("key");
  EXPECT_FALSE(value.has_value());

  // Set key-value pair and get later.
  conn.Set("key", "val");
  value = conn.Get("key");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "val");
}

}  // namespace mvcc

int main(int argc, char** argv) {
  mvcc::TestGetAndSetInOneTxn();
  return 0;
}
