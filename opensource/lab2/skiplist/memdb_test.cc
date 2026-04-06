// 수정불가능!!

#include "memdb.h"

#include <algorithm>
#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {
std::vector<std::pair<int, std::string>> LoadWorkload(const std::string& path) {
  std::ifstream in(path);
  std::vector<std::pair<int, std::string>> out;
  int key = 0;
  std::string value;
  while (in >> key >> value) {
    out.emplace_back(key, value);
  }
  return out;
}

std::vector<int> SortedKeys(const std::map<int, std::string>& expected) {
  std::vector<int> keys;
  keys.reserve(expected.size());
  for (const auto& kv : expected) {
    keys.push_back(kv.first);
  }
  return keys;
}

struct WorkloadOp {
  char type;
  int key;
  std::string value;
};

std::vector<WorkloadOp> LoadWorkloadOps(const std::string& path) {
  std::ifstream in(path);
  std::vector<WorkloadOp> ops;
  std::string tag;
  while (in >> tag) {
    if (tag == "P") {
      int key = 0;
      std::string value;
      if (!(in >> key >> value)) {
        break;
      }
      ops.push_back(WorkloadOp{'P', key, value});
    } else if (tag == "D") {
      int key = 0;
      if (!(in >> key)) {
        break;
      }
      ops.push_back(WorkloadOp{'D', key, ""});
    } else {
      // Fallback: key value format.
      int key = std::stoi(tag);
      std::string value;
      if (!(in >> value)) {
        break;
      }
      ops.push_back(WorkloadOp{'P', key, value});
    }
  }
  return ops;
}
} // namespace

TEST(MemDBTest, PutGetBasic) {
  MemDBOptions options;
  options.max_memtable_bytes = 1024;
  options.skiplist_max_height = 8;
  options.skiplist_p = 0.5f;

  InMemoryDB db(options);
  db.Put(1, "one");
  db.Put(2, "two");

  std::string value;
  EXPECT_TRUE(db.Get(1, &value));
  EXPECT_EQ(value, "one");
  EXPECT_TRUE(db.Get(2, &value));
  EXPECT_EQ(value, "two");
  EXPECT_FALSE(db.Get(3, &value));
}

TEST(MemDBTest, DeleteMasksOlderValue) {
  MemDBOptions options;
  options.max_memtable_bytes = 1024;
  InMemoryDB db(options);

  db.Put(1, "v1");
  db.Put(2, "v2");
  db.Delete(1);

  std::string value;
  EXPECT_FALSE(db.Get(1, &value));
  EXPECT_TRUE(db.Get(2, &value));
  EXPECT_EQ(value, "v2");
}

TEST(MemDBTest, ImmutableSearchOrder) {
  MemDBOptions options;
  options.max_memtable_bytes = 12; // sizeof(int)=4, value size=4 -> entry 8
  InMemoryDB db(options);

  db.Put(1, "aaaa"); // mutable size 8
  db.Put(2, "bbbb"); // rotates, then inserts into new mutable

  EXPECT_EQ(db.ImmutableCount(), 1u);

  std::string value;
  EXPECT_TRUE(db.Get(1, &value));
  EXPECT_EQ(value, "aaaa");
  EXPECT_TRUE(db.Get(2, &value));
  EXPECT_EQ(value, "bbbb");
}

TEST(MemDBTest, TombstoneStopsOlderLookup) {
  MemDBOptions options;
  options.max_memtable_bytes = 12; // force rotation on second insert
  InMemoryDB db(options);

  db.Put(1, "old");  // goes into first memtable
  db.Put(2, "fill"); // rotates to new memtable
  db.Delete(1);      // tombstone in newest memtable

  std::string value;
  EXPECT_FALSE(db.Get(1, &value));
  EXPECT_TRUE(db.Get(2, &value));
  EXPECT_EQ(value, "fill");
}

TEST(MemDBTest, MultipleVersionsSameKey) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  db.Put(7, "v1");
  db.Put(7, "v2");
  db.Put(7, "v3");

  std::string value;
  ASSERT_TRUE(db.Get(7, &value));
  ASSERT_EQ(value, "v3") << "expected newest version for same key";
}

TEST(MemDBTest, TombstoneMasksOlderVersions) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  db.Put(7, "v1");
  db.Put(7, "v2");
  db.Put(8, "v8");
  db.Delete(7);

  std::string value;
  ASSERT_TRUE(db.Get(8, &value));
  ASSERT_EQ(value, "v8");
  ASSERT_FALSE(db.Get(7, &value)) << "tombstone should hide older versions";
}

TEST(MemDBWorkloadTest, FillRandom) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  auto workload = LoadWorkload("../workloads/fillrandom.txt");
  ASSERT_FALSE(workload.empty());

  std::map<int, std::string> expected;
  for (const auto& kv : workload) {
    db.Put(kv.first, kv.second);
    expected[kv.first] = kv.second;
  }

  for (int key : SortedKeys(expected)) {
    std::string value = "<not read>";
    bool found = db.Get(key, &value);
    ASSERT_TRUE(found) << "key=" << key << " expected=" << expected[key]
                       << " actual=<not found>";
    ASSERT_EQ(value, expected[key])
        << "key=" << key << " expected=" << expected[key]
        << " actual=" << value;
  }
}

TEST(MemDBWorkloadTest, FillSequential) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  auto workload = LoadWorkload("../workloads/fillseq.txt");
  ASSERT_FALSE(workload.empty());

  std::map<int, std::string> expected;
  for (const auto& kv : workload) {
    db.Put(kv.first, kv.second);
    expected[kv.first] = kv.second;
  }

  for (int key : SortedKeys(expected)) {
    std::string value = "<not read>";
    bool found = db.Get(key, &value);
    ASSERT_TRUE(found) << "key=" << key << " expected=" << expected[key]
                       << " actual=<not found>";
    ASSERT_EQ(value, expected[key])
        << "key=" << key << " expected=" << expected[key]
        << " actual=" << value;
  }
}

TEST(MemDBWorkloadTest, RangeScanAcrossMemtables) {
  MemDBOptions options;
  options.max_memtable_bytes = 12; // small to force rotation
  InMemoryDB db(options);

  db.Put(1, "v1");
  db.Put(2, "v2");
  db.Put(3, "v3");
  db.Put(4, "v4"); // rotates
  db.Delete(3);    // tombstone in newest memtable
  db.Put(5, "v5");

  auto result = db.RangeScan(1, 5);
  std::map<int, std::string> expected = {
      {1, "v1"}, {2, "v2"}, {4, "v4"}, {5, "v5"}};

  ASSERT_EQ(result.size(), expected.size());
  for (const auto& kv : result) {
    auto it = expected.find(kv.first);
    ASSERT_TRUE(it != expected.end()) << "unexpected key=" << kv.first;
    ASSERT_EQ(kv.second, it->second) << "key=" << kv.first;
  }
}

TEST(MemDBWorkloadTest, FillRandom2) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  auto workload = LoadWorkload("../workloads/fillrandom_2.txt");
  ASSERT_FALSE(workload.empty());

  std::map<int, std::string> expected;
  for (const auto& kv : workload) {
    db.Put(kv.first, kv.second);
    expected[kv.first] = kv.second;
  }

  for (const auto& kv : expected) {
    std::string value = "<not read>";
    bool found = db.Get(kv.first, &value);
    ASSERT_TRUE(found) << "key=" << kv.first << " expected=" << kv.second
                       << " actual=<not found>";
    ASSERT_EQ(value, kv.second)
        << "key=" << kv.first << " expected=" << kv.second
        << " actual=" << value;
  }
}

TEST(MemDBWorkloadTest, FillDeleteRandom) {
  MemDBOptions options;
  options.max_memtable_bytes = 1 << 20;
  InMemoryDB db(options);

  auto ops = LoadWorkloadOps("../workloads/filldelete_random.txt");
  ASSERT_FALSE(ops.empty());

  std::map<int, std::string> expected;
  for (const auto& op : ops) {
    if (op.type == 'P') {
      db.Put(op.key, op.value);
      expected[op.key] = op.value;
    } else if (op.type == 'D') {
      db.Delete(op.key);
      expected.erase(op.key);
    }
  }

  for (const auto& kv : expected) {
    std::string value = "<not read>";
    bool found = db.Get(kv.first, &value);
    ASSERT_TRUE(found) << "key=" << kv.first << " expected=" << kv.second
                       << " actual=<not found>";
    ASSERT_EQ(value, kv.second)
        << "key=" << kv.first << " expected=" << kv.second
        << " actual=" << value;
  }
}

int main() {
  ::testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}
