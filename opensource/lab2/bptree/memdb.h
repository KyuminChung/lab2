#ifndef LAB2_BPTREE_MEMDB_H
#define LAB2_BPTREE_MEMDB_H

#include <string>
#include <utility>
#include <vector>

#include "bptree.h"

struct MemDBOptions {
  int bptree_degree = 4;
};

class InMemoryDB {
 public:
  explicit InMemoryDB(const MemDBOptions& options);

  void Put(int key, const std::string& value);
  bool Get(int key, std::string* out_value) const;
  bool Delete(int key);
  std::vector<std::pair<int, std::string>> RangeScan(int start_key,
                                                     int end_key) const;

 private:
  BPlusTree tree_;
};

#endif  // LAB2_BPTREE_MEMDB_H
