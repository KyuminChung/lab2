// 수정 불가능!!
#include "memdb.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>

struct BenchOptions {
  std::string benchmarks;
  int64_t num = 100000;
  int key_range = 100000;
  int value_size = 100;
  int range_size = 100;
  int bptree_degree = 4;
  uint64_t seed = 0x3669F210;
};

static bool ParseFlag(const std::string& arg, std::string* name,
                      std::string* value) {
  if (arg.rfind("--", 0) != 0) {
    return false;
  }
  size_t eq = arg.find('=');
  if (eq == std::string::npos) {
    *name = arg.substr(2);
    *value = "";
  } else {
    *name = arg.substr(2, eq - 2);
    *value = arg.substr(eq + 1);
  }
  return true;
}

static void ParseArgs(int argc, char** argv, BenchOptions* options) {
  for (int i = 1; i < argc; ++i) {
    std::string name;
    std::string value;
    if (!ParseFlag(argv[i], &name, &value)) {
      continue;
    }
    if (name == "benchmarks") {
      options->benchmarks = value;
    } else if (name == "num") {
      options->num = std::strtoll(value.c_str(), nullptr, 10);
    } else if (name == "key_range") {
      options->key_range = std::atoi(value.c_str());
    } else if (name == "value_size") {
      options->value_size = std::atoi(value.c_str());
    } else if (name == "range_size") {
      options->range_size = std::atoi(value.c_str());
    } else if (name == "bptree_degree") {
      options->bptree_degree = std::atoi(value.c_str());
    } else if (name == "seed") {
      options->seed =
          static_cast<uint64_t>(std::strtoull(value.c_str(), nullptr, 10));
    }
  }
}

static std::string MakeValue(int size) { return std::string(size, 'v'); }

static int RandomKey(std::mt19937_64* rng, int key_range) {
  std::uniform_int_distribution<int> dist(0, key_range - 1);
  return dist(*rng);
}

static void FillRandom(InMemoryDB* db, const BenchOptions& options,
                       std::mt19937_64* rng) {
  const std::string value = MakeValue(options.value_size);
  for (int64_t i = 0; i < options.num; ++i) {
    db->Put(RandomKey(rng, options.key_range), value);
  }
}

static void FillSequential(InMemoryDB* db, const BenchOptions& options) {
  const std::string value = MakeValue(options.value_size);
  for (int64_t i = 0; i < options.num; ++i) {
    db->Put(static_cast<int>(i % options.key_range), value);
  }
}

static std::pair<int64_t, int64_t>
ReadRandom(InMemoryDB* db, const BenchOptions& options, std::mt19937_64* rng) {
  std::string value;
  int64_t found = 0;
  for (int64_t i = 0; i < options.num; ++i) {
    if (db->Get(RandomKey(rng, options.key_range), &value)) {
      ++found;
    }
  }
  return {found, options.num};
}

static std::pair<int64_t, int64_t> ReadSequential(InMemoryDB* db,
                                                  const BenchOptions& options) {
  std::string value;
  int64_t found = 0;
  for (int64_t i = 0; i < options.num; ++i) {
    if (db->Get(static_cast<int>(i % options.key_range), &value)) {
      ++found;
    }
  }
  return {found, options.num};
}

static void RangeSequential(InMemoryDB* db, const BenchOptions& options) {
  int range = std::max(1, options.range_size);
  for (int64_t i = 0; i < options.num; ++i) {
    int start = static_cast<int>((i * range) % options.key_range);
    int end = start + range - 1;
    if (end >= options.key_range) {
      end = options.key_range - 1;
    }
    db->RangeScan(start, end);
  }
}

static void RangeRandom(InMemoryDB* db, const BenchOptions& options,
                        std::mt19937_64* rng) {
  int range = std::max(1, options.range_size);
  for (int64_t i = 0; i < options.num; ++i) {
    int start = RandomKey(rng, options.key_range);
    int end = start + range - 1;
    if (end >= options.key_range) {
      end = options.key_range - 1;
    }
    db->RangeScan(start, end);
  }
}

static void Mixed(InMemoryDB* db, const BenchOptions& options,
                  std::mt19937_64* rng) {
  const std::string value = MakeValue(options.value_size);
  std::string out;
  std::bernoulli_distribution choose_write(0.5);
  for (int64_t i = 0; i < options.num; ++i) {
    int key = RandomKey(rng, options.key_range);
    if (choose_write(*rng)) {
      db->Put(key, value);
    } else {
      db->Get(key, &out);
    }
  }
}

int main(int argc, char** argv) {
  BenchOptions options;
  if (argc == 1) {
    std::cout
        << "memdb_bench options:\n"
        << "  "
           "--benchmarks=fillrandom|fillseq|readrandom|"
           "readseq|mixed|rangeseq|rangerand\n"
        << "  --num=NUM\n"
        << "  --key_range=NUM\n"
        << "  --value_size=NUM\n"
        << "  --range_size=NUM\n"
        << "  --bptree_degree=NUM\n"
        << "  --seed=NUM\n"
        << "Example:\n"
        << "  ./memdb_bench --benchmarks=fillrandom --num=100000 "
           "--value_size=100\n"
        << "  ./memdb_bench --benchmarks=fillrandom,readrandom --num=100000\n";
    return 0;
  }
  ParseArgs(argc, argv, &options);

  if (options.key_range <= 0) {
    options.key_range = static_cast<int>(options.num);
  }
  if (options.benchmarks.empty()) {
    std::cerr << "Missing --benchmarks option\n";
    return 1;
  }

  MemDBOptions db_options;
  db_options.bptree_degree = options.bptree_degree;

  InMemoryDB db(db_options);
  std::mt19937_64 rng(options.seed);

  auto run_benchmark = [&](const std::string& name, bool prefill_reads,
                           std::pair<int64_t, int64_t>* read_counts) {
    if (read_counts) {
      *read_counts = {0, 0};
    }
    if (name == "fillrandom") {
      FillRandom(&db, options, &rng);
    } else if (name == "fillseq") {
      FillSequential(&db, options);
    } else if (name == "readrandom") {
      if (prefill_reads) {
        FillRandom(&db, options, &rng);
      }
      if (read_counts) {
        *read_counts = ReadRandom(&db, options, &rng);
      } else {
        ReadRandom(&db, options, &rng);
      }
    } else if (name == "readseq") {
      if (prefill_reads) {
        FillSequential(&db, options);
      }
      if (read_counts) {
        *read_counts = ReadSequential(&db, options);
      } else {
        ReadSequential(&db, options);
      }
    } else if (name == "rangeseq") {
      if (prefill_reads) {
        FillSequential(&db, options);
      }
      RangeSequential(&db, options);
    } else if (name == "rangerand") {
      if (prefill_reads) {
        FillRandom(&db, options, &rng);
      }
      RangeRandom(&db, options, &rng);
    } else if (name == "mixed") {
      Mixed(&db, options, &rng);
    } else {
      std::cerr << "Unknown workload: " << name << "\n";
      std::exit(1);
    }
  };

  size_t start_pos = 0;
  while (start_pos < options.benchmarks.size()) {
    size_t comma = options.benchmarks.find(',', start_pos);
    if (comma == std::string::npos) {
      comma = options.benchmarks.size();
    }
    std::string name = options.benchmarks.substr(start_pos, comma - start_pos);
    auto start = std::chrono::steady_clock::now();
    std::pair<int64_t, int64_t> read_counts;
    run_benchmark(name, false, &read_counts);
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "benchmark=" << name << " num=" << options.num
              << " key_range=" << options.key_range
              << " value_size=" << options.value_size
              << " range_size=" << options.range_size
              << " bptree_degree=" << options.bptree_degree
              << " elapsed_sec=" << elapsed.count();
    if (name == "readrandom" || name == "readseq") {
      std::cout << " found=" << read_counts.first
                << " requested=" << read_counts.second;
    }
    std::cout << std::endl;
    start_pos = comma + 1;
  }
  return 0;
}
