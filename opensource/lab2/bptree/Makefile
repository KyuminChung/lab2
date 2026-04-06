# DO NOT MODIFY!
TOP=..
include $(TOP)/common.mk

.DEFAULT_GOAL := all

LDLIBS=-lpthread
GTEST_OBJS=gtest-all.o

TEST_TARGET=memdb_test
BENCH_TARGET=memdb_bench

TEST_SRCS=memdb_test.cc memdb.cc bptree.cc
BENCH_SRCS=memdb_bench.cc memdb.cc bptree.cc

TEST_OBJS=$(TEST_SRCS:.cc=.o)
BENCH_OBJS=$(BENCH_SRCS:.cc=.o)

all: $(TEST_TARGET) $(BENCH_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(GTEST_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

$(BENCH_TARGET): $(BENCH_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TEST_OBJS) $(BENCH_OBJS) $(GTEST_OBJS) $(TEST_TARGET) $(BENCH_TARGET)

.PHONY: all clean
