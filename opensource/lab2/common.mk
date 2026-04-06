CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread
LDFLAGS = -lpthread

GTEST_DIR = $(TOP)/../googletest
GTEST_INCLUDES = -I$(GTEST_DIR)/include -I$(GTEST_DIR)

# Rule for the core gtest logic
gtest-all.o: $(GTEST_DIR)/src/gtest-all.cc
	$(CXX) $(CXXFLAGS) $(GTEST_INCLUDES) -c $< -o $@

# Rule for the standard gtest main() entry point
gtest_main.o: $(GTEST_DIR)/src/gtest_main.cc
	$(CXX) $(CXXFLAGS) $(GTEST_INCLUDES) -c $< -o $@

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(GTEST_INCLUDES) -c $< -o $@
