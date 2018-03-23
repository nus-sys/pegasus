.DEFAULT_GOAL := all

# General definitions
SRC_DIR := .
BIN_DIR := ./bin
TEST_DIR := ./tests
BUILD_DIR := ./build
BIN := ./bin/emulator
TEST_BIN := $(TEST_DIR)/memcachekv-test

SRCS := $(shell find $(SRC_DIR) -path $(BUILD_DIR) -prune -o -name "*.cc" -print)
OBJS := $(SRCS:%.cc=$(BUILD_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

BIN_SRCS := $(filter-out $(wildcard $(TEST_DIR)/*.cc), $(SRCS))
BIN_OBJS := $(BIN_SRCS:%.cc=$(BUILD_DIR)/%.o)
TEST_SRCS := $(filter-out $(wildcard $(BIN_DIR)/*.cc), $(SRCS))
TEST_OBJS := $(TEST_SRCS:%.cc=$(BUILD_DIR)/%.o)

INC_DIRS := $(shell find $(SRC_DIR) -type d)
INC_DIRS += $(BUILD_DIR)/gen
INC_FLAGS := $(addprefix -I,$(INC_DIRS))

CPPFLAGS := $(INC_FLAGS) -MMD -MP
CFLAGS := -g -Wall -pthread -O3
CXXFLAGS := -std=c++0x -O3
LDFLAGS := -levent_pthreads

# Protobuf definitions
PROTOC := protoc
PROTOS := $(shell find $(SRC_DIR) -name "*.proto")
PROTOSRCS := $(PROTOS:%.proto=$(BUILD_DIR)/gen/%.pb.cc)
PROTOOBJS := $(PROTOS:%.proto=$(BUILD_DIR)/%.o)
PROTO_CFLAGS := $(shell pkg-config --cflags protobuf)
PROTO_LDFLAGS := $(shell pkg-config --libs protobuf)
CFLAGS += $(PROTO_CFLAGS)
LDFLAGS += $(PROTO_LDFLAGS)

# Libevent definitions
LIBEVENT_CFLAGS := $(shell pkg-config --cflags libevent)
LIBEVENT_LDFLAGS := $(shell pkg-config --libs libevent)
CFLAGS += $(LIBEVENT_CFlAGS)
LDFLAGS += $(LIBEVENT_LDFLAGS)

# Google Test definitions
GTEST_DIR := /usr/src/gtest
GTEST_OBJS := $(BUILD_DIR)/gtest/gtest-all.o $(BUILD_DIR)/gtest/gtest_main.o
GTEST_MAIN := $(BUILD_DIR)/gtest/gtest_main.a

# Generate protobuf files
$(PROTOSRCS): $(BUILD_DIR)/gen/%.pb.cc: %.proto
	$(MKDIR_P) $(dir $@)
	$(PROTOC) --cpp_out=$(BUILD_DIR)/gen $^

# Object files
$(PROTOOBJS): $(BUILD_DIR)/%.o: $(BUILD_DIR)/gen/%.pb.cc
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CFLAGS) $(CXXFLAGS) -c $< -o $@

$(OBJS): $(BUILD_DIR)/%.o: %.cc $(PROTOSRCS)
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CFLAGS) $(CXXFLAGS) -c $< -o $@

$(GTEST_OBJS): $(BUILD_DIR)/gtest/%.o: $(GTEST_DIR)/src/%.cc
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CFLAGS) $(CXXFLAGS) -I$(GTEST_DIR) -c $< -o $@

$(GTEST_MAIN): $(BUILD_DIR)/gtest/gtest-all.o $(BUILD_DIR)/gtest/gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

# Linking
$(BIN): $(BIN_OBJS) $(PROTOOBJS)
	$(CXX) $(BIN_OBJS) $(PROTOOBJS) -o $@ $(LDFLAGS)

$(TEST_BIN): $(TEST_OBJS) $(PROTOOBJS) $(GTEST_MAIN)
	$(CXX) $(TEST_OBJS) $(PROTOOBJS) $(GTEST_MAIN) -o $@ $(LDFLAGS)

-include $(DEPS)

MKDIR_P := mkdir -p

# Targets
.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
	$(RM) $(BIN)

.PHONY: all
all: $(BIN)

$(TEST_BIN:%=run-%): run-%: %
	$<

.PHONY: test
test: $(TEST_BIN:%=run-%)
