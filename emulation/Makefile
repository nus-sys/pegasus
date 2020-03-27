.DEFAULT_GOAL := all

# General definitions
SRC_DIR := .
BIN_DIR := ./bin
BUILD_DIR := ./build
EMULATOR := ./bin/emulator
CLI := ./bin/cli

SRCS := $(shell find $(SRC_DIR) -path $(BUILD_DIR) -prune -o -path $(BIN_DIR) -prune -o -name '*.cc' -print)
OBJS := $(SRCS:%.cc=$(BUILD_DIR)/%.o)
DEPS := $(shell find $(BUILD_DIR) -type f -name '*.d')

EMULATOR_OBJS := $(OBJS) $(BUILD_DIR)/bin/emulator.o
CLI_OBJS := $(OBJS) $(BUILD_DIR)/bin/cli.o

INC_DIRS := $(shell find $(SRC_DIR) -type d)
INC_DIRS += $(BUILD_DIR)/gen
INC_FLAGS := $(addprefix -I,$(INC_DIRS))

CPPFLAGS := $(INC_FLAGS) -MMD -MP
CFLAGS := -g -Wall -pthread -O3
CXXFLAGS := -std=c++0x -O3
LDFLAGS := -levent_pthreads

# Libevent definitions
LIBEVENT_CFLAGS := $(shell pkg-config --cflags libevent)
LIBEVENT_LDFLAGS := $(shell pkg-config --libs libevent)
CFLAGS += $(LIBEVENT_CFlAGS)
LDFLAGS += $(LIBEVENT_LDFLAGS)

# Intel TBB definitions
LDFLAGS += -ltbb

# Pthread library
LDFLAGS += -lpthread

# Object files
$(BUILD_DIR)/%.o: %.cc
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CFLAGS) $(CXXFLAGS) -c $< -o $@

# Linking
$(EMULATOR): $(EMULATOR_OBJS)
	$(CXX) $(EMULATOR_OBJS) -o $@ $(LDFLAGS)

$(CLI): $(CLI_OBJS)
	$(CXX) $(CLI_OBJS) -o $@ $(LDFLAGS)

-include $(DEPS)

MKDIR_P := mkdir -p

# Targets
.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
	$(RM) $(EMULATOR) $(CLI)

.PHONY: all
all: $(EMULATOR) $(CLI)
