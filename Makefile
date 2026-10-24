CXX ?= g++
CPPFLAGS += -Iinclude
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra
LDLIBS += -lssl -lcrypto -lpthread
BUILD_DIR ?= build
SOURCES := $(shell find src -name '*.cpp' | sort)
OBJECTS := $(patsubst src/%.cpp,$(BUILD_DIR)/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d) $(BUILD_DIR)/test_main.d

spred: $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(BUILD_DIR)/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) spred

-include $(DEPS)
.PHONY: clean

TEST_OBJECTS := $(filter-out $(BUILD_DIR)/main.o,$(OBJECTS))
$(BUILD_DIR)/test_main.o: tests/test_main.cpp tests/cases.hpp
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

$(BUILD_DIR)/test_bot: $(TEST_OBJECTS) $(BUILD_DIR)/test_main.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

check: spred $(BUILD_DIR)/test_bot
	python3 tests/run_checks.py --build-dir $(BUILD_DIR)

.PHONY: check

test: check
.PHONY: test
