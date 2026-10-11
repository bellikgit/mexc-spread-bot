CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2
LDLIBS    = -lssl -lcrypto -lpthread

spred: src/spred.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f spred

.PHONY: clean
