# OPT ?= -O2 -DNDEBUG     # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror  # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols

# Thirdparty
include depends.mk

CC = gcc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS =  -Wl,-soname -Wl,

INCPATH += -I./src $(DEPS_INCPATH) 
CFLAGS += -std=c99 $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz 


PREFIX=./output


PROTO_FILE = $(wildcard src/proto/*.proto)
PROTO_SRC = $(patsubst %.proto,%.pb.cc,$(PROTO_FILE))
PROTO_HEADER = $(patsubst %.proto,%.pb.h,$(PROTO_FILE))
PROTO_OBJ = $(patsubst %.proto,%.pb.o,$(PROTO_FILE))

DQS_SRC = $(wildcard src/*.cc)
DQS_OBJ = $(patsubst %.cc, %.o, $(DQS_SRC))
DQS_HEADER = $(wildcard src/*.h)

PERF_SRC = $(wildcard src/performance/*.cc)
PERF_OBJ = $(patsubst %.cc, %.o, $(PERF_SRC))
PERF_HEADER = $(wildcard src/performance/*.h)

BIN = dqs perf
all: $(BIN)  

.PHONY: all clean test
# Depends
$(DQS_OBJ) : $(PROTO_HEADER)
$(PERF_OBJ) : $(PERF_HEADER)
# Targets
dqs: $(DQS_OBJ) $(PROTO_OBJ)
	$(CXX) $(DQS_OBJ) src/proto/dqs.pb.o  -o $@  $(LDFLAGS)

perf : $(PERF_OBJ) $(PROTO_OBJ)
	$(CXX) $(PERF_OBJ) src/proto/dqs.pb.o  -o $@  $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $< -o $@

%.pb.h %.pb.cc: %.proto
	$(PROTOC) --proto_path=src/proto/  --cpp_out=src/proto/ $<

clean:
	rm -rf $(BIN) 
	rm -rf $(DQS_OBJ) $(PERF_OBJ)  
	rm -rf $(PROTO_OBJ)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)

install: $(BIN) $(LIBS)
	mkdir -p $(PREFIX)/bin
	mkdir -p $(PREFIX)/lib
	mkdir -p $(PREFIX)/include/sdk
	cp $(BIN) $(PREFIX)/bin
	cp $(LIBS) $(PREFIX)/lib
	cp src/sdk/*.h $(PREFIX)/include/sdk



