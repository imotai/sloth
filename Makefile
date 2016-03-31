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

SLOTH_SRC = $(wildcard src/*.cc)
SLOTH_OBJ = $(patsubst %.cc, %.o, $(SLOTH_SRC))
SLOTH_HEADER = $(wildcard src/*.h)

BIN = sloth 
all: $(BIN)  

.PHONY: all clean test
# Depends
$(PROTO_OBJ) : $(PROTO_HEADER)
$(SLOTH_OBJ) : $(PROTO_OBJ) 
# Targets
sloth: $(SLOTH_OBJ) $(PROTO_OBJ)
	$(CXX) $(SLOTH_OBJ) $(PROTO_OBJ) -o $@  $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $< -o $@

%.pb.h %.pb.cc: %.proto
	$(PROTOC) --proto_path=src/proto/  --cpp_out=src/proto/ $<

clean:
	rm -rf $(BIN) 
	rm -rf $(SLOTH_OBJ)  
	rm -rf $(PROTO_OBJ)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)



