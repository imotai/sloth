#ifndef RAFT_BIN_LOGGER_H
#define RAFT_BIN_LOGGER_H

#include <string>
#include <stdint.h>
#include "leveldb/db.h"
#include "proto/sloth_node.pb.h"

namespace sloth {

class BinLogger {

public:
  BinLogger(const std::string& db_path);
  ~BinLogger();
  bool Recover();
  // append log and return the log index
  uint64_t Append(const Entry& log);
  // get log by log index
  bool GetLog(uint64_t index, Entry* log);

private:
  std::string db_path_;
  leveldb::DB* db_;
  uint64_t size_;
};

}
#endif
