#ifndef RAFT_BIN_LOGGER_H
#define RAFT_BIN_LOGGER_H

#include <string>
#include <stdint.h>
#include "leveldb/db.h"

namespace raft {

struct LogEntry {
  uint64_t term;
  uint64_t log_index;
  std::string key;
  std::string value;
};

class BinLogger {

public:
  BinLogger();
  ~BinLogger();
  // append log and return the log index
  uint64_t Append(const LogEntry& log);
  // get log by log index
  bool GetLog(uint64_t index, LogEntry* log);
private:
  leveldb::DB* db_;
};

}
#endif
