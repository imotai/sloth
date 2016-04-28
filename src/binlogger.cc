#include "binlogger.h"

#include <boost/lexical_cast.hpp>
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "logging.h"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

namespace sloth {

BinLogger::BinLogger(const std::string& db_path):db_path_(db_path),
  db_(NULL), size_(0) {}

BinLogger::~BinLogger() {}

bool BinLogger::Recover() {
  leveldb::Options options;
  options.block_cache = leveldb::NewLRUCache(100 * 1024 * 1024);
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, db_path_, &db_);
  if (status.ok()) {
    LOG(DEBUG, "[BinLogger] create db successfully");
    return true;
  }
  LOG(DEBUG, "[BinLogger] fail to create db");
  return false;
}

uint64_t BinLogger::Append(const Entry& log) {
  uint64_t offset = size_ + 1;
  LOG(INFO, "[BinLogger] append log with offset %lld term %lld", offset, log.term());
  std::string value;
  bool serialize_ok = log.SerializeToString(&value);
  if (!serialize_ok) {
    LOG(WARNING, "[BinLogger] fail to serialize entry");
    return 0;
  }
  leveldb::Status put_ok = db_->Put(leveldb::WriteOptions(), 
                                    boost::lexical_cast<std::string>(offset),
                                    value);
  if (put_ok.ok()) {
    return offset;
  }
  size_ = offset;
  LOG(WARNING, "[BinLogger] fail to append log to leveldb with term %lld offset %lld",
      log.term(), offset);
  return 0;
}
  
bool BinLogger::GetLog(uint64_t index, Entry* log) {
  if (!log) {
    LOG(WARNING, "input log is NULL");
    return false;
  }
  std::string value;
  leveldb::Status get_ok = db_->Get(leveldb::ReadOptions(), 
                                    boost::lexical_cast<std::string>(index),
                                    &value);
  if (!get_ok.ok()) {
    LOG(WARNING, "fail to get value with key %lld", index);
    return false;
  }
  bool parse_ok = log->ParseFromString(value);
  if (!parse_ok) {
    LOG(WARNING, "fail to parse %s to entry", value.c_str());
    return false;
  }
  return true;
}

} // end of namespace sloth

