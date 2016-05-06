#ifndef BENCHMARK_DQS
#define BENCHMARK_DQS

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "logging.h"
#include "thread_pool.h"
#include "counter.h"

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;


namespace dqs {

class LoggerWrapper;

class BenchMark {

public:
  BenchMark();
  ~BenchMark();
  void Start();
private:
  bool BuildDb();
  void PutData();
  void GenKey(std::string* key);
  void ConsumeData();
private:
  leveldb::DB* db_;
  ::baidu::common::ThreadPool* pool_;
  ::baidu::common::Counter counter_;
  int32_t produce_turn_;
};

}

#endif
