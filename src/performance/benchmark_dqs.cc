#include "benchmark_dqs.h"

#include <stdlib.h>
#include <gflags/gflags.h>
#include <stdio.h>
#include <syscall.h>
#include <sys/types.h>
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"
#include "leveldb/posix_logger.h"
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include "timer.h"

DEFINE_int32(max_delay_time, 1000 * 60 * 10, "the max delay time of msg");
DEFINE_int32(min_delay_time, 1000 * 5 , "the min delay time of msg");
DEFINE_int32(prefetch_time_offset, 1000 * 5 , "the min delay time of msg");
DEFINE_int32(produce_turn, 100, "the turn of produce data");
DEFINE_string(db_path, "./db", "the path of db");
DEFINE_int32(key_count_per_second, 40000, "write qps");

namespace dqs {

const static std::string data_prefix="delay_msg#";
const static std::string data_current_key="DELAY_MSG_CURRENT#";

uint64_t GetTid() {
  return syscall(__NR_gettid);
}

BenchMark::BenchMark():db_(NULL), pool_(NULL), counter_(), produce_turn_(0), gen_key_turn_(0){}
BenchMark::~BenchMark() {}

void BenchMark::Start() {
  srand(::baidu::common::timer::get_micros());
  BuildDb();
  pool_ = new ::baidu::common::ThreadPool(2);

  pool_->AddTask(boost::bind(&BenchMark::PutData, this));

  pool_->AddTask(boost::bind(&BenchMark::ConsumeData, this));
}

bool BenchMark::BuildDb() {
  leveldb::Options options;
//  FILE* file = fopen("/dev/stdout", "w");
//  leveldb::Logger* log = new ::leveldb::PosixLogger(file, &::dqs::GetTid);
//  options.info_log = log;
  options.block_cache = leveldb::NewLRUCache(100 * 1024 * 1024);
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, FLAGS_db_path, &db_);
  if (!status.ok()) {
    LOG(WARNING, "fail to open leveldb");
    return false;
  }
  return true;
}


void BenchMark::PutData() {
  LOG(INFO, "PENDING %lld", counter_.Get());
  int64_t now = ::baidu::common::timer::get_micros();
  leveldb::WriteOptions write_options;
  std::string value;
  value.resize(3* 128);
  for (int32_t i = 0; i < FLAGS_key_count_per_second; i++) {
    std::string key;
    GenKey(&key);
    db_->Put(write_options, key, value);
    counter_.Inc();
  }
  produce_turn_++;
  if (produce_turn_ >= FLAGS_produce_turn) {
    LOG(INFO, "stop produce data");
    return;
  }
  int64_t consume = ::baidu::common::timer::get_micros() - now;
  LOG(INFO, "write %d key using %ld ms",FLAGS_key_count_per_second, consume/1000);
  int64_t delay = (1000000 - consume )/1000;
  if (delay > 0) {
    pool_->DelayTask(delay, boost::bind(&BenchMark::PutData, this));
  } else {
    pool_->AddTask(boost::bind(&BenchMark::PutData, this));
  }
}


void BenchMark::GenKey(std::string* key) {
  gen_key_turn_++;
  uint32_t offset = FLAGS_max_delay_time - FLAGS_min_delay_time;
  int32_t int_key = rand() % offset + ::baidu::common::timer::now_time();
  *key = data_prefix + boost::lexical_cast<std::string>(int_key) + "#" + boost::lexical_cast<std::string>(gen_key_turn_);
}

void BenchMark::ConsumeData() {
  int64_t start = ::baidu::common::timer::get_micros();
  int32_t now = ::baidu::common::timer::now_time();
  int64_t end_time = now + FLAGS_prefetch_time_offset;
  std::string end_key = data_prefix +  boost::lexical_cast<std::string>(end_time);
  std::string start_key;
  leveldb::Status status = db_->Get(leveldb::ReadOptions(), data_current_key, &start_key);
  if (!status.ok()) {
    start_key = data_prefix + boost::lexical_cast<std::string>(now);
  }
  LOG(INFO, "consume range [%s, %s]", start_key.c_str(), end_key.c_str());
  leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
  leveldb::WriteBatch batch;
  int count = 0;
  for (it->Seek(start_key); it->Valid() && it->key().ToString() < end_key; it->Next()) {
    LOG(INFO, "consume key %s", it->key().ToString().c_str());
    batch.Delete(it->key());
    counter_.Dec();
    count++;
  }
  batch.Put(data_current_key, end_key);
  db_->Write(leveldb::WriteOptions(), &batch);
  int64_t consume = ::baidu::common::timer::get_micros() - start;
  LOG(INFO, "process %d msg with %ld ms, PENDING %lld", count, consume/1000, counter_.Get());
  int64_t delay = FLAGS_prefetch_time_offset - consume/1000;
  if (delay > 0) {
    pool_->DelayTask(delay, boost::bind(&BenchMark::ConsumeData, this));
  } else {
    pool_->AddTask(boost::bind(&BenchMark::ConsumeData, this));
  }
}

}
