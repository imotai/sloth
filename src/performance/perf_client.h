#ifndef DQS_PERF_CLIENT_H
#define DQS_PERF_CLIENT_H

#include <stdint.h>
#include "proto/dqs.pb.h"
#include "thread_pool.h"

using baidu::common::ThreadPool;
namespace dqs {

class PerfClient {

public:
  PerfClient();
  ~PerfClient();
  void Start();
private:
  void PutMsg();
  void PutMsgCallback();
  void PutMsgCallback(const PutDelayMsgRequest* request,
                      PutDelayMsgResponse* response,
                      bool failed, int);
private:
  volatile uint64_t g_done_count_;
  volatile uint64_t g_send_count_;
  ThreadPool pool;
};

}

#endif
