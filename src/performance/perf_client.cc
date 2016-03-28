#include "performance/perf_client.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>

#include <syscall.h>
#include <sys/types.h>
#include <gflags/gflags.h>
#include "rpc/rpc_client.h"
#include <stdlib.h>
#include "logging.h"

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;


DECLARE_string(dqs_server);
DECLARE_int32(perf_thread_count);
DECLARE_string(perf_value);


namespace dqs {

PerfClient::PerfClient():g_done_count_(0),
  g_send_count_(0),
  pool(FLAGS_perf_thread_count){}

PerfClient::~PerfClient(){}

void PerfClient::Start() {
  for (int32_t i =0 ; i < FLAGS_perf_thread_count; ++i) {
    pool.AddTask(boost::bind(&PerfClient::PutMsg, this));
  }
}

void PerfClient::PutMsg() {
  uint64_t thread_id = syscall(__NR_gettid);
  uint32_t delay_offet  = 1000 * 60 * 60; // one hour
  RpcClient* client = new RpcClient();
  Dqs_Stub* dqs_stub = NULL;
  bool ok = client->GetStub(FLAGS_dqs_server, &dqs_stub);
  if (!ok) {
    LOG(WARNING, "fail to build dqs stub");
    return;
  }
  while (true) {
    PutDelayMsgRequest* req = new PutDelayMsgRequest();
    req->set_delay(rand() % delay_offet);
    req->set_queue(boost::lexical_cast<std::string>(thread_id));
    req->set_value(FLAGS_perf_value);
    PutDelayMsgResponse* response = new PutDelayMsgResponse();
    boost::function<void (const PutDelayMsgRequest*, PutDelayMsgResponse*, bool, int)> callback;
    callback = boost::bind(&PerfClient::PutMsgCallback, this, _1, _2, _3, _4);
    client->AsyncRequest(dqs_stub,
                         &Dqs_Stub::PutDelayMsg,
                            req,
                            response,
                            callback,
                           10000, 0);
    g_send_count_++;
    LOG(INFO, "send count %ld", g_send_count_);
  }

}

void PerfClient::PutMsgCallback(const PutDelayMsgRequest* request,
                      PutDelayMsgResponse* response,
                      bool failed, int) {
  if (!failed) {
    g_done_count_++;
    LOG(INFO, "process count %ld", g_done_count_);
  }
  delete request;
  delete response;
}

}// end of dqs
