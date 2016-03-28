#include "dqs_impl.h"

#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>
#include "logging.h"
#include "timer.h"


DECLARE_string(ins_servers);
DECLARE_string(dqs_root_path);

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;

namespace dqs {

DqsImpl::DqsImpl():ins_(NULL) {
  ins_ = new InsSDK(FLAGS_ins_servers);
}

DqsImpl::~DqsImpl() {
  delete ins_;
}

void DqsImpl::PutDelayMsg(RpcController* controller,
                          const PutDelayMsgRequest* request,
                          PutDelayMsgResponse* response,
                          Closure* done) {
  std::string key = FLAGS_dqs_root_path + "/" + request->queue() +
                    "/" + boost::lexical_cast<std::string>(::baidu::common::timer::get_micros());
  ::galaxy::ins::sdk::SDKError err;
  bool ok = ins_->Put(key, request->value(), &err);
  if (!ok) {
    LOG(WARNING, "fail to put key %s", key.c_str());
    response->set_status(kRpcError);
    done->Run();
    return;
  }
  //TODO add time consume log
  LOG(INFO, "put key %s successfully", key.c_str());
  response->set_status(kRpcOk);
  done->Run();
}

} // end of dqs
