#ifndef DQS_IMPL_H
#define DQS_IMPL_H

#include "proto/dqs.pb.h"
#include <sofa/pbrpc/pbrpc.h>
#include "ins_sdk.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::galaxy::ins::sdk::InsSDK;

namespace dqs {

class DqsImpl : public Dqs {

public:
  DqsImpl();
  ~DqsImpl();
  void PutDelayMsg(RpcController* controller,
                   const PutDelayMsgRequest* request,
                   PutDelayMsgResponse* response,
                   Closure* done);
private:
  InsSDK* ins_;
};

}
#endif
