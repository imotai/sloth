#include "sloth_node_impl.h"

#include <vector>
#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace sloth {

SlothNodeImpl::SlothNodeImpl():core_(NULL), queue_(NULL){
  queue_ = new boost::lockfree::queue<SlothEvent>(1024);
  core_ = new SlothCore(queue_);
}

SlothNodeImpl::~SlothNodeImpl() {}

bool SlothNodeImpl::Init() {
  bool ok = core_->Init();
  if (!ok) {
    return false;
  }
  core_->Start();
  return true;
}

void SlothNodeImpl::AppendEntries(RpcController* controller,
                                  const AppendEntriesRequest* request,
                                  AppendEntriesResponse* response,
                                  Closure* done) {
  AppendEntryData* data = new AppendEntryData(request, response, done);
  SlothEvent e;
  e.data = data;
  e.type = kAppendEntry;
  while(!queue_->push(e));
}



void SlothNodeImpl::RequestVote(RpcController* controller, 
                                const RequestVoteRequest* request,
                                RequestVoteResponse* response,
                                Closure* done) {
  RequestVoteData* data = new RequestVoteData();
  data->response = response;
  data->request = request;
  data->done = done;
  SlothEvent e;
  e.data = data;
  e.type = kRequestVote;
  while(!queue_->push(e));
}

void SlothNodeImpl::GetClusterStatus(RpcController* controller,
                                    const GetClusterStatusRequest* request,
                                    GetClusterStatusResponse* response,
                                    Closure* done) {
}


}
