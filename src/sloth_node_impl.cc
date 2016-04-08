#include "sloth_node_impl.h"

#include <vector>
#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"

DECLARE_string(node_list);
DECLARE_int32(node_idx);
DECLARE_int32(max_follower_elect_timeout);
DECLARE_int32(min_follower_elect_timeout);
DECLARE_int32(replicate_log_interval);
DECLARE_int32(wait_vote_back_timeout);

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace sloth {

SlothNodeImpl::SlothNodeImpl() {

}

SlothNodeImpl::~SlothNodeImpl() {}

bool SlothNodeImpl::Init() {
  return false;
}

void SlothNodeImpl::AppendEntries(RpcController* controller,
                                  const AppendEntriesRequest* request,
                                  AppendEntriesResponse* response,
                                  Closure* done) {
}



void SlothNodeImpl::RequestVote(RpcController* controller, 
                                const RequestVoteRequest* request,
                                RequestVoteResponse* response,
                                Closure* done) {
}

void SlothNodeImpl::GetClusterStatus(RpcController* controller,
                                    const GetClusterStatusRequest* request,
                                    GetClusterStatusResponse* response,
                                    Closure* done) {
}


}
