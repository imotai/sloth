#ifndef SLOTH_NODE_H
#define SLOTH_NODE_H

#include <map>
#include "proto/sloth_node.pb.h"
#include "sloth_types.h"
#include "thread_pool.h"
#include "rpc/rpc_client.h"
#include "mutex.h"

using ::baidu::common::ThreadPool;
using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;

namespace sloth {


class SlothNodeImpl : public SlothNode {

public:
  SlothNodeImpl();
  ~SlothNodeImpl();

  // init raft node , this function is not thread safe
  bool Init();

  // request vote for requestor
  void RequestVote(RpcController* controller,
                   const RequestVoteRequest* request,
                   RequestVoteResponse* response,
                   Closure* done);

  // append entry to follower
  void AppendEntries(RpcController* controller,
                     const AppendEntriesRequest* request,
                     AppendEntriesResponse* response,
                     Closure* done);
  void GetClusterStatus(RpcController* controller,
                        const GetClusterStatusRequest* request,
                        GetClusterStatusResponse* response,
                        Closure* done);
private:

};

}
#endif
