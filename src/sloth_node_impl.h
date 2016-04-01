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

struct NodeIndex {
  uint64_t next_index;
  uint64_t match_index;
  NodeIndex():next_index(0), match_index(0){}
};

struct VoteCount {
  // the term when start a election
  uint64_t term;
  // the count of vote
  uint64_t count;
  VoteCount():term(0),count(0) {}
};

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
  void HandleVoteTimeout();
  void HandleElectionTimeout();
  void SendVoteRequest(const std::string& endpoint);
  void SendVoteRequestCallback(const std::string endpoint,
                               const RequestVoteRequest* request,
                               RequestVoteResponse* response,
                               bool failed, int error);

  void DoReplicateLog();
  void SendAppendEntries(const std::string& endpoint);
  void SendAppendEntriesCallback(const AppendEntriesRequest* request,
                                 AppendEntriesResponse* response,
                                 bool failed, int error);
  uint32_t GenerateRandTimeout();

private:
  Mutex mu_;
  // the term of raft
  uint64_t current_term_;
  // the state of node
  uint32_t state_;
  // the commit index
  uint64_t commit_index_;
  // the last applied index 
  uint64_t last_applied_index_;
  std::string node_endpoint_;
  RpcClient* client_;

  // for follower
  ThreadPool* election_timeout_checker_; 
  int64_t election_timeout_task_id_;
  std::string leader_endpoint_;
  // for leaders
  std::map<std::string, NodeIndex>* node_index_;
  ThreadPool* replicate_log_worker_;
  // for candidate
  VoteCount vote_count_;
  ThreadPool* vote_timeout_checker_;
  int64_t vote_timeout_task_id_;

};

}
#endif
