#ifndef SLOTH_CORE_H
#define SLOTH_CORE_H
#include <boost/lockfree/queue.hpp>
#include "proto/sloth_node.pb.h"
#include "thread_pool.h"
#include "rpc/rpc_client.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::ThreadPool;

namespace sloth {

enum SlothEventType {
  kAppendEntry = 0;
  kElectionTimeout = 1;
  kVoteTimeout = 2;

  kAppendEntryCallback = 3;
  kRequestVoteCallback = 4;
};

struct SlothEvent {
  void* data;
  SlothEventType type;
};

struct VoteCount {
  int64_t term;
  int64_t count;
  VoteCount():term(0),count(0){}
  void Reset();
};

struct AppendEntryData {
  const AppendEntriesRequest* request;
  AppendEntriesResponse* response;
  Closure* done;
  AppendEntryData(const AppendEntriesRequest* request,
                  AppendEntriesResponse* response,
                  Closure* done):request(request),
  response(response),done(done){}
};

struct ElectionTimeoutData {
  // the term when bind function
  int64_t term;
};

struct VoteTimeoutData {
  int64_t term;
};

// the core logic for raft 
// all functions will be processed by one thread and no mutex lock
class SlothCore {

public:
  SlothCore(boost::lockfree::queue<SlothEvent>* queue);
  ~SlothCore();
  void Run();

private:
  void HandleAppendEntry(AppendEntryData* data);
  void HandleElectionTimeout(ElectionTimeoutData* data);
  void ResetElectionTimeout();
  uint32_t GenRandTime();
  void DispatchElectionTimeout(uint64_t term);
  void SendVoteRequest(const std::string& endpoint);
private:
  uint64_t current_term_;
  SlothNodeRole role_;
  boost::lockfree::queue<SlothEvent>* queue_;
  // the worker for processing all roft event
  ThreadPool* core_worker_;
  // the worker for dispatching time_out event;
  ThreadPool* time_worker_;

  // for follower 
  int64_t election_timeout_task_id_;
  // for candidate 
  int64_t vote_timeout_task_id_;
  VoteCount count;
  // for leader  dispatch append entry event
  ThreadPool* append_entry_worker_;

  volatile bool running_;
  RpcClient* rpc_client_;
  // for node
  std::string endpoint_;
};

}
#endif
