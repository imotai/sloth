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
  // for follower that receives entry from master
  kAppendEntry = 0,
  kElectionTimeout = 1,
  kVoteTimeout = 2,
  // for leader that receives callback from follower or candidate
  kSendAppendEntryCallback = 3,
  kRequestVoteCallback = 4,
  // candidate request a vote
  kRequestVote = 5
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

struct RequestVoteData {
  const RequestVoteRequest* request;
  RequestVoteResponse* response;
  Closure* done;
};

struct ElectionTimeoutData {
  // the term when bind function
  uint64_t term;
};

struct VoteTimeoutData {
  uint64_t term;
};

// the vote from other node
struct VoteData {
  // the term when candidate request a vote
  uint64_t term_snapshot;
  uint64_t term_from_node;
  bool vote_granted;
};

// callback from follower or candidate
struct SendAppendEntriesCallbackData {
  uint64_t term_snapshot;
  uint64_t term_from_node;
  bool success;
};

// the core logic for raft 
// all functions will be processed by one thread and no mutex lock
class SlothCore {

public:
  SlothCore(boost::lockfree::queue<SlothEvent>* queue);
  ~SlothCore();
  bool Init();
  void Start();
  void Stop();

private:
  // boot raft core logic in single thread
  void Run();
  // process append entry request from leader
  void HandleAppendEntry(AppendEntryData* data);
  // send append entry to followers
  void HandleSendEntry();
  // process election timeout event
  void HandleElectionTimeout(ElectionTimeoutData* data);
  // process wait vote timeout for candidate
  void HandleWaitVoteTimeout(VoteTimeoutData* data);
  // when receiving heart beat from leader,
  // reset all timeout checker
  void ResetElectionTimeout();
  // when request a vote , reset wait vote timeout checker
  // this function can be invoked by candidate
  void ResetWaitVoteTimeout();
  // generate randome election timeout
  uint32_t GenRandTime();
  // dispatch election timeout event 
  // and put it to queue
  void DispatchElectionTimeout(uint64_t term);
  // dispatch wait vote timeout event 
  // and put it to queue
  void DispatchWaitVoteTimeout(uint64_t term);
  void SendVoteRequest(const std::string& endpoint);
  void SendVoteRequestCallback(uint64_t term,
                               const RequestVoteRequest* request,
                               RequestVoteResponse* response,
                               bool failed,
                               int error);
  // process the vote for me from other node
  void HandleVote(VoteData* data);
  // when a node become a leader 
  // stop check election timeout
  void StopCheckElectionTimeoutTask();
  // when a node become a leader or follower
  // stop check vote timeout
  void StopCheckVoteTimeoutTask();

  void DoReplicateLog();
  void SendAppendEntries(const std::string endpoint);
  void SendAppendEntriesCallback(uint64_t term,
                                 const AppendEntriesRequest* request,
                                 AppendEntriesResponse* response,
                                 bool failed,
                                 int error);
  // process append entry callback from followers and candidate
  void HandleSendEntriesCallback(SendAppendEntriesCallbackData* data);
  // process request vote from candidate
  void HandleRequestVote(RequestVoteData* data);
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
  int64_t append_entry_task_id_;

  volatile bool running_;
  RpcClient* rpc_client_;

  // for node
  int32_t id_;
  int32_t leader_id_;
  // cluster information
  std::vector<std::string> cluster_;
};

}
#endif
