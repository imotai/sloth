#include "sloth_core.h"

#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include "gflags/gflags.h"

DECLARE_string(node_list);
DECLARE_int32(node_idx);
DECLARE_int32(max_follower_elect_timeout);
DECLARE_int32(min_follower_elect_timeout);
DECLARE_int32(replicate_log_interval);
DECLARE_int32(wait_vote_back_timeout);

namespace sloth {

SlothCore::SlothCore(boost::lockfree::queue<SlothEvent>* queue):current_term_(1),
  role_(kFollower),queue_(queue),core_worker_(NULL),
  time_worker_(NULL),election_timeout_task_id_(0),
  vote_timeout_task_id_(0),count(),append_entry_worker_(NULL),
  append_entry_task_id_(0),running_(false),
  rpc_client_(NULL), id_(-1), leader_id_(-1), cluster_(){
  core_worker_ = new ThreadPool(1);
  time_worker_ = new ThreadPool(3);
  append_entry_worker_ = new ThreadPool(5);
  rpc_client_ = new RpcClient();
}

SlothCore::~SlothCore() {}

bool SlothCore::Init() {
  boost::split(cluster_, FLAGS_node_list, boost::is_any_of(","));
  if (cluster_.size() <=2) {
    return false;
  }
  if ((size_t)FLAGS_node_idx >= cluster_.size()) {
    return false;
  }
  id_ = FLAGS_node_idx;
  return true;
}

void SlothCore::Start() {
  if (running_) {
    return;
  }
  running_ = true;
  core_worker_->AddTask(boost::bind(&SlothCore::Run, this));
}

void SlothCore::Stop() {
  running_ = false;
}

void SlothCore::Run() {
  while (running_) {
    SlothEvent event;
    bool ok = queue_->pop(event);
    if (!ok) {
      continue;
    }
    switch (event.type) {
      case kAppendEntry:
        AppendEntryData* data = reinterpret_cast<AppendEntryData*>(event.data);
        HandleAppendEntry(data);
        break;
      case kElectionTimeout:
        ElectionTimeoutData* data = reinterpret_cast<ElectionTimeoutData*>(event.data);
        HandleElectionTimeout(data);
        break;
      case kRequestVoteCallback:
        VoteData* data = reinterpret_cast<VoteData*>(event.data);
        HandleVote(data);
        break;
      case kVoteTimeout:
        VoteTimeoutData* data = reinterpret_cast<VoteTimeoutData*>(event.data);
        HandleWaitVoteTimeout(data);
        break;
    }
  }
}

void SlothCore::StopCheckElectionTimeoutTask() {
  if (election_timeout_task_id_ > 0) {
    time_worker_->CancelTask(election_timeout_task_id_, true);
    election_timeout_task_id_ = 0;
  }
}

void SlothCore::StopCheckVoteTimeoutTask() {
  if (vote_timeout_task_id_ > 0) {
    time_worker_->CancelTask(election_timeout_task_id_, true);
    vote_timeout_task_id_ = 0;
  }
}

void SlothCore::HandleVote(VoteData* data) {
  // the vote date has been expired
  if (data->term_snapshot != current_term_) {
    return;
  }
  // only candidate process vote from other node
  if (role_ != kCandidate) {
    return;
  }
  if (data->vote_granted) {
    count.count += 1;
    int32_t major_count = cluster_.size() / 2 +1;
    if (count.count >= major_count) {
      StopCheckVoteTimeoutTask();
      StopCheckElectionTimeoutTask();
      role_ = kLeader;
    }
  }
}

void SlothCore::HandleAppendEntry(AppendEntryData* data) {
  if (current_term_ <= data->request->term()) {
    role_ = kFollower;
    ResetElectionTimeout();
    current_term_ = data->request->term();
    data->response->set_success(true);
  } else {
    data->response->set_success(false);
  }
  data->response->set_term(current_term_);
  data->response->set_status(kRpcOk);
  data->done->Run();
}

void SlothCore::ResetElectionTimeout() {
  StopCheckElectionTimeoutTask();
  StopCheckVoteTimeoutTask();
  uint32_t timeout = GenRandTime();
  election_timeout_task_id_ = time_worker_->DelayTask(boost::bind(&SlothCore::DispatchElectionTimeout, this, current_term_));
}

void SlothCore::ResetVoteTimeout() {
  StopCheckVoteTimeoutTask();
  vote_timeout_task_id_ = time_worker_->DelayTask(boost::bind(&SlothCore::DispatchWaitVoteTimeout, this, current_term_));
}

void SlothCore::DispatchElectionTimeout(uint64_t term) {
  ElectionTimeoutData* data = new ElectionTimeoutData();
  data->term = term;
  SlothEvent e;
  e.data = data;
  e.type = kElectionTimeout;
  while(!queue_->push(e));
}

void SlothCore::DispatchWaitVoteTimeout(uint64_t term) {
  VoteTimeoutData* data = new VoteTimeoutData();
  data->term = term;
  SlothEvent e;
  e.data = data;
  e.type = kVoteTimeout;
  while(!queue_->push(e));
}

void SlothCore::SendAppendEntries(const std::string endpoint) {
  SlothNode_Stub* other_node;
  client_->GetStub(endpoint, &other_node);
  AppendEntriesRequest* request = new AppendEntriesRequest();
  AppendEntriesResponse* response = new AppendEntriesResponse();
  request->set_term(current_term_);
  request->set_leader_id(id_);
  boost::function<void (const AppendEntriesRequest*, AppendEntriesResponse*, bool, int)> callback;
  callback = boost::bind(&SlothNodeImpl::SendAppendEntriesCallback,
                         this, _1, _2, _3, _4);
  client_->AsyncRequest(other_node,
                        &SlothNode_Stub::AppendEntries,
                        request, response,
                        callback,
                        5, 0);
  delete other_node;
}

void SlothCore::SendAppendEntriesCallback(uint64_t term,
           )
uint32_t SlothCore::GenRandTime() {
  uint32_t offset = FLAGS_max_follower_elect_timeout - FLAGS_min_follower_elect_timeout;
  uint32_t timeout = FLAGS_min_follower_elect_timeout + rand() % offset;
  return timeout;
}

void SlothCore::HandleElectionTimeout(ElectionTimeoutData* data) {
  // the election timeout has been expired
  // do nothing
  if (data->term < current_term_) {
    return;
  }
  ++current_term_;
  count.Reset();
  count.term = current_term_;
  // vote for myself
  count.count++;
  std::vector<std::string>::iterator node_it = cluster_.begin();
  int32_t count = -1;
  for (; node_it !=  cluster_.end(); ++node_it) {
    ++count;
    if (id_ == count) {
      continue;
    }
    SendVoteRequest(*node_it);
  }
}

void SlothCore::SendVoteRequest(const std::string& endpoint) {
  SlothNode_Stub* other_node;
  rpc_client_->GetStub(endpoint, &other_node);
  RequestVoteRequest* request = new RequestVoteRequest();
  RequestVoteResponse* response = new RequestVoteResponse();
  request->set_term(count.term);
  request->set_candidate_id(endpoint_);
  boost::function<void (const RequestVoteRequest*, RequestVoteResponse*, bool, int)> callback;
  callback = boost::bind(&SlothCore::SendVoteRequestCallback, this, endpoint, count.term,
                         _1, _2, _3, _4);
  rpc_client_->AsyncRequest(other_node,
                        &SlothNode_Stub::RequestVote,
                        request, response,
                        callback,
                        5, 0);
  delete other_node;
}

void SlothCore::SendVoteRequestCallback(uint64_t term,
                                        const RequestVoteRequest* request,
                                        RequestVoteResponse* response,
                                        bool failed,
                                        int error) {
  if (failed) {
    return;
  }
  VoteData* data = new VoteData();
  data->term_snapshot = term;
  data->term_from_node = response->term();
  data->vote_granted = response->vote_granted();
  delete resquest;
  delete response;
  SlothEvent e;
  e.data = data;
  e.type = kRequestVoteCallback;
  while(!queue_->push(e));
}


void VoteCount::Reset() {
  term = 0;
  count = 0;
}

}

