#include "sloth_core.h"

#include <boost/algorithm/string.hpp>
#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"

DECLARE_string(node_list);
DECLARE_int32(node_idx);
DECLARE_int32(max_follower_elect_timeout);
DECLARE_int32(min_follower_elect_timeout);
DECLARE_int32(replicate_log_interval);
DECLARE_int32(wait_vote_back_timeout);

using ::baidu::common::DEBUG;

namespace sloth {

SlothCore::SlothCore(boost::lockfree::queue<SlothEvent>* queue):current_term_(1),
  role_(kFollower),queue_(queue),core_worker_(NULL),
  time_worker_(NULL),election_timeout_task_id_(0),
  vote_timeout_task_id_(0),count_(),append_entry_worker_(NULL),
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
  LOG(DEBUG, "init sloth core with id %d and endpoint %s",
      id_,
      cluster_[id_].c_str());
  srand(::baidu::common::timer::get_micros());
  return true;
}

void SlothCore::Start() {
  if (running_) {
    return;
  }
  LOG(DEBUG, "start sloth core");
  running_ = true;
  role_ = kFollower;
  core_worker_->AddTask(boost::bind(&SlothCore::Run, this));
  ResetElectionTimeout();
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
        {
          AppendEntryData* data = reinterpret_cast<AppendEntryData*>(event.data);
          HandleAppendEntry(data);
          break;
        }
      case kElectionTimeout:
        {
          ElectionTimeoutData* data = reinterpret_cast<ElectionTimeoutData*>(event.data);
          HandleElectionTimeout(data);
          break;
        }
      case kRequestVoteCallback:
        {
          VoteData* data = reinterpret_cast<VoteData*>(event.data);
          HandleVote(data);
          break;
        }
      case kVoteTimeout:
        {
          VoteTimeoutData* data = reinterpret_cast<VoteTimeoutData*>(event.data);
          HandleWaitVoteTimeout(data);
          break;
        }
      case kSendAppendEntryCallback:
        {
          SendAppendEntriesCallbackData* data = reinterpret_cast<SendAppendEntriesCallbackData*>(event.data);
          HandleSendEntriesCallback(data);
          break;
        }
      case kRequestVote:
        {
          RequestVoteData* data = reinterpret_cast<RequestVoteData*>(event.data);
          HandleRequestVote(data);
        }
    }
  }
}

void SlothCore::HandleRequestVote(RequestVoteData* data) {
  if (data->request->term() <= current_term_) {
    data->response->set_vote_granted(false);
  }else {
    data->response->set_vote_granted(true);
  }
  data->response->set_term(current_term_);
  data->response->set_status(kRpcOk);
  data->done->Run();
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

void SlothCore::HandleWaitVoteTimeout(VoteTimeoutData* data) {
}

void SlothCore::HandleSendEntriesCallback(SendAppendEntriesCallbackData* data) {
  if (data->term_snapshot != current_term_) {
    return;
  }
  // 
  if (data->term_from_node > current_term_) {
    role_ = kFollower;
    ResetElectionTimeout();
    current_term_ = data->term_from_node;
  }
  delete data;
}

void SlothCore::HandleVote(VoteData* data) {
  // the vote date has been expired
  if (data->term_snapshot != current_term_) {
    delete data;
    return;
  }
  // only candidate process vote from other node
  if (role_ != kCandidate) {
    delete data;
    return;
  }
  if (data->vote_granted) {
    ++count_.count;
    int32_t major_count = cluster_.size() / 2 +1;
    if (count_.count >= major_count) {
      StopCheckVoteTimeoutTask();
      StopCheckElectionTimeoutTask();
      role_ = kLeader;
      DispatchReplicateLog(current_term_);
      LOG(DEBUG, "I am the leader with term %ld", current_term_);
    }
    LOG(DEBUG, "vote come back count %d granted %d", count_.count, data->vote_granted);
  }
  delete data;
}

void SlothCore::DispatchReplicateLog(uint64_t term) {
  if (term != current_term_
      || role_ != kLeader) {
    return;
  }
  int32_t index = -1;
  std::vector<std::string>::iterator node_it = cluster_.begin();
  for (; node_it != cluster_.end(); ++node_it) {
    ++index;
    if (index == id_) {
      continue;
    }
    std::string node = *node_it;
    SendAppendEntries(node);
  }
  append_entry_task_id_ = append_entry_worker_->DelayTask(FLAGS_replicate_log_interval,
                          boost::bind(&SlothCore::DispatchReplicateLog, this, term));
}

void SlothCore::StopReplicateLog() {
  if (append_entry_task_id_ > 0) {
    append_entry_worker_->CancelTask(append_entry_task_id_, true);
  }
}

void SlothCore::HandleAppendEntry(AppendEntryData* data) {
  if (current_term_ <= data->request->term()) {
    if (role_ == kCandidate) {
      StopCheckVoteTimeoutTask();
    } else if (role_ == kLeader) {
      StopReplicateLog();
    }
    role_ = kFollower;
    ResetElectionTimeout();
    current_term_ = data->request->term();
    leader_id_ = data->request->leader_index();
    data->response->set_success(true);
  } else {
    data->response->set_success(false);
  }
  data->response->set_term(current_term_);
  data->response->set_status(kRpcOk);
  data->done->Run();
  delete data;
}

void SlothCore::ResetElectionTimeout() {
  StopCheckElectionTimeoutTask();
  StopCheckVoteTimeoutTask();
  uint32_t timeout = GenRandTime();
  election_timeout_task_id_ = time_worker_->DelayTask(timeout, boost::bind(&SlothCore::DispatchElectionTimeout, this, current_term_));
  LOG(DEBUG, "reset election timeout %d with task id %ld",
      timeout,
      election_timeout_task_id_);
}

void SlothCore::ResetWaitVoteTimeout() {
  StopCheckVoteTimeoutTask();
  vote_timeout_task_id_ = time_worker_->DelayTask(FLAGS_wait_vote_back_timeout, boost::bind(&SlothCore::DispatchWaitVoteTimeout, this, current_term_));
  LOG(DEBUG, "reset wait vote timeout with task id %ld",
      vote_timeout_task_id_);
}

void SlothCore::DispatchElectionTimeout(uint64_t term) {
  LOG(DEBUG, "dispatch election timeout event with term %ld", term);
  ElectionTimeoutData* data = new ElectionTimeoutData();
  data->term = term;
  SlothEvent e;
  e.data = data;
  e.type = kElectionTimeout;
  while(!queue_->push(e));
}

void SlothCore::DispatchWaitVoteTimeout(uint64_t term) {
  LOG(DEBUG, "dispatch wait vote timeout event with term %ld", term);
  VoteTimeoutData* data = new VoteTimeoutData();
  data->term = term;
  SlothEvent e;
  e.data = data;
  e.type = kVoteTimeout;
  while(!queue_->push(e));
}

void SlothCore::SendAppendEntries(const std::string endpoint) {
  SlothNode_Stub* other_node;
  rpc_client_->GetStub(endpoint, &other_node);
  AppendEntriesRequest* request = new AppendEntriesRequest();
  AppendEntriesResponse* response = new AppendEntriesResponse();
  request->set_term(current_term_);
  request->set_leader_index(id_);
  boost::function<void (const AppendEntriesRequest*, AppendEntriesResponse*, bool, int)> callback;
  callback = boost::bind(&SlothCore::SendAppendEntriesCallback,
                         this, current_term_, _1, _2, _3, _4);
  rpc_client_->AsyncRequest(other_node,
                        &SlothNode_Stub::AppendEntries,
                        request, response,
                        callback,
                        5, 0);
  delete other_node;
}

void SlothCore::SendAppendEntriesCallback(uint64_t term,
                                         const AppendEntriesRequest* request,
                                         AppendEntriesResponse* response,
                                         bool failed,
                                         int error) {
  SendAppendEntriesCallbackData* data = new SendAppendEntriesCallbackData();
  data->term_from_node = response->term();
  data->success = response->success();
  data->term_snapshot = term;
  SlothEvent e;
  e.data = data;
  e.type = kSendAppendEntryCallback;
  while (!queue_->push(e));
}

uint32_t SlothCore::GenRandTime() {
  uint32_t offset = FLAGS_max_follower_elect_timeout - FLAGS_min_follower_elect_timeout;
  uint32_t timeout = FLAGS_min_follower_elect_timeout + rand() % offset;
  return timeout;
}

void SlothCore::HandleElectionTimeout(ElectionTimeoutData* data) {
  LOG(DEBUG, "election timeout with term %d current_term %d", 
      data->term,
      current_term_);
  // the election timeout has been expired
  // do nothing
  if (data->term < current_term_) {
    delete data;
    return;
  }
  if (role_ == kLeader) {
    delete data;
    return;
  }
  role_ = kCandidate;
  ++current_term_;
  count_.Reset();
  count_.term = current_term_;
  // vote for myself
  count_.count++;
  std::vector<std::string>::iterator node_it = cluster_.begin();
  int32_t count = -1;
  for (; node_it !=  cluster_.end(); ++node_it) {
    ++count;
    if (id_ == count) {
      continue;
    }
    std::string node = *node_it;
    LOG(DEBUG, "request vote to %s", node.c_str());
    SendVoteRequest(node);
  }
  delete data;
  // add wait vote timeout task
  ResetWaitVoteTimeout();
}

void SlothCore::SendVoteRequest(const std::string& endpoint) {
  SlothNode_Stub* other_node;
  rpc_client_->GetStub(endpoint, &other_node);
  RequestVoteRequest* request = new RequestVoteRequest();
  RequestVoteResponse* response = new RequestVoteResponse();
  request->set_term(count_.term);
  request->set_candidate_id(id_);
  boost::function<void (const RequestVoteRequest*, RequestVoteResponse*, bool, int)> callback;
  callback = boost::bind(&SlothCore::SendVoteRequestCallback, this, count_.term,
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
  delete request;
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

void SlothCore::GetStatus(GetClusterStatusResponse* response) {
  response->set_node_role(SlothNodeRole_Name(role_));
  response->set_node_endpoint(cluster_[id_]);
  if (leader_id_ >= 0) {
    response->set_leader_endpoint(cluster_[leader_id_]);
  }
  response->set_current_term(current_term_);
  response->set_node_idx(id_);
}

}

