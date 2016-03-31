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

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace sloth {

SlothNodeImpl::SlothNodeImpl():mu_(),
  current_term_(0),
  state_(ROLE_STATE_FOLLWER),
  commit_index_(0),
  last_applied_index_(0),
  node_endpoint_(),
  client_(NULL),
  election_timeout_checker_(NULL),
  node_index_(NULL),
  replicate_log_worker_(NULL),
  vote_count_(){
  node_index_ = new std::map<std::string, NodeIndex>();
  election_timeout_checker_ = new ThreadPool(1);
  replicate_log_worker_ = new ThreadPool(1);
  vote_count_.term = current_term_;
  vote_count_.count = 0;
  client_ = new RpcClient();
}

SlothNodeImpl::~SlothNodeImpl() {}

bool SlothNodeImpl::Init() {
  MutexLock lock(&mu_);
  std::vector<std::string> nodes;
  boost::split(nodes, FLAGS_node_list, boost::is_any_of(","));
  if ((size_t)FLAGS_node_idx >= nodes.size()) {
    LOG(WARNING, "fail to init node for node idx is invalid");
    return false;
  }
  node_endpoint_ = nodes[FLAGS_node_idx];
  NodeIndex node_index;
  node_index.match_index = 0;
  node_index.next_index = 1;
  // init follower next_index and commit_index
  for (size_t index = 0; index < nodes.size(); ++index) {
    node_index_->insert(std::make_pair(nodes[index], node_index));
  }
  srand(::baidu::common::timer::get_micros());
  uint32_t timeout = GenerateRandTimeout();
  LOG(DEBUG, "add election timeout handler with timeout %d", timeout);
  election_timeout_task_id_ = election_timeout_checker_->DelayTask(timeout, boost::bind(&SlothNodeImpl::HandleElectionTimeout, this));
  LOG(DEBUG, "init node with endpoint %s successfully", node_endpoint_.c_str());
  return true;
}


void SlothNodeImpl::HandleElectionTimeout() {
  MutexLock lock(&mu_);
  if (state_ == ROLE_STATE_LEADER) {
    // exit election timeout check
    return;
  }
  current_term_++;
  vote_count_.term = current_term_;
  vote_count_.count = 0;
  vote_count_.count++;
  LOG(DEBUG, "change role to candidate, start to vote vote_count.term %ld, node.term %ld",
      vote_count_.term,
      current_term_);
  std::map<std::string, NodeIndex>::iterator it = node_index_->begin();
  for (; it != node_index_->end(); ++it) {
    std::string endpoint = it->first;
    if (endpoint == node_endpoint_) {
      continue;
    }
    SendVoteRequest(endpoint);
  }
  uint32_t timeout = GenerateRandTimeout();
  LOG(DEBUG, "add vote timeout handler with timeout %d", timeout);
  election_timeout_task_id_ = election_timeout_checker_->DelayTask(timeout, boost::bind(&SlothNodeImpl::HandleElectionTimeout, this));
}


void SlothNodeImpl::SendVoteRequest(const std::string& endpoint) {
  mu_.AssertHeld();
  SlothNode_Stub* other_node;
  client_->GetStub(endpoint, &other_node);
  RequestVoteRequest* request = new RequestVoteRequest();
  RequestVoteResponse* response = new RequestVoteResponse();
  request->set_term(vote_count_.term);
  request->set_candidate_id(endpoint);
  boost::function<void (const RequestVoteRequest*, RequestVoteResponse*, bool, int)> callback;
  callback = boost::bind(&SlothNodeImpl::SendVoteRequestCallback, this, _1, _2, _3, _4);
  client_->AsyncRequest(other_node,
                        &SlothNode_Stub::RequestVote,
                        request, response,
                        callback,
                        5, 0);
  LOG(DEBUG, "send vote request to %s", endpoint.c_str());
  delete other_node;
}

void SlothNodeImpl::SendVoteRequestCallback(const RequestVoteRequest* request,
                                            RequestVoteResponse* response,
                                            bool, int) {
  bool do_replication = false;
  {
    MutexLock lock(&mu_); 
    if (state_ == ROLE_STATE_FOLLWER) {
      return;
    }
    if (state_ == ROLE_STATE_LEADER) {
      return;
    }
    if (response->vote_granted()) {
      vote_count_.count++;
      uint32_t major_count = 0;
      if (node_index_->size() % 2) {
        major_count = node_index_->size() / 2 +1;
      }else {
        major_count = node_index_->size() / 2;
      }
      if (vote_count_.count >= major_count) {
        LOG(DEBUG, "I am the leader with term %ld", current_term_);
        election_timeout_checker_->CancelTask(election_timeout_task_id_);
        state_ = ROLE_STATE_LEADER;
        election_timeout_task_id_ = -1;
        do_replication = true;
      }
      LOG(INFO, "vote result major count %d", major_count);
    } else {

    }

  }
  if (do_replication) {
    DoReplicateLog();
  }
  delete request;
  delete response;
}

void SlothNodeImpl::AppendEntries(RpcController* controller,
                                  const AppendEntriesRequest* request,
                                  AppendEntriesResponse* response,
                                  Closure* done) {
  MutexLock lock(&mu_);
  if (request->term() >= current_term_) {
    state_ = ROLE_STATE_FOLLWER;
    LOG(DEBUG, "receive append request from %s", request->leader_id().c_str());
    election_timeout_checker_->CancelTask(election_timeout_task_id_);
    uint32_t timeout = GenerateRandTimeout();
    election_timeout_task_id_ = election_timeout_checker_->DelayTask(timeout, boost::bind(&SlothNodeImpl::HandleElectionTimeout, this));
  }
}

void SlothNodeImpl::DoReplicateLog() {
  MutexLock lock(&mu_);
  if (state_ != ROLE_STATE_LEADER) {
    LOG(DEBUG, "only leader do replicate log");
    return;
  }
  std::map<std::string, NodeIndex>::iterator it = node_index_->begin();
  for (; it != node_index_->end(); ++it) {
    std::string endpoint = it->first;
    if (endpoint == node_endpoint_) {
      continue;
    }
    SendAppendEntries(endpoint);
  }
  replicate_log_worker_->DelayTask(FLAGS_replicate_log_interval,
      boost::bind(&SlothNodeImpl::DoReplicateLog, this));
}

void SlothNodeImpl::SendAppendEntries(const std::string& endpoint) {
  mu_.AssertHeld();
  if (state_ != ROLE_STATE_LEADER) {
    LOG(DEBUG, "only leader send append request");
    return;
  }
  SlothNode_Stub* other_node;
  client_->GetStub(endpoint, &other_node);
  AppendEntriesRequest* request = new AppendEntriesRequest();
  AppendEntriesResponse* response = new AppendEntriesResponse();
  request->set_term(current_term_);
  request->set_leader_id(node_endpoint_);
  boost::function<void (const AppendEntriesRequest*, AppendEntriesResponse*, bool, int)> callback;
  callback = boost::bind(&SlothNodeImpl::SendAppendEntriesCallback,
                         this, _1, _2, _3, _4);
  client_->AsyncRequest(other_node,
                        &SlothNode_Stub::AppendEntries,
                        request, response,
                        callback,
                        5, 0);
  LOG(DEBUG, "append entries request to %s", endpoint.c_str());
  delete other_node;
}

void SlothNodeImpl::SendAppendEntriesCallback(const AppendEntriesRequest* request,
                                 AppendEntriesResponse* response,
                                 bool failed, int error) {
  MutexLock lock(&mu_);
  if (request->term() > current_term_) {
    state_ = ROLE_STATE_FOLLWER;
    uint32_t timeout = GenerateRandTimeout();
    current_term_ = request->term();
    election_timeout_checker_->CancelTask(election_timeout_task_id_);
    election_timeout_task_id_ = election_timeout_checker_->DelayTask(timeout, boost::bind(&SlothNodeImpl::HandleElectionTimeout, this));
  }
  delete request;
  delete response;
}

void SlothNodeImpl::RequestVote(RpcController* controller, 
                                const RequestVoteRequest* request,
                                RequestVoteResponse* response,
                                Closure* done) {
  MutexLock lock(&mu_);
  response->set_status(kRpcOk);
  response->set_term(current_term_);
  if (request->term() <= current_term_) {
    LOG(DEBUG, "candidate %s term %ld is out of date my term is %ld", 
        request->candidate_id().c_str(),
        request->term(),
        current_term_);
    response->set_term(current_term_);
    response->set_vote_granted(false);
    done->Run();
    return;
  }
  LOG(DEBUG, "vote for %s with term %ld, my term %ld", 
        request->candidate_id().c_str(),
        request->term(),
        current_term_);
  response->set_vote_granted(true);
  done->Run();
}


uint32_t SlothNodeImpl::GenerateRandTimeout() {
  uint32_t offset = FLAGS_max_follower_elect_timeout - FLAGS_min_follower_elect_timeout;
  uint32_t timeout = FLAGS_min_follower_elect_timeout + rand() % offset;
  return timeout;
}

}