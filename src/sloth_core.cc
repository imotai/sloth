#include "sloth_core.h"

#include <stdlib.h>
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
  vote_timeout_task_id_(0),count(),append_entry_worker_(NULL), running_(false),
  rpc_client_(NULL){
    core_worker_ = new ThreadPool(1);
    time_worker_ = new ThreadPool(2);
    append_entry_worker_ = new ThreadPool(5);
    rpc_client_ = new RpcClient();
}

SlothCore::~SlothCore() {}

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
        HandleElectionTimeout();
        break;
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
  if (election_timeout_task_id_ > 0) {
    time_worker_->CancelTask(election_timeout_task_id_, true);
    election_timeout_task_id_ = 0;
  }
  if (vote_timeout_task_id_ > 0) {
    time_worker_->CancelTask(election_timeout_task_id_, true);
    vote_timeout_task_id_ = 0;
  }
  uint32_t timeout = GenRandTime();
  election_timeout_task_id_ = time_worker_-DelayTask(boost::bind(&SlothCore::DispatchElectionTimeout, this, current_term_));
}

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

void VoteCount::Reset() {
  term = 0;
  count = 0;
}

}

