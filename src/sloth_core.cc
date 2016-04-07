#include "sloth_core.h"

namespace sloth {

SlothCore::SlothCore(boost::lockfree::queue<SlothEvent>* queue):current_term_(1),
  role_(kFollower),queue_(queue),core_worker_(NULL),
  time_worker_(NULL),election_timeout_task_id_(0),
  vote_timeout_task_id_(0),append_entry_worker_(NULL), running_(false) {
    core_worker_ = new ThreadPool(1);
    time_worker_ = new ThreadPool(1);
    append_entry_worker_ = new ThreadPool(5);
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

}

void SlothCore::HandleElectionTimeout() {
  ++current_term_;
}

}

