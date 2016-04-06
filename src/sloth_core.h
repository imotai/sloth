#ifndef SLOTH_CORE_H
#define SLOTH_CORE_H
#include <boost/lockfree/queue.hpp>
#include "proto/sloth_node.pb.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

namespace sloth {

enum SlothEventType {
  kAppendEntry = 0;
  kElectionTimeout = 1;
  kVoteTimeout = 2;
};

struct SlothEvent {
  void* data;
  SlothEventType type;
};

// the core logic for raft 
// all functions will be processed by one thread and no mutex lock
class SlothCore {

public:
  SlothCore(boost::lockfree::queue<SlothEvent>* queue);
  ~SlothCore();
  void AppendEntries(const AppendEntriesRequest* request,
                     AppendEntriesResponse* response,
                     Closure* done);

private:
  uint64_t current_term_;
  SlothNodeRole role_;
  boost::lockfree::queue<SlothEvent>* queue_;
};

}
#endif
