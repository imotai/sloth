#ifndef SLOTH_CORE_H
#define SLOTH_CORE_H
#include "proto/sloth_node.pb.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

namespace sloth {

// the core logic for raft
class SlothCore {

public:
  SlothCore();
  ~SlothCore();
  void AppendEntries(const AppendEntriesRequest* request,
                     AppendEntriesResponse* response,
                     Closure* done);

private:
  uint64_t current_term_;
  SlothNodeRole role_;
};

}
#endif
