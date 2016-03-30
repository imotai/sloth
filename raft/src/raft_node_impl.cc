#include "raft_node_impl.h"

#include <vector>
#include <boost/algorithm/string.hpp>
#include "gflags/gflags.h"
#include "logging.h"

DECLARE_string(node_list);
DECLARE_uint32(node_idx);

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace raft {

RaftNodeImpl::RaftNodeImpl():current_term_(0),
  state_(RAFT_STATE_FOLLWER),
  commit_index_(0),
  last_applied_index_(0),
  node_endpoint_(),
  node_index_(NULL){
  node_index_ = new std::map<std::string, NodeIndex>();
}

RaftNodeImpl::~RaftNodeImpl() {}

bool RaftNodeImpl::Init() {
  std::vector<std::string> nodes;
  boost::split(nodes, FLAGS_node_list, boost::is_any_of(","));
  if (FLAGS_node_idx >= nodes.size()) {
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
  LOG(INFO, "init node with endpoint %s successfully", node_endpoint_.c_str());
  return true;
}

}
