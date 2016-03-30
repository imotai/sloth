#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include <map>
#include "proto/raft_node.pb.h"
#include "raft_types.h"

namespace raft {

struct NodeIndex {
  volatile uint64_t next_index;
  volatile uint64_t match_index;
  NodeIndex():next_index(0), match_index(0){}
};

class RaftNodeImpl : public RaftNode {

public:
  RaftNodeImpl();
  ~RaftNodeImpl();

  // init raft node , this function is not thread safe
  bool Init();
private:
  // the term of raft
  volatile uint64_t current_term_;
  // the state of node
  volatile uint32_t state_;
  // the commit index
  volatile uint64_t commit_index_;
  // the last applied index 
  volatile uint64_t last_applied_index_;
  std::string node_endpoint_;
  // for leaders
  std::map<std::string, NodeIndex>* node_index_;

};

}
#endif
