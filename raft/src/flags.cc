#include <gflags/gflags.h>

DEFINE_string(node_list, "xxx:9537,xxx:9529,xxx:9530", "the node list of raft cluster");
DEFINE_uint32(node_idx, 0, "the index of node list for node");

