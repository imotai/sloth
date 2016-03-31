#include <gflags/gflags.h>

DEFINE_string(node_list, "xxx:9537,xxx:9529,xxx:9530", "the node list of raft cluster");
DEFINE_uint32(node_idx, 0, "the index of node list for node");
DEFINE_uint32(min_follower_elect_timeout, 150, "the min of follower election timeout in ms");
DEFINE_uint32(max_follower_elect_timeout, 150, "the max of follower election timeout in ms");


