#include <gflags/gflags.h>

DEFINE_string(node_list, "127.0.0.1:9537,127.0.0.1:9529,127.0.0.1:9530", "the node list of raft cluster");
DEFINE_int32(node_idx, 0, "the index of node list for node");
DEFINE_int32(min_follower_elect_timeout, 150, "the min of follower election timeout in ms");
DEFINE_int32(max_follower_elect_timeout, 300, "the max of follower election timeout in ms");
DEFINE_int32(replicate_log_interval, 50, "the interval of leader replicate log");
DEFINE_int32(wait_vote_back_timeout, 500, "the time that wati vote come back");

