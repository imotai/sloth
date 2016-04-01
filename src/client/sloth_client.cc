#include "gflags/gflags.h"
#include "rpc/rpc_client.h"
#include "string_util.h"
#include <boost/algorithm/string.hpp>
#include "tprinter.h"
#include "proto/sloth_node.pb.h"

DEFINE_string(node_list, "127.0.0.1:8868,127.0.0.1:8867,127.0.0.1:8868,127.0.0.1:8869,127.0.0.1:8870", "the node list of raft cluster");


void GetClusterStatus() {
  sloth::RpcClient* rpc_client = new sloth::RpcClient();
  std::vector<std::string> nodes;
  boost::split(nodes, FLAGS_node_list, boost::is_any_of(","));
  std::vector<sloth::GetClusterStatusResponse> status;
  std::vector<std::string>::iterator it = nodes.begin();
  for (; it != nodes.end(); ++it) {
    sloth::SlothNode_Stub* sloth_node = NULL;
    rpc_client->GetStub(*it, &sloth_node);
    sloth::GetClusterStatusRequest request;
    sloth::GetClusterStatusResponse response;
    bool ok = rpc_client->SendRequest(sloth_node, &sloth::SlothNode_Stub::GetClusterStatus,
                                     &request, &response, 5, 1);
    if (!ok) {
      response.set_node_idx(-1);
      response.set_node_endpoint(*it);
      response.set_node_role("dead");
      response.set_current_term(0);
    }
    status.push_back(response);
  }
  ::baidu::common::TPrinter tp(5);
  tp.AddRow(5, "id", "endpoint", "role","leader", "term");
  for (size_t index = 0 ; index < status.size(); ++index) {
    std::vector<std::string> vs;
    vs.push_back(baidu::common::NumToString(status[index].node_idx()));
    vs.push_back(status[index].node_endpoint());
    vs.push_back(status[index].node_role());
    vs.push_back(status[index].leader_endpoint());
    int64_t term = status[index].current_term();
    vs.push_back(baidu::common::NumToString(term));
    tp.AddRow(vs);
  }
  printf("%s\n", tp.ToString().c_str());
}


int main(int argc, char* argv[]) {
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  GetClusterStatus();
  return 0;
}
