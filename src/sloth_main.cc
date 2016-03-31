#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include "gflags/gflags.h"
#include "sloth_node_impl.h"
#include "logging.h"
#include <sofa/pbrpc/pbrpc.h>
#include <boost/algorithm/string.hpp>

DECLARE_string(node_list);
DECLARE_int32(node_idx);

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
  s_quit = true;
}

int main(int argc, char* args[]) {
  ::google::ParseCommandLineFlags(&argc, &args, true);
  std::vector<std::string> nodes;
  boost::split(nodes, FLAGS_node_list, boost::is_any_of(","));
  if ((size_t)FLAGS_node_idx >= nodes.size()) {
    LOG(WARNING, "fail to init node for node idx is invalid");
    return false;
  }
  std::string node_endpoint = nodes[FLAGS_node_idx];
  ::baidu::common::SetLogLevel(DEBUG);
  sloth::SlothNodeImpl* sloth_node = new sloth::SlothNodeImpl();
  sofa::pbrpc::RpcServerOptions options;
  sofa::pbrpc::RpcServer rpc_server(options);
  if (!rpc_server.RegisterService(sloth_node)) {
    LOG(WARNING, "fail to register sloth_node rpc service");
    exit(1);
  }
  if (!rpc_server.Start(node_endpoint)) {
    LOG(WARNING, "fail to listen on %s", node_endpoint.c_str());
    exit(1);
  }
  sloth_node->Init();
  LOG(INFO, "start sloth on %s", node_endpoint.c_str());
  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  while (!s_quit) {
    sleep(1);
  }
  return 0;
}
