#include "dqs_impl.h"

#include <signal.h>
#include <unistd.h>
#include <sstream>
#include <string>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>
#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include "logging.h"

DECLARE_string(dqs_port);

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
  s_quit = true;
}

void StartDqs() {

  sofa::pbrpc::RpcServerOptions options;
  sofa::pbrpc::RpcServer rpc_server(options);
  dqs::DqsImpl* dqs_impl = new dqs::DqsImpl();
  if (!rpc_server.RegisterService(dqs_impl)) {
    LOG(WARNING, "fail to register dqs rpc service");
    exit(1);
  }
  std::string endpoint = "0.0.0.0:" + FLAGS_dqs_port;
  if (!rpc_server.Start(endpoint)) {
    LOG(WARNING, "fail to listen port %s", FLAGS_dqs_port.c_str());
    exit(1);
  }
  LOG(INFO, "start dqs on port %s", FLAGS_dqs_port.c_str());
  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  while (!s_quit) {
    sleep(1);
  }
}

int main(int argc, char* args[]) {
  ::google::ParseCommandLineFlags(&argc, &args, true);
  StartDqs();
  return 0;
}
