#include "performance/perf_client.h"


#include <signal.h>
#include <unistd.h>
#include <sstream>
#include <string>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

#include <gflags/gflags.h>
static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
  s_quit = true;
}


void StartPerf() {
  dqs::PerfClient test;
  test.Start();
  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  while (!s_quit) {
    sleep(1);
  }
}

int main(int argc, char* args[]) {
  ::google::ParseCommandLineFlags(&argc, &args, true);
  StartPerf();
  return 0;
}
