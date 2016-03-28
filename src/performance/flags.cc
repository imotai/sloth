#include <gflags/gflags.h>

DEFINE_string(dqs_server, "127.0.0.1:9527", "the server addr of dqs");
DEFINE_int32(perf_thread_count, 1, "the count of perf thread");
DEFINE_string(perf_value, "xxxxxxxxxxxxxxx", "the value of perf");
