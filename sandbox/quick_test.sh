#!/usr/bin/env sh
hn=`hostname -i`
echo "--ins_servers=$hn:8868,$hn:8869,$hn:8870,$hn:8871,$hn:8872" > dqs.flags
echo "--dqs_port=9527" >> dqs.flags

echo "--dqs_server=127.0.0.1:9527" > client.flags
echo "--perf_thread_count=1" >> client.flags
echo "--perf_value=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" >>client.flags

sh start_all.sh


