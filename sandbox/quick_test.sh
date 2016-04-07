#!/usr/bin/env sh
hn=`hostname -i`
echo "/tmp/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern
ulimit -c unlimited

echo "--node_list=$hn:8868,$hn:8869,$hn:8870,$hn:8871,$hn:8872" > sloth.flag
echo "--replicate_log_interval=30" >> sloth.flag
sh start_all.sh


