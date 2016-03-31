killall sloth 
ps -ef | grep dqs | grep -v grep | awk '{print $2}' | while read line; do kill -9 $line;done
ps -ef | grep perf | grep -v grep | awk '{print $2}' | while read line; do kill -9 $line;done
