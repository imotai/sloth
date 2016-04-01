ps -ef | grep sloth | grep -v grep | awk '{print $2}' | while read line; do kill -9 $line;done
