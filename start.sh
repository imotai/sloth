JVM_HEAP_MAX=4096
LOCAL_HEAD_NUMBER=`free -m | grep Mem | awk '{print $2}'`
if [ $LOCAL_HEAD_NUMBER -lt $JVM_HEAP_MAX ]
then
  JVM_HEAP_MAX=`expr $LOCAL_HEAD_NUMBER - 100`
fi
LOG_HOME=log
echo "heap max is $JVM_HEAP_MAX m"
JVM_LOG=''
JVM_LOG=$JVM_LOG" -Xloggc:$LOG_HOME/sloth.gc.log.$(date +%Y%m%d%H%M)"
JVM_LOG=$JVM_LOG" -XX:ErrorFile=$LOG_HOME/sloth.vmerr.log.$(date +%Y%m%d%H%M)"
JVM_LOG=$JVM_LOG" -XX:HeapDumpPath=$LOG_HOME/sloth.heaperr.log.$(date +%Y%m%d%H%M)"
echo $JVM_LOG
JVM_OPT="-server -Xmx${JVM_HEAP_MAX}m -Xms${JVM_HEAP_MAX}m -XX:SurvivorRatio=8 -XX:NewRatio=2 -XX:PermSize=128m -XX:MaxPermSize=256m -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintCommandLineFlags -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelCMSThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=1 -XX:CMSInitiatingOccupancyFraction=72"
echo $JVM_OPT
DEP_LIBS=""
for name in `ls lib/*.jar`
do
  DEP_LIBS=$DEP_LIBS:$name
done

if [ -f /usr/local/java7/jre/bin/java ]; then
  JAVA_BIN=/usr/local/java7/jre/bin/java
else
  JAVA_BIN=java
fi
$JAVA_BIN -version
$JAVA_BIN $JVM_LOG $JVM_OPT -Dfile.encoding=UTF-8  -cp app:$DEP_LIBS io.microstack.sloth.Bootstrap
