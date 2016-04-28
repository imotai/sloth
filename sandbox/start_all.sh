WORKDIR=`pwd`

echo "start sloth"
nohup ../sloth --flagfile=sloth.flag --node_idx=0 --binlogger_db_path=binlog0 >0.log 2>&1 &
nohup ../sloth --flagfile=sloth.flag --node_idx=1 --binlogger_db_path=binlog1 >1.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=2 --binlogger_db_path=binlog2 >2.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=3 --binlogger_db_path=binlog3 >3.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=4 --binlogger_db_path=binlog4 >4.log 2>&1 &

