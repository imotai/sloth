WORKDIR=`pwd`

echo "start sloth"
nohup ../sloth --flagfile=sloth.flag --node_idx=0 >0.log 2>&1 &
nohup ../sloth --flagfile=sloth.flag --node_idx=1 >1.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=2 >2.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=3 >3.log 2>&1 & 
nohup ../sloth --flagfile=sloth.flag --node_idx=4 >4.log 2>&1 &

