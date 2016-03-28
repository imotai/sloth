WORKDIR=`pwd`

echo "start nexus"
# mv ins to ~
cp ../thirdsrc/ins/ins ~
cd ~
host_name=`hostname -i`
echo "--cluster_members=$host_name:8868,$host_name:8869,$host_name:8870,$host_name:8871,$host_name:8872" >ins.flag
nohup ./ins --flagfile=ins.flag --server_id=1 >1.log 2>&1 &
nohup ./ins --flagfile=ins.flag --server_id=2 >2.log 2>&1 & 
nohup ./ins --flagfile=ins.flag --server_id=3 >3.log 2>&1 & 
nohup ./ins --flagfile=ins.flag --server_id=4 >4.log 2>&1 & 
nohup ./ins --flagfile=ins.flag --server_id=5 >5.log 2>&1 &
cd $WORKDIR
sleep 5

nohup ../dqs --flagfile=dqs.flags >> dqs.log 2>&1 &

sleep 1

nohup ../perf --flagfile=client.flags >> perf.log 2>&1 &


