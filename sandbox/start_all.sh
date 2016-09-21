WORK_DIR=`pwd`

for i in {0..4}
do
   cd $WORK_DIR/app$i
   nohup sh start.sh >/dev/null 2>&1 &
   echo "start app $i successfully"
done
