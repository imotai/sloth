for i in {0..4}
do
   test -d app$i && rm -rf app$0
   cp -rf ../output app$i
   cp logback$i.xml app$i/app/logback.xml
   cp sloth$i.xml app$i/app/sloth.xml
   cp ../start.sh app$i
   mkdir -p app$i/log
done
