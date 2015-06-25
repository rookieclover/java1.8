#!/bin/sh
echo "control server start"
HOME=/app/search/app
CLASSES=$HOME/controller
COMMONLIB=$HOME/lib/common
LD_LIBRARY_PATH=$HOME/clib

$CLASSPATH=CLASSES
echo $CLASSPATH
for _jar in $COMMONLIB/*.jar
do
  CLASSPATH=$CLASSPATH:$_jar
done
echo $CLASSPATH

LANG=zh_CN.gbk
LC_ALL=zh_CN.gbk


export LANG LC_ALL CLASSES COMMONLIB CLASSPATH LD_LIBRARY_PATH

java  -Xloggc:gc.log -server -Dpid=sis2 -server -Xmx2000m -Xms1024m -Xmn360m -XX:PermSize=128m -XX:MaxPermSize=256m -XX:+UseConcMarkSweepGC cn.b2b.control.search.controller.StartControl 8000 control.properties  &
