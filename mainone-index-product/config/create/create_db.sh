#!/bin/sh
echo "indexer server start"
pid=$(ps -awx|grep java|grep IndexProductFromDB|grep -v grep|awk 'print $2' )
kill -9 $pid

JAVA_HOME=/usr/local/jdk1.7.0_09
HOME=/app/search/app
CLASSES=$HOME/porduct_create
COMMONLIB=$HOME/lib/common
LD_LIBRARY_PATH=$HOME/clib

CLASSPATH=$CLASSPATH:$CLASSES
echo $CLASSPATH
for _jar in $COMMONLIB/*.jar
do
  CLASSPATH=$CLASSPATH:$_jar
done
echo $CLASSPATH

LANG=zh_CN.gbk
LC_ALL=zh_CN.gbk

export LANG LC_ALL CLASSES COMMONLIB CLASSPATH LD_LIBRARY_PATH

$JAVA_HOME/bin/java -Xloggc:gc.log -server -Dpid=sis2 -server -Xmx4000m -Xms1024m -Xmn360m -XX:PermSize=128m -XX:MaxPermSize=256m -XX:+UseConcMarkSweepGC cn.b2b.index.product.create.IndexProductFromDB create.conf >> res.log &