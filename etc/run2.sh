#!/bin/sh
if test -s ~/.bashrc
then
source ~/.bashrc
fi
java -Xmx10000m  -classpath "/fs/clip-trec/umd-hadoop-core/lib/google-collect-1.0-rc2.jar:/fs/clip-trec/umd-hadoop-core/lib/commons-logging-1.1.jar:/fs/clip-trec/umd-hadoop-core/hadoop/hadoop-0.20.1/lib/log4j-1.2.15.jar:/fs/clip-trec/umd-hadoop-core/cloud9.jar:/fs/clip-trec/umd-hadoop-core/hadoop/hadoop-0.20.1/hadoop-0.20.1-core.jar:/fs/clip-trec/trunk_new/ivory.jar" $1 $2 $3 $4 $5 $6 $7 $8 $9
