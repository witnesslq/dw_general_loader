#!/bin/bash

source /home/dwadmin/cdh/env.sh
HADOOP=$HADOOP_PREFIX/bin/hadoop

# delete file
$HADOOP fs -test -e $2
if [ $? -eq 0 ]; then
    $HADOOP fs -rm $2
fi

# create parent folder
$HADOOP fs -mkdir `dirname $2`

# copy
$HADOOP fs -copyFromLocal $1 $2

