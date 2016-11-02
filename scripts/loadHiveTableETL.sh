#!/bin/bash

tableName=${1}
filePath=${2}

#export HADOOP_HOME=/home/hadoop/hadoop-1.0.1
/usr/bin/hive -e "load data local inpath '${filePath}' overwrite into table ${tableName};"
