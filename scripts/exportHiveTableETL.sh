#!/bin/bash -l

tableName=${1}
filePath=${2}

#export HADOOP_HOME=/home/hadoop/hadoop-1.0.1
hive -e "select * from ${tableName}" > ${filePath}

sed -i 's/NULL/\\\N/g' ${filePath}


