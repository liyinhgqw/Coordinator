#! /usr/bin/env bash

# PYTHON
export PYTHON="/usr/bin/env python"

# Hadoop
if [ "$HADOOP_HOME" = "" ]; then
HADOOP_HOME=/home/liyinhgqw/hadoop-1.1.0
fi
HADOOP_CONF=$HADOOP_HOME/conf
 

# Coord, python-hdfs
export LD_LIBRARY_PATH=${HADOOP_HOME}/c++/Linux-amd64-64/lib:${JAVA_HOME}/jre/lib/amd64/server
export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar |
sort | tr '\n' ':')
 
export COORD_HOME=`dirname $0`
export COORD_HOME=`cd $COORD_HOME; pwd`
export PYHDFS_HOME=$COORD_HOME/src/python-hdfs/hdfs
export PYTHONPATH=${COORD_HOME}/src:$PYHDFS_HOME


bash


