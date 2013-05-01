#!/bin/bash

PYTHON="/usr/bin/env python"
HADOOP_CONF=/home/liyinhgqw/hadoop-1.1.0/conf
HADOOP_HOME=/home/liyinhgqw/hadoop-1.1.0

export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | sort | tr '\n' ':')

export PYTHONPATH=hdfs:${PYTHONPATH}

#$PYTHON hdfs/hfilesystem_test.py
#$PYTHON hdfs/hfile_test.py

$PYTHON example.py

