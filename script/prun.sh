#! /usr/bin/env bash

script=`dirname '$0'`
HOME=`cd script/..; pwd`

export PYTHONPATH=$PYTHONPATH:"$HOME/src"

if [ $# -lt 2 ]; then
  echo "Usage: prun COMMAND INTERVAL"
  exit 1
fi

round=0
while true; do
  python $1
  sleep $2
done

