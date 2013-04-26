#! /usr/bin/env bash

RAND_MAX=32768


erand() {
  avg=$1
  r=$RAND_MAX
  while [ $r -ge $RAND_MAX ]; do
    r=$RANDOM
    p=`echo $r / $RAND_MAX | bc -l`
  done
  x=`echo "-1.0 * $avg * l(1-$p)" | bc -l`
  echo "$x"             # use echo to return a value
}

script=`dirname '$0'`
HOME=`cd $script/..; pwd`

export PYTHONPATH=$PYTHONPATH:"$HOME/src"

if [ $# -lt 2 ]; then
  echo "Usage: prun COMMAND INTERVAL"
  exit 1
fi

round=0
while true; do
  python $1
  er=`erand $2`
  sleep $er
done

