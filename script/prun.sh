#! /usr/bin/env bash

if [ $# -lt 2 ]; then
  echo "Usage: prun COMMAND INTERVAL"
  exit 1
fi

round=0
while true; do
  echo $1
  $1
  sleep $2
done

