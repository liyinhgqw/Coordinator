#! /usr/bin/env bash

curdir=`dirname $0`
curdir=`pwd`/$curdir; `cd $curdir`

./stat.py $1 100
./stat.py $1 300
./stat.py $1 500
./stat.py $1 700
./stat.py $1 900

