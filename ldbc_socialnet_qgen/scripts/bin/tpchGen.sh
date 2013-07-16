#!/bin/bash

if [ $# -lt 1 ]; then
  echo "usage: $0 <datadir> <scale> <options>"
  exit 1
fi
datadir=$1
shift
scale=$1
shift

rm -rf $datadir
mkdir -p $datadir
cd $datadir
ln -s $dbgen/dists.dss

log=log
echo "data generation scale=$scale started at `date`" > $log
cmd="$dbgen/dbgen -v -s $scale $@"
echo $cmd
eval $cmd >> $log
echo "data generation finished at `date`" >> $log

rm dists.dss
