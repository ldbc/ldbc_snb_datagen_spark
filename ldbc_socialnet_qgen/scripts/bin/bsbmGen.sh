#!/bin/sh
# generates turtle datataset
# args:
# $1 destination directory
# $2 product count (scale factor)
# $3 additional parameters

if [ $# -le 1 ]; then
  echo usage: "$0 <datadir> <pc> <options>?"
  exit 1
fi
datadir=$1
shift
pc=$1
shift
count=count

rm -rf $datadir
mkdir -p $datadir

cd $datadir

echo "data generation started at `date`" > $count
cmd="$bibm/generate -fc -pc $pc -s ttl -fn dataset -dir td_data -pareto $@"
echo $cmd
eval $cmd >> $count
echo "data generation finished at `date`" >> $count

cd ..