#!/bin/sh

. ${0%/*}/test_fn.sh

if [ $# -lt 1 ]; then
  echo usage: "$0 <data dir>"
  exit 1
fi

ddir=$1
graph=http://example.com/tpcd
scalestampfile=scale.loaded

echo "loading scale=$scale $ddir `ls $ddir/*.gz|wc -l` files started at `date` ..." > $scalestampfile

LOAD_RDFH_GZ $ddir $graph
CHECKPOINT

echo "loading finished at `date`" >> $scalestampfile

