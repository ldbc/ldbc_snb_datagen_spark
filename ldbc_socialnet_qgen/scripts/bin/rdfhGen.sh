#!/bin/sh
if [ $# -lt 2 ]; then
  echo "usage: $0 <tpchdatadir> <rdfdatadir> "
  exit 1
fi

tpchdata=$1
resdir=$2
log=$resdir/log

rm -rf $resdir
mkdir $resdir

echo "data convertion started at `date`" > $log
cmd="$bibm/tpch/virtuoso/tbl2ttl.sh -d $resdir -gz $tpchdata"
echo $cmd
eval $cmd >> $log
echo "data convertion finished at `date`" >> $log