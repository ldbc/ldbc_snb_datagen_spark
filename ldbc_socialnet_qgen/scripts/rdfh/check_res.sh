#!/bin/sh
# check rdfh/tpch result 

. ${0%/*}/config.sh

if [ $# -lt 1 ]; then
  file=run.qual
else
  file=run.$1.qual
fi

$bindir/validateResult.sh $file
