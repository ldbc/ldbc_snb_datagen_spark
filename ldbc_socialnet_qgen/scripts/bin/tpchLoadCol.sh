#!/bin/sh

. ${0%/*}/test_fn.sh

if [ $# -lt 1 ]; then
  echo usage: "$0 <data dir>"
  exit 1
fi

tpch=$bibm/tpch
scalestampfile=scale.loaded
echo "loading column scale=$scale started at `date` ..." > $scalestampfile

RUNSQL ${tpch}/virtuoso/schema_col.sql
$tpch/virtuoso/tblLoad.sh -dbdriver virtuoso.jdbc4.Driver -dburl jdbc:virtuoso://localhost:$PORT/UID=dba/PWD=dba $@
CHECKPOINT

echo "loading finished at `date`" >> $scalestampfile
