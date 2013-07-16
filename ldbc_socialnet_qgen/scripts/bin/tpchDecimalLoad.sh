#!/bin/sh

. ${0%/*}/test_fn.sh

if [ $# -lt 1 ]; then
  echo usage: "$0 <data dir>"
  exit 1
fi

schemaDir=$bibm/tpch/virtuosoDecimal
scalestampfile=scale.loaded
echo "loading scale=$scale started at `date` ..." > $scalestampfile

RUNSQL ${schemaDir}/schema.sql

$schemaDir/tblLoad.sh -dbdriver virtuoso.jdbc4.Driver -dburl jdbc:virtuoso://localhost:$PORT/UID=dba/PWD=dba $@

CHECKPOINT

echo "loading finished at `date`" >> $scalestampfile
