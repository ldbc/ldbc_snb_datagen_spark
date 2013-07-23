#!/bin/sh

. ${0%/*}/test_fn.sh
if [ $# -lt 1 ]; then
  echo usage: "$0 <ddir>"
  exit 1
fi
datadir=$1

scalestampfile=scale.loadeds

echo "loading pc= $pc started at `date` ...  " > $scalestampfile

$bindir/grantSparql.sh

RUNSQL $bindir/bsbm_load.sql

EXEC_SCL "DB.DBA.BSBM_LOAD('$datadir');"

echo "loading finished at `date` ">> $scalestampfile
EXEC_SCL "DB.DBA.RDF_OBJ_FT_RULE_ADD (null, null, 'All');"

echo "text indexing finished at `date` ">> $scalestampfile

