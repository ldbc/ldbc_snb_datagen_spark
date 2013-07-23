#!/bin/bash
# runs TPCH refresh function 1 (insert)
# expects environment variable PORT to be set

basedir=${0%/*}
. ${basedir}/classpath.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <update data directory>  <stream number 0..N>"
  exit 1;
fi
udd=$1
fileNumber=$2

cmd="java -Xmx256M com.openlinksw.bibm.tpchRefresh.Refresh1 -dbdriver virtuoso.jdbc4.Driver jdbc:virtuoso://localhost:$PORT/UID=dba/PWD=dba \
 -schema $basedir/tpch/virtuoso/tpch_schema.json -udd $udd -t 18000 -ds $fileNumber"
echo $cmd
eval $cmd
