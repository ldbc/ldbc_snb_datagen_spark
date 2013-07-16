#!/bin/bash
# runs TPCH refresh function 2 (delete)
# expects environment variable PORT to be set

. ${0%/*}/classpath.sh
if [ $# -lt 2 ]; then
  echo "Usage: $0 <update data directory>  <file number 1..N>"
  exit 1;
fi
udd=$1
fileNumber=$2

cmd="java -Xmx256M com.openlinksw.bibm.tpchRefresh.Refresh2 -dbdriver virtuoso.jdbc4.Driver jdbc:virtuoso://localhost:$PORT/UID=dba/PWD=dba \
 -udd $udd -t 18000 -ds $fileNumber"
echo $cmd
eval $cmd
