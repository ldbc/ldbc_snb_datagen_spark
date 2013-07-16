#!/bin/sh
# runs SQL tpch tests

if [ $# -lt 1 ]; then
  echo "usage: $0 [options...] -uc <usecaseDir>"
  exit 1
fi

if [ "$port" == "" ]; then
  if [ "$PORT" == "" ]; then
    echo "PORT environment variable is not set:$PORT"
    exit 1
  fi
   let iport=$PORT
fi
if [ "$scale" == "" ]; then
  echo "scale environment variable is not set"
  exit 1
fi

cmd="$bibm/tpchdriver -sql -err-log err.log -dbdriver virtuoso.jdbc4.Driver jdbc:virtuoso://localhost:$iport/UID=dba/PWD=dba \
-t 300000 -scale $scale  $@"
echo $cmd
eval $cmd