#!/bin/sh

# compose result file

basedir=${0%/*}
. ${basedir}/classpath.sh

if [ $# -le 2 ]; then
  echo "Usage: $0 <res file> <defaultparams.qual> <tpch answers dir>"
  exit 1;
fi

java -Xmx256M com.openlinksw.bibm.tpch.ValidTPCH  $1 $2 $3
