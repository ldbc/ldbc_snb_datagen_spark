#!/bin/sh
# validates TPCH result

if [ $# -lt 1 ]; then
  echo usage: "$0 <qualification file to check>"
  exit 1
fi

cmd="$bibm/compareresults.sh $bibm/tpch/valid.qual ${1}"
echo $cmd
eval $cmd