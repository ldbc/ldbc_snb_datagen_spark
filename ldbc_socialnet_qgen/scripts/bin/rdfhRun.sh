#!/bin/sh
# runs tests

if [ $# -lt 1 ]; then
  echo "usage: $0 [options...] -uc <usecaseDir>"
  exit 1
fi

if [ "$scale" == "" ]; then
  echo "scale environment variable must be set"
  exit 1
fi

$bibm/tpchdriver -err-log err.log  -t 200000 http://localhost:$HTTPPORT/sparql -uqp query -scale $scale $@


