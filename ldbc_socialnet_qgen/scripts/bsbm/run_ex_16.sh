#!/bin/sh
# runs SPARQL bsbm IR tests

. ${0%/*}/config.sh

$bindir/bsbmRun.sh -uc bsbm/explore -mt 16
