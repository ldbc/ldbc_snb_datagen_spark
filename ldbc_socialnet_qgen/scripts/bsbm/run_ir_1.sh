#!/bin/sh
# runs SPARQL bsbm IR tests

. ${0%/*}/config.sh

$bindir/bsbmRun.sh -uc bsbm/ir -mt 1
