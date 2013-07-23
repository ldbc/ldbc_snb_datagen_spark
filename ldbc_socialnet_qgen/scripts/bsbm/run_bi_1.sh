#!/bin/sh
# runs SPARQL bsbm IR tests

. ${0%/*}/config.sh

$bindir/bsbmRun.sh -uc bsbm/bi -mt 1
