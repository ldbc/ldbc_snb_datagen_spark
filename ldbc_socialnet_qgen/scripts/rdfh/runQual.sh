#!/bin/sh
# runs SPARQL tpch qualification test

. ${0%/*}/config.sh

$bindir/rdfhRun.sh -uc tpch/sparql -mt 4 -defaultparams -q -printres