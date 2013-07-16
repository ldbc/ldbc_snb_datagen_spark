#!/bin/sh
# runs SPARQL tpch performance test

. ${0%/*}/config.sh

$bindir/rdfhRun.sh -uc tpch/sparql -mt 16