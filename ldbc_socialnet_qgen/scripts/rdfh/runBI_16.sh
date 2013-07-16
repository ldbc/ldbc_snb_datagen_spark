#!/bin/sh
# runs SPARQL tpch business intelligence test

. ${0%/*}/config.sh

$bindir/rdfhRun.sh -uc tpch/sparqlbi -mt 16