#!/bin/sh
# runs SPARQL tpch  tests

. ${0%/*}/config.sh

$bindir/tpchRun.sh -uc tpch/sql -defaultparams -q -printres -mt 1