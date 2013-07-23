#!/bin/sh
# runs SPARQL bsbm tests in dry-run mode

. ${0%/*}/config.sh

$bindir/bsbmRun.sh -uc bsbm/explore -q -dry-run -printres