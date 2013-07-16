#!/bin/sh
# runs SPARQL bsbm tests in dry-run mode

. ${0%/*}/config.sh

$bindir/bsbmRun.sh -udataset $datadir/dataset_update.nt -ucf $bibm/bsbm/exploreAndUpdate/sparql.txt -q -dry-run -printres