#!/bin/sh
# runs SPARQL BSBM explore and update tests

. ${0%/*}/config.sh

#$bindir/grantSparql.sh
$bindir/bsbmRun.sh -udataset $datadir/dataset_update.nt -ucf $bibm/bsbm/exploreAndUpdate/sparql.txt -mt 1 -printres
