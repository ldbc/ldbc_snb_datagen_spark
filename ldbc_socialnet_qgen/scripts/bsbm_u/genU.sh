#!/bin/sh
# generates bsbm update datataset with 1m triples

. ${0%/*}/config.sh

$bindir/bsbmGen.sh $datadir $pc -nof 2 -ud