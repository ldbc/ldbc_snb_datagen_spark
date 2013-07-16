#!/bin/sh
# generates bsbm datataset with 1m triples

. ${0%/*}/config.sh

$bindir/bsbmGen.sh $datadir $pc -nof 2