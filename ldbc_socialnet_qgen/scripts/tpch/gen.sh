#!/bin/sh
# generates datataset with tbl files

. ${0%/*}/config.sh

$bindir/tpchGen.sh $datadir $scale -fF -C 8