#!/bin/sh
# generates update datataset with tbl files

. ${0%/*}/config.sh

$bindir/tpchGen.sh ${datadir}_u $scale -U 10