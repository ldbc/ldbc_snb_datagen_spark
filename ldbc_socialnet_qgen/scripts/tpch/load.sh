#!/bin/sh

. ${0%/*}/config.sh

$bindir/tpchLoad.sh $datadir
