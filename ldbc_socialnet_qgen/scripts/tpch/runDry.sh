#!/bin/sh
# runs SQL tpch tests in dry-run mode

. ${0%/*}/config.sh

$bindir/tpchRun.sh -uc tpch/sql -mt 1 -defaultparams -q -dry-run -printres