#!/bin/sh
# runs tpch SQL tests

. ${0%/*}/config.sh

$bindir/tpchRun.sh -uc tpch/sql -mt 16