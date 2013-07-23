#!/bin/sh

#usage: $0 -dbdriver <driver> -dburl <dburl> -d <resdir> source...

tpchVdir=${0%/*}

$tpchVdir/../../csvload.sh -schema $tpchVdir/tpch_schema.json -ext tbl "$@"
