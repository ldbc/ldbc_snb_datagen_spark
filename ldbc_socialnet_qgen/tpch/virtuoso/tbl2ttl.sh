#!/bin/sh

tpchVdir=${0%/*}

$tpchVdir/../../csv2ttl.sh -schema $tpchVdir/rdfh_schema.json -ext tbl "$@"
