#!/bin/sh

dataset=rdfh_0

rm -f $dataset/*.ttl

../../csv2ttl.sh -ext tbl -schema smoke_schema.json -d $dataset tpch_0