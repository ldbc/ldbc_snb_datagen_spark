#!/bin/sh
# runs SPARQL BSBM tests

idir=$datadir/td_data

cmd="$bibm/bsbmdriver -t 300000 http://localhost:$HTTPPORT/sparql -idir $idir -uqp query $@"
echo $cmd
eval $cmd