#!/bin/sh

basedir=${0%/*}
. ${basedir}/classpath.sh

# first run${basedir}/tpch/schema.sql

cmd="java -Xmx25G com.openlinksw.util.csvLoader.CsvLoader $@"
echo $cmd
eval $cmd
