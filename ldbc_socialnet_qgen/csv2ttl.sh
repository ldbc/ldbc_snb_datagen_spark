#!/bin/sh

. ${0%/*}/classpath.sh

cmd="java -Xmx256M com.openlinksw.util.csv2ttl.Main $@"
echo $cmd
eval $cmd

