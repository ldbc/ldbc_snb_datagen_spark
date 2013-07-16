#!/bin/sh

. ${0%/*}/classpath.sh

if [ $# -le 1 ]; then
  echo "Usage: $0 <base.qual> <checked.qual>"
  exit 1;
fi

cmd="java -Xmx256M com.openlinksw.bibm.qualification.Comparator $1 $2"
echo $cmd
eval $cmd
