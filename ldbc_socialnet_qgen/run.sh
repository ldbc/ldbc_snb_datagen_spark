#!/bin/bash

basedir=${0%/*}
libdir=${basedir}/lib
SEP=':'

if [ -z "$CLASSPATH" ]
then
  CLASSPATH=${basedir}/bin  # for development version only
else
  CLASSPATH=$CLASSPATH$SEP${basedir}/bin  # for development version only
fi    

if [ "$(uname)" = "Cygwin" ]; then SEP=';'; fi
for jar in $libdir/*.jar
do
  if [ ! -e "$jar" ]; then continue; fi
  CLASSPATH="$CLASSPATH$SEP$jar"
done

export CLASSPATH

./sibdriver -t 300000 -idir td_data -uqp query -uc sib/int -mt 1 -runs 1 http://localhost:8607/sparql -printres -q -defaultparams