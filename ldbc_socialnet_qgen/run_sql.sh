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

./sibdriver -t 600000 -idir td_data -uc sib/int_sql -mt 1 -runs 15 -sql jdbc:virtuoso://localhost:1207/UID=dba/PWD=dba -dbdriver virtuoso.jdbc4.Driver -printres -w 5