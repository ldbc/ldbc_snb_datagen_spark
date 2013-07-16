#!/bin/sh

$isql -S $PORT dba dba  <<INPUT
grant execute on DB.DBA.SPARQL_INSERT_DICT_CONTENT to "SPARQL";  
grant execute on DB.DBA.SPARQL_DELETE_DICT_CONTENT to "SPARQL";  
INPUT

status=$?
if [ $status -ne 0 ]
then 
  echo "priviledge grant failed: $status"
  exit $status
fi;

