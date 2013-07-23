#!/bin/sh

if [ $# -eq 0 ]; then
  echo usage: ${0%/*} command
  exit 1;
fi

bin_name="$@"

ps -a -o time -o args |
while read line
do
  if [[ ${line:9} = ${bin_name}* ]]
  then  
     echo ${line:0:8}
#  else 
#     echo no match: ${line:9}
  fi
done
 