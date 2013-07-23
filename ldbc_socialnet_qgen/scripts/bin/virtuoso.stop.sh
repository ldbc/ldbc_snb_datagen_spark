#!/bin/sh

cmd="$isql -S $PORT dba dba \"exec=shutdown();\""
echo $cmd
eval $cmd