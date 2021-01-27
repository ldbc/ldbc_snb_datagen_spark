#!/bin/bash

[ ! -f params.ini ] && echo "params.ini does not exist, exiting" && exit 1
[ ! -f target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar ] && echo "target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar does not exist, exiting" && exit 1

# make sure that out directory exists and clean previously generated data
mkdir -p out/
rm -rf out/*
docker run \
  --env uid=`id -u` \
  --volume `pwd`/out:/mnt/data \
  --volume `pwd`/params.ini:/mnt/params.ini \
  --volume `pwd`/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar:/mnt/datagen.jar \
  ldbc/spark \
  --sn-dir /mnt/data
