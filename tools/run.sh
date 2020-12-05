#!/bin/bash

[ ! -f params.ini ] && echo "params.ini does not exist, exiting" && exit 1
[ ! -f target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar ] && echo "target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar does not exist, exiting" && exit 1

# make sure that out directory exists and clean previously generated data
mkdir -p out/
rm -rf out/*

spark-submit \
  --master local[*] \
  --class ldbc.snb.datagen.spark.LdbcDatagen \
  target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar \
  params.ini
