#!/bin/bash

[ ! -f target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}-jar-with-dependencies.jar ] && echo "target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}-jar-with-dependencies.jar does not exist, exiting" && exit 1

# make sure that out directory exists and clean previously generated data
mkdir -p out/
rm -rf out/*
docker run \
  --env uid=`id -u` \
  --volume `pwd`/out:/mnt/data \
  --volume `pwd`/target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}-jar-with-dependencies.jar:/mnt/datagen.jar \
  ldbc/spark \
  --output-dir /mnt/data \
  ${@} # pass arguments of this script to the submit.sh script (Docker entrypoint)
