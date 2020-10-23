#!/bin/bash

# clean previously generated data
rm -rf out/*
docker run -v `pwd`/out:/mnt/data -v `pwd`/params.ini:/mnt/params.ini -v `pwd`/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar:/mnt/datagen.jar ldbc/spark

# see issue #178
sudo chown -R `id -u`:`id -g` out
