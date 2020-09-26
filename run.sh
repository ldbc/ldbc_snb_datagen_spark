#!/bin/bash

sudo chcon -Rt svirt_sandbox_file_t .

docker run -v `pwd`/out:/mnt/data -v `pwd`/params.ini:/mnt/params.ini -v `pwd`/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar:/mnt/datagen.jar ldbc/spark

sudo chown -R `id -u`:`id -g` out
