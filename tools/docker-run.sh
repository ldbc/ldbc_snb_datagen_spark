#!/bin/bash

# make sure that out directory exists and clean previously generated data
mkdir -p out/
rm -rf out/*
docker run --volume `pwd`/out:/out ldbc/datagen-standalone:latest ${@}
