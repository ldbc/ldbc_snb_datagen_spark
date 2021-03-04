#!/bin/bash

export HADOOP_CLIENT_OPTS="-Xmx20G"

for SF in 0.1 0.3 1 3 10; do
    echo SF: $SF

    echo > params.ini
    echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.$SF >> params.ini
    echo ldbc.snb.datagen.serializer.numUpdatePartitions:16 >> params.ini
    echo ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvBasicDynamicActivitySerializer >> params.ini
    echo ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvBasicDynamicPersonSerializer >> params.ini
    echo ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvBasicStaticSerializer >> params.ini

    ./run.sh
    mv social_network ../datagen-graphs/social_network-sf$SF
done
