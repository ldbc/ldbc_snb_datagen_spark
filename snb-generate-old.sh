#!/bin/bash

# This script generates old versions of SNB datasets.
# Beware that the script cleans the current git repository and discards any changes.

# To run this script:
#
# 1. Configure Hadoop and set $HADOOP_HOME. Hadoop 2.6.0 works for all DATAGEN versions.
#
# 2. Make sure Hadoop's temp directory has enough space,
#    see https://github.com/ldbc/ldbc_snb_datagen/wiki/Troubleshooting#javaioioexception-no-space-left-on-device
#
# 3. Make sure the operating system's temp directory (e.g. /tmp) has enough space
#
# 4. Set up DATAGEN as required, ensuring that Hadoop has enough memory if its not already configured.
#
# export HADOOP_CLIENT_OPTS=-Xmx20G
#
# 5. Set the following environment variables, e.g.
#
# export SCALE_FACTOR=30
# export STORE=false # only set this to true if you have enough space to store all graphs
#
# 6. Move this script outside the ldbc_snb_datagen directory and run it.

# Set the environment variables
export SCALE_FACTOR=
export STORE=

if [ -z "$SCALE_FACTOR" ] || [ -z "$STORE" ]; then
    echo Please set the SCALE_FACTOR and STORE variables in the script.
    exit 1
fi

# Start generating graphs
cd ldbc_snb_datagen || { echo "Could not change directory into ldbc_snb_datagen"; exit 1; }
echo "Generation sequence started" >> ../datagen-snb.log

if [ "$STORE" = true ] ; then
    mkdir ../datagen-graphs
fi

for VERSION in v0.2.1 v0.2.2 v0.2.3 v0.2.4 v0.2.5 v0.2.6 v0.2.7 v0.2.8; do
    echo $VERSION >> ../datagen-snb.log

    git checkout -- .
    git clean -fxd .
    git checkout $VERSION

    echo > params.ini
    echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.$SCALE_FACTOR >> params.ini
    echo ldbc.snb.datagen.serializer.personSerializer:ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer >> params.ini
    echo ldbc.snb.datagen.serializer.invariantSerializer:ldbc.snb.datagen.serializer.snb.interactive.CSVInvariantSerializer >> params.ini
    echo ldbc.snb.datagen.serializer.personActivitySerializer:ldbc.snb.datagen.serializer.snb.interactive.CSVPersonActivitySerializer >> params.ini

    ./run.sh
    tail -n +2 social_network/person_0_0.csv | wc -l >> ../datagen-snb.log
    tail -n +2 social_network/person_knows_person_0_0.csv | wc -l >> ../datagen-snb.log

    if [ "$STORE" = true ] ; then
        mv social_network ../datagen-graphs/social_network-$VERSION
    fi
done
