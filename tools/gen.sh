#!/bin/sh

# only use this script once Datagen has been tested and compiled

$HADOOP_HOME/bin/hadoop jar $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar $LDBC_SNB_DATAGEN_HOME/params.ini
