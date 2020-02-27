#!/bin/bash

mvn -DskipTests assembly:assembly

for SERIALIZER in csv-basic csv-composite csv-composite-merge-foreign csv-merge-foreign; do
    echo "=== $SERIALIZER ==="
    cp params-$SERIALIZER.ini params.ini
    sed -i 's/interactive\.1$/interactive.0.1/' params.ini
    $HADOOP_HOME/bin/hadoop jar $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar $LDBC_SNB_DATAGEN_HOME/params.ini
    mv social_network social_network_$SERIALIZER
done
