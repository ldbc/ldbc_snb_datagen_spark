#!/bin/bash

# Variables for the default settings
DEFAULT_SCALE_FACTOR=snb.interactive.1
DEFAULT_PERSON_SERIALIZER=ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer
DEFAULT_INVARIANT_SERIALIZER=ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer
DEFAULT_PERSON_ACTIVITY_SERIALIZER=ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer

# Parameter serialization
PARAMS_FILE=params.ini
echo "ldbc.snb.datagen.generator.scaleFactor:${SCALE_FACTOR:-$DEFAULT_SCALE_FACTOR}" > ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.personSerializer:${PERSON_SERIALIZER:-$DEFAULT_SERIALIZER}" >> ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.invariantSerializer:${INVARIANT_SERIALIZER:-$DEFAULT_INVARIANT_SERIALIZER}" >> ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.personActivitySerializer:${PERSON_ACTIVITY_SERIALIZER:-$DEFAULT_PERSON_ACTIVITY_SERIALIZER}" >> ${PARAMS_FILE}

# Running the generator
/opt/hadoop-2.6.0/bin/hadoop jar /opt/ldbc_snb_datagen/target/ldbc_snb_datagen-0.2.7-jar-with-dependencies.jar /opt/ldbc_snb_datagen/params.ini

# Cleanup
rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*
