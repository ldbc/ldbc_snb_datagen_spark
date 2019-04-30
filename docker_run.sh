#o!/bin/bash

# Parameter serialization
PARAMS_FILE=params.ini
echo "ldbc.snb.datagen.generator.scaleFactor:${DATAGEN_SCALE_FACTOR}" >> ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.personSerializer:${DATAGEN_PERSON_SERIALIZER}" >> ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.invariantSerializer:${DATAGEN_INVARIANT_SERIALIZER}" >> ${PARAMS_FILE}
echo "ldbc.snb.datagen.serializer.personActivitySerializer:${DATAGEN_PERSON_ACTIVITY_SERIALIZER}" >> ${PARAMS_FILE}

# Running the generator
/opt/hadoop-2.6.0/bin/hadoop jar /opt/ldbc_snb_datagen/target/ldbc_snb_datagen-0.2.7-jar-with-dependencies.jar /opt/ldbc_snb_datagen/params.ini

# Cleanup
rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*
