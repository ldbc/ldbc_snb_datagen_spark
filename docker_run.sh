#!/bin/bash

set -e

if [ ! -f params.ini ]; then
    echo "The params.ini file is not present"
    exit 1
fi

# Running the generator
hadoop jar target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar params.ini

# Cleanup
rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*

# Move stuff to directory mounted to the host machine
mv social_network/ out/
