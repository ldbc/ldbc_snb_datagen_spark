#!/bin/bash

cd ../social_network/csv/raw

while read line; do
  IFS=' ' read -r -a array <<< $line
  FILENAME=${array[0]}
  HEADER=${array[1]}

  echo ${FILENAME}: ${HEADER}
  echo ${HEADER} | cat - ${FILENAME}/*.csv > ${FILENAME}.csv
done < ../../../tools/headers.txt

NEO4J_CONTAINER_ROOT=`pwd`/neo4j-scratch
NEO4J_DATA_DIR=`pwd`/neo4j-scratch/data
NEO4J_CSV_DIR=`pwd`
NEO4J_VERSION=4.2.1

rm -rf ${NEO4J_DATA_DIR}
mkdir ${NEO4J_DATA_DIR}

# make sure directories exist
mkdir -p ${NEO4J_CONTAINER_ROOT}/{logs,import,plugins}

# start with a fresh data dir (required by the CSV importer)
mkdir -p ${NEO4J_DATA_DIR}
rm -rf ${NEO4J_DATA_DIR}/*

docker run --rm \
    --user="$(id -u):$(id -g)" \
    --publish=7474:7474 \
    --publish=7687:7687 \
    --volume=${NEO4J_DATA_DIR}:/data \
    --volume=${NEO4J_CSV_DIR}:/import \
    ${NEO4J_ENV_VARS} \
    neo4j:${NEO4J_VERSION} \
    neo4j-admin import \
    --delimiter '|' \
    --id-type=INTEGER \
    --nodes=Forum="/import/forum.csv" \
    --nodes=Person="/import/person.csv" \
    --nodes=Message:Post="/import/post.csv" \
    --relationships=KNOWS="/import/person_knows_person.csv" \
    --relationships=HAS_CREATOR="/import/post_hasCreator_person.csv" \
    --relationships=CONTAINER_OF="/import/forum_containerOf_post.csv" \
