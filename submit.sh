#!/bin/bash

# based on: http://www.inanzzz.com/index.php/post/dna6/unning-docker-container-with-a-non-root-user-and-fixing-shared-volume-permissions-with-gosu
CURRENT_UID=${uid:-9999}
useradd --shell /bin/bash -u $CURRENT_UID -o -c "" -m docker
export HOME=/home/docker

export SPARK_HOME=/spark

echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
echo "Passing arguments ${SPARK_APPLICATION_ARGS}"

exec /usr/local/bin/gosu docker /spark/bin/spark-submit \
    --class ${SPARK_APPLICATION_MAIN_CLASS} \
    --master ${SPARK_MASTER_URL} \
    ${SPARK_SUBMIT_ARGS} \
    ${SPARK_APPLICATION_JAR_LOCATION} \
    ${SPARK_APPLICATION_ARGS} $@ # pass arguments of this script to the submit script
