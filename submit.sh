#!/bin/bash

# based on
# - http://www.inanzzz.com/index.php/post/dna6/unning-docker-container-with-a-non-root-user-and-fixing-shared-volume-permissions-with-gosu
# - https://github.com/tianon/gosu/blob/master/INSTALL.md
# - https://gist.github.com/yogeek/bc8dc6dadbb72cb39efadf83920077d3
# - https://github.com/ncopa/su-exec
CURRENT_UID=${uid:-9999}
useradd --shell /bin/bash -u $CURRENT_UID -o -c "" -m docker
export HOME=/home/docker

export SPARK_HOME=/spark

echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
echo "Passing arguments ${@}"

exec /sbin/su-exec docker /spark/bin/spark-submit \
    --class ${SPARK_APPLICATION_MAIN_CLASS} \
    --master ${SPARK_MASTER_URL} \
    ${SPARK_SUBMIT_ARGS} \
    ${SPARK_APPLICATION_JAR_LOCATION} \
    ${@} # pass arguments of this script to the spark-submit script
