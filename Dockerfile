FROM bde2020/spark-master:3.2.1-hadoop3.2

ENV GOSU_VERSION 1.12

RUN apk add --no-cache su-exec
RUN apk add shadow
RUN [ -d /var/mail ] || mkdir /var/mail

VOLUME /mnt/datagen.jar /mnt/params.ini /mnt/data

WORKDIR /mnt/data

# adjust these environment variables
ENV TEMP_DIR /tmp
ENV EXECUTOR_MEMORY "1G"
ENV DRIVER_MEMORY "5G"

# the SPARK_* variables are used by submit.sh to configure the Spark job
ENV SPARK_LOCAL_DIRS ${TEMP_DIR}
ENV SPARK_SUBMIT_ARGS --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY}
ENV SPARK_APPLICATION_MAIN_CLASS ldbc.snb.datagen.LdbcDatagen
ENV SPARK_MASTER_URL local[*]
ENV SPARK_APPLICATION_JAR_LOCATION /mnt/datagen.jar

COPY submit.sh /

ENTRYPOINT ["/bin/bash", "/submit.sh"]
