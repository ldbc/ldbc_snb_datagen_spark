FROM bde2020/spark-master:2.4.5-hadoop2.7

VOLUME /mnt/datagen.jar /mnt/params.ini /mnt/data

WORKDIR /mnt/data

# adjust these environment variables
ENV TEMP_DIR /tmp
ENV EXECUTOR_MEMORY "1G"
ENV DRIVER_MEMORY "5G"

# the SPARK_* variables are used by submit.sh to configure the Spark job
ENV SPARK_LOCAL_DIRS ${TEMP_DIR}
ENV SPARK_SUBMIT_ARGS --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY}
ENV SPARK_APPLICATION_MAIN_CLASS ldbc.snb.datagen.spark.LdbcDatagen
ENV SPARK_MASTER_URL local[*]
ENV SPARK_APPLICATION_JAR_LOCATION /mnt/datagen.jar
ENV SPARK_APPLICATION_ARGS /mnt/params.ini

COPY submit.sh /

ENTRYPOINT ["/bin/bash", "/submit.sh"]
