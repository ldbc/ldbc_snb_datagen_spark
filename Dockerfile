FROM bde2020/spark-master:2.4.5-hadoop2.7

VOLUME /mnt/datagen.jar /mnt/params.ini /mnt/data

WORKDIR /mnt/data

ENTRYPOINT ["/spark/bin/spark-submit"]

# memory settings are sufficient for SF10,
# increase proportionally for larger scale factors
CMD ["--class", "ldbc.snb.datagen.spark.LdbcDatagen", \
     "--master", "local[*]", \
     "--executor-memory", "1G", \
     "--driver-memory", "5G", \
     "/mnt/datagen.jar", \
     "/mnt/params.ini" \
]
