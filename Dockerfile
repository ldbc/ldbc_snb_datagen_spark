FROM bde2020/spark-master:2.4.5-hadoop2.7

VOLUME /mnt/datagen.jar /mnt/params.ini /mnt/data

WORKDIR /mnt/data

ENTRYPOINT ["/spark/bin/spark-submit"]

CMD ["--class", "ldbc.snb.datagen.spark.LdbcDatagen", \
     "--master", "local[*]", \
     "/mnt/datagen.jar", \
     "/mnt/params.ini" \
]
