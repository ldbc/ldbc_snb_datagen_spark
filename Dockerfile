FROM bde2020/spark-master:2.4.5-hadoop2.7

ENV GOSU_VERSION 1.12
RUN set -eux; \
	\
	apk add --no-cache --virtual .gosu-deps \
		ca-certificates \
		dpkg \
		gnupg \
	; \
	\
	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
	wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
	wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
	\
# verify the signature
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
	gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
	command -v gpgconf && gpgconf --kill all || :; \
	rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
	\
# clean up fetch dependencies
	apk del --no-network .gosu-deps; \
	\
	chmod +x /usr/local/bin/gosu; \
# verify that the binary works
	gosu --version; \
	gosu nobody true

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
ENV SPARK_APPLICATION_MAIN_CLASS ldbc.snb.datagen.spark.LdbcDatagen
ENV SPARK_MASTER_URL local[*]
ENV SPARK_APPLICATION_JAR_LOCATION /mnt/datagen.jar

COPY submit.sh /

ENTRYPOINT ["/bin/bash", "/submit.sh"]
