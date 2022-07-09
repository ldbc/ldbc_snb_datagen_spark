FROM eclipse-temurin:8 as build-jar
ARG SBT_VERSION=1.5.2
RUN cd /opt && curl -fSsL https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz | tar xvz
ENV PATH=/opt/sbt/bin:$PATH
WORKDIR build
COPY build.sbt build.sbt
COPY project project
RUN sbt update
COPY src src
RUN sbt assembly

FROM scratch as jar
COPY --from=build-jar /build/target/ldbc_snb_datagen_*-jar-with-dependencies.jar /jar

FROM python:3.7-slim as build-tools
RUN pip install --no-cache virtualenv && virtualenv -p python3.7 /tools
COPY tools build
WORKDIR build
RUN . /tools/bin/activate && pip install .

FROM python:3.7-slim as tools
COPY --from=build-tools /tools /tools

FROM bde2020/spark-master:3.2.1-hadoop3.2 as standalone
COPY --from=jar /jar /jar
COPY --from=tools /tools /tools
RUN ln -sf /usr/bin/python3 /tools/bin/python

ENV TEMP_DIR /tmp
ENV SPARK_LOCAL_DIRS ${TEMP_DIR}
ENV PATH=/tools/bin:/spark/bin:$PATH
ENV LDBC_SNB_DATAGEN_JAR=/jar

WORKDIR /
ENTRYPOINT ["run.py"]
