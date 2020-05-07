FROM ubuntu:20.04 AS base

RUN mkdir -p /usr/share/man/man1 && mkdir -p /usr/share/man/man7 

ARG HADOOP_VERSION=3.2.1

# Download hadoop
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y maven python curl openjdk-8-jdk
RUN curl -L "http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" | tar -xz

ENV HADOOP_HOME "/opt/hadoop-${HADOOP_VERSION}"
ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen
RUN mvn -DskipTests -ntp clean assembly:assembly

FROM base

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
ENV HADOOP_LOGLEVEL WARN
CMD ["/bin/sh", "/opt/ldbc_snb_datagen/docker_run.sh"]
