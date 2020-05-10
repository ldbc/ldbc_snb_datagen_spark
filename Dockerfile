FROM ubuntu:20.04 AS build

ARG HADOOP_VERSION=3.2.1
ENV DEBIAN_FRONTEND=noninteractive

# Use Unicode
ENV LANG=C.UTF-8

RUN apt-get update
RUN apt-get install -y locales ssh git maven python curl openjdk-8-jdk && \
    update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
    
ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"

# Download hadoop
WORKDIR /opt
RUN curl -L "http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" | tar -xz

ENV HADOOP_HOME "/opt/hadoop-${HADOOP_VERSION}"
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

FROM build

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen

RUN mvn -DskipTests -ntp clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
ENV HADOOP_LOGLEVEL WARN
CMD ["/bin/sh", "/opt/ldbc_snb_datagen/docker_run.sh"]
