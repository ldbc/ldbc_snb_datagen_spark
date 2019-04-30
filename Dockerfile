FROM openjdk:8-jdk-alpine

# Download hadoop
WORKDIR /opt
RUN apk add bash curl maven python
RUN curl -L 'http://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz' | tar -xz

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen
RUN mvn -DskipTests clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
ENV DATAGEN_SCALE_FACTOR 'snb.interactive.1'
ENV DATAGEN_PERSON_SERIALIZER 'ldbc.snb.datagen.serializer.snb.interactive.CSVPersonSerializer'
ENV DATAGEN_INVARIANT_SERIALIZER 'ldbc.snb.datagen.serializer.snb.interactive.CSVInvariantSerializer'
ENV DATAGEN_PERSON_ACTIVITY_SERIALIZER 'ldbc.snb.datagen.serializer.snb.interactive.CSVPersonActivitySerializer'

CMD /opt/ldbc_snb_datagen/docker_run.sh