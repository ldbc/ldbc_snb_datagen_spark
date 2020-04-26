FROM openjdk:8-jdk-stretch

# Download hadoop
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y maven python
RUN curl -L 'http://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz' | tar -xz

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen
# Remove sample parameters
# Build jar bundle
RUN mvn -DskipTests clean assembly:assembly
