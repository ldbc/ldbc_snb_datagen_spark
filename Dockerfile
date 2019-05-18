FROM openjdk:8-jdk-stretch

# Download hadoop
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y bash curl maven python
RUN curl -L 'http://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz' | tar -xz
RUN curl -L 'https://julialang-s3.julialang.org/bin/linux/x64/1.1/julia-1.1.1-linux-x86_64.tar.gz' | tar -xz

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen
# Remove sample parameters
RUN rm params*.ini
# Build jar bundle
RUN mvn -DskipTests clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
ENV PATH "/opt/julia-1.1.1/bin:${PATH}"
CMD /opt/ldbc_snb_datagen/docker_run.sh
CMD julia -e 1+1
