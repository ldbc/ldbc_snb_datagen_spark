FROM ldbc/datagen-base:latest

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen
# Remove sample parameters
RUN rm params*.ini
# Build jar bundle
RUN mvn -DskipTests clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
ENV PATH "/opt/julia-1.2.0/bin:${PATH}"
CMD /opt/ldbc_snb_datagen/docker_run.sh
