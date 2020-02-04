![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

LDBC-SNB Data Generator
----------------------

[![Build Status](https://travis-ci.org/ldbc/ldbc_snb_datagen.svg?branch=master)](https://travis-ci.org/ldbc/ldbc_snb_datagen)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5b0c677c9c4c4de3b6af15f118c9212c)](https://www.codacy.com/app/ArnauPrat/ldbc_snb_datagen?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ldbc/ldbc_snb_datagen&amp;utm_campaign=Badge_Grade)

The LDBC-SNB Data Generator (Datagen) is the responsible of providing the datasets used by all the LDBC benchmarks. This data generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by Datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs).


`ldbc_snb_datagen` is part of the [LDBC project](http://www.ldbcouncil.org/).
`ldbc_snb_datagen` is GPLv3 licensed, to see detailed information about this license read the `LICENSE.txt` file.

* **[Releases](https://github.com/ldbc/ldbc_snb_datagen/releases)**
* **[Configuration](https://github.com/ldbc/ldbc_snb_datagen/wiki/Configuration)**
* **[Compilation and Execution](https://github.com/ldbc/ldbc_snb_datagen/wiki/Compilation_Execution)**
* **[Advanced Configuration](https://github.com/ldbc/ldbc_snb_datagen/wiki/Advanced_Configuration)**
* **[Output](https://github.com/ldbc/ldbc_snb_datagen/wiki/Data-Output)**
* **[Troubleshooting](https://github.com/ldbc/ldbc_snb_datagen/wiki/Troubleshooting)**

## Quick start

### Configuration

Initialize the `params.ini` file as needed. For example, to generate the basic CSV files, issue:

```bash
cp params-csv-basic.ini params.ini
```

There are three main ways to run Datagen, each using a different approach to configure the amount of memory available.

1. using a pseudo-distributed Hadoop installation,
2. running the same setup in a Docker image,
3. running on a distributed Hadoop cluster.

### Pseudo-distributed Hadoop node

To configure the amount of memory available, set the `HADOOP_CLIENT_OPTS` environment variable.
To grab Hadoop, extract it, and set the environment values to sensible defaults, and generate the data as specified in the `params-csv-params.ini` template file, run the following script:

```bash
cp params-csv-basic.ini params.ini
wget http://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xf hadoop-3.2.1.tar.gz
export HADOOP_CLIENT_OPTS="-Xmx2G"
# set this to the Hadoop 3.2.1 directory
export HADOOP_HOME=`pwd`/hadoop-3.2.1
# set this to the repository's directory
export LDBC_SNB_DATAGEN_HOME=`pwd`
./run.sh
```

### Docker image

SNB datagen images are available via [Docker Hub](https://hub.docker.com/r/ldbc/datagen/) where you may find both the latest version of the generator as well as previous stable versions.

Alternatively, the image can be built with the provided Dockerfile. To build, execute the following command from the repository directory:

```bash
docker build . --tag ldbc/datagen
```

#### Running

Set the `params.ini` in the repository as for the pseudo-distributed case. The file will be mounted in the container by the `--mount type=bind,source="$(pwd)/params.ini,target="/opt/ldbc_snb_datagen/params.ini"` option. If required, the source path can be set to a different path.

The container outputs its results in the `/opt/ldbc_snb_datagen/out/` directory which contains two sub-directories, `social_network/` and `substitution_parameters`. In order to save the results of the generation, a directory must be mounted in the container from the host. The driver requires the results be in the datagen repository directory. To generate the data, run the following command which includes changing the owner (`chown`) of the Docker-mounted volumes.

:warning: This removes the previously generated `social_network` directory:

```bash
rm -rf social_network/ substitution_parameters && \
  docker run --rm --mount type=bind,source="$(pwd)/",target="/opt/ldbc_snb_datagen/out" --mount type=bind,source="$(pwd)/params.ini",target="/opt/ldbc_snb_datagen/params.ini" ldbc/datagen; \
  sudo chown -R $USER:$USER social_network/ substitution_parameters/
```

If you need to raise the memory limit, use the `-e HADOOP_CLIENT_OPTS="-Xmx..."` parameter to override the default value (`-Xmx2G`).

### Hadoop cluster

Instructions are currently not provided.

### Graph schema

The graph schema is as follows:

![](https://raw.githubusercontent.com/ldbc/ldbc_snb_docs/dev/figures/schema-comfortable.png)

### Community provided tools

* **[Apache Flink Loader:](https://github.com/s1ck/ldbc-flink-import)** A loader of LDBC datasets for Apache Flink.
