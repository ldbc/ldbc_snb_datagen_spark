![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

LDBC SNB Data Generator
----------------------

[![Build Status](https://travis-ci.org/ldbc/ldbc_snb_datagen.svg?branch=master)](https://travis-ci.org/ldbc/ldbc_snb_datagen)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5b0c677c9c4c4de3b6af15f118c9212c)](https://www.codacy.com/app/ArnauPrat/ldbc_snb_datagen?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ldbc/ldbc_snb_datagen&amp;utm_campaign=Badge_Grade)

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks).

The LDBC SNB Data Generator (Datagen) is the responsible of providing the datasets used by all the LDBC benchmarks. This data generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by Datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs).


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

### Docker image

SNB datagen images are available via [Docker Hub](https://hub.docker.com/r/ldbc/datagen/) where you may find both the latest version of the generator as well as previous stable versions.

Alternatively, the image can be built with the provided Dockerfile. To build, execute the following command from the repository directory:

```bash
docker build . -t ldbc/spark
```

Then, assemble the JAR file and run the docker image using the mounted jar:

```bash
mvn assembly:assembly -DskipTests && \
  docker run -v `pwd`/out:/mnt/data -v `pwd`/params.ini:/mnt/params.ini -v `pwd`/target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar:/mnt/jar ldbc/spark &&
  sudo chown -R $USER:$USER social_network/ substitution_parameters/
```

### Graph schema

The graph schema is as follows:

![](https://raw.githubusercontent.com/ldbc/ldbc_snb_docs/dev/figures/schema-comfortable.png)

### Community provided tools

* **[Apache Flink Loader:](https://github.com/s1ck/ldbc-flink-import)** A loader of LDBC datasets for Apache Flink.
