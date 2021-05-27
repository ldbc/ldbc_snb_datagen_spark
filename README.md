![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)

LDBC SNB Data Generator
----------------------

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_datagen.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_datagen)

:scroll: If you wish to cite the LDBC SNB, please refer to the [documentation repository](https://github.com/ldbc/ldbc_snb_docs#how-to-cite-ldbc-benchmarks).

:warning: There are two versions of this repository:
* To generate the Interactive SF1-1000 data sets, use the non-default [`stable` branch](https://github.com/ldbc/ldbc_snb_datagen/tree/stable) which runs on Hadoop.
* For the Interactive workload's larger data sets (up to SF30k) and for the BI workload, use the [`dev` branch](https://github.com/ldbc/ldbc_snb_datagen/) which runs on Spark. This is an experimental implementation. :warning: **Parameter generation is currently disabled for this branch and will be back in May 2021.**

The LDBC SNB Data Generator (Datagen) is the responsible for providing the datasets used by all the LDBC benchmarks. This data generator is designed to produce directed labelled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by Datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs).

[Generated small data sets](https://ldbc.github.io/ldbc_snb_datagen/) are deployed by the CI.

`ldbc_snb_datagen` is part of the [LDBC project](http://www.ldbcouncil.org/).
`ldbc_snb_datagen` is GPLv3 licensed, to see detailed information about this license read the `LICENSE.txt` file.

## Quick start

### Build the JAR

You can build the JAR with both Maven and SBT.

* To assemble the JAR file with Maven, run:

    ```bash
    tools/build.sh
    ```

* For faster builds during development, consider using SBT. To assemble the JAR file with SBT, run:

    ```bash
    sbt assembly
    ```

    :warning: When using SBT, change the path of the JAR file in the instructions provided in the README (`target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar` -> `./target/scala-2.11/ldbc_snb_datagen-assembly-${DATAGEN_VERSION}.jar`).

### Install tools

Some of the build utilities are written in Python. To use them, you have to create a Python virtual environment
and install the dependencies.

E.g. with [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv):
```bash
pyenv install 3.7.7
pyenv virtualenv 3.7.7 ldbc_datagen_tools
echo "3.7.7/envs/ldbc_datagen_tools" > .python-version
pip install --user -U pip -r tools/requirements.txt
```
### Running locally

The `tools/run.py` is intended for **local runs**. To use it, download and extract Spark as follows.

#### Spark 2.4.x

```bash
curl https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz | sudo tar -xz -C /opt/
export SPARK_HOME="/opt/spark-2.4.8-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin":"$PATH"
```

Make sure you use Java 8.

Run the script with:

```bash
export PLATFORM_VERSION=2.11_spark2.4
export DATAGEN_VERSION=0.4.0-SNAPSHOT

tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar <runtime configuration arguments> -- <generator configuration arguments>
```

#### Spark 3.1.x

```bash
curl https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz | sudo tar -xz -C /opt/
export SPARK_HOME="/opt/spark-3.1.1-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin":"$PATH"
```

Both Java 8 and Java 11 work.

To build, run

```bash
tools/build.sh -Pspark3.1
```

Run the script with:

```bash
export PLATFORM_VERSION=2.12_spark3.1
export DATAGEN_VERSION=0.4.0-SNAPSHOT
tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar <runtime configuration arguments> -- <generator configuration arguments>
```

The rest of the instructions are provided assuming Spark 2.4.x.

#### Runtime configuration arguments

The runtime configuration arguments determine the amount of memory, number of threads, degree of parallelism. For a list of arguments, see:

```bash
tools/run.py --help
```

To generate a single `part-*.csv` file, reduce the parallelism (number of Spark partitions) to 1.

```bash
./tools/run.py ./target/ldbc_snb_datagen_2.11_spark2.4-0.4.0-SNAPSHOT.jar --parallelism 1 -- --format csv --scale-factor 0.003 --mode interactive
```
#### Generator configuration arguments

The generator configuration arguments allow the configuration of the output directory, output format, layout, etc.

To get a complete list of the arguments, pass `--help` to the JAR file:

```bash
./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --help
```

* Passing `params.ini` files:

  ```bash
  ./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --param-file params.ini
  ```

* Generating `CsvBasic` files in Interactive mode:

  ```bash
  ./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor 0.003 --explode-edges --explode-attrs --mode interactive
  ```

* Generating `CsvCompositeMergeForeign` files in BI mode resulting in compressed `.csv.gz` files:

  ```bash
  ./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor 0.003 --mode bi --format-options compression=gzip
  ```

* Generating CSVs in `Raw` mode:

  ```bash
  ./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor 0.003 --mode raw
  ```

* For the `interactive` and `bi` formats, the `--format-options` argument allows passing formatting options such as timestamp/date formats and the presence/abscence of headers (see the [Spark formatting options](https://spark.apache.org/docs/2.4.8/api/scala/index.html#org.apache.spark.sql.DataFrameWriter) for details):

  ```bash
  ./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor 0.003 --mode interactive --format-options timestampFormat=MM/dd/YYYY\ HH:mm:ss,dateFormat=MM/dd/YYYY,header=false
  ```

To change the Spark configuration directory, adjust the `SPARK_CONF_DIR` environment variable.

A complex example:

```bash
export SPARK_CONF_DIR=./conf
./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar --parallelism 4 --memory 8G -- --format csv --format-options timestampFormat=MM/dd/YYYY\ HH:mm:ss,dateFormat=MM/dd/YYYY --explode-edges --explode-attrs --mode interactive --scale-factor 0.003
```

### Docker image

<!-- SNB Datagen images are available via [Docker Hub](https://hub.docker.com/r/ldbc/datagen/) (currently outdated). -->

The Docker image can be built with the provided Dockerfile. To build, execute the following command from the repository directory:

```bash
tools/docker-build.sh
```

See [Build the JAR](#build-the-jar) to build the library. Then, run the following:

```bash
tools/docker-run.sh
```

### Elastic MapReduce

We provide scripts to run Datagen on AWS EMR. See the README in the [`tools/emr`](tools/emr) directory for details.

## Parameter generation

The parameter generator is currently being reworked (see [relevant issue](https://github.com/ldbc/ldbc_snb_datagen/issues/83)) and no parameters are generated by default.
However, the legacy parameter generator is still available. To use it, run the following commands:

```bash
mkdir substitution_parameters
# for Interactive
paramgenerator/generateparams.py out/build/ substitution_parameters
# for BI
paramgenerator/generateparamsbi.py out/build/ substitution_parameters
```

## Larger scale factors

The scale factors SF3k+ are currently being fine-tuned, both regarding optimizing the generator and also for tuning the distributions.

## Graph schema

The graph schema is as follows:

![](https://raw.githubusercontent.com/ldbc/ldbc_snb_docs/dev/figures/schema-comfortable.png)

## Troubleshooting

* When running the tests, they might throw a `java.net.UnknownHostException: your_hostname: your_hostname: Name or service not known` coming from `org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal`. The solution is to add an entry of your machine's hostname to the `/etc/hosts` file: `127.0.1.1 your_hostname`.
* If you are using Docker and Spark runs out of space, make sure that Docker has enough space to store its containers. To move the location of the Docker containers to a larger disk, stop Docker, edit (or create) the `/etc/docker/daemon.json` file and add `{ "data-root": "/path/to/new/docker/data/dir" }`, then sync the old folder if needed, and restart Docker. (See [more detailed instructions](https://www.guguweb.com/2019/02/07/how-to-move-docker-data-directory-to-another-location-on-ubuntu/)).
* If you are using a local Spark installation and run out of space in `/tmp`, set the `SPARK_LOCAL_DIRS` to point to a directory with enough free space.
