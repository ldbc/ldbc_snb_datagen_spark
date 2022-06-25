# Running Datagen on EMR

We provide support scripts for running LDBC Datagen on EMR and storing the results on S3.

## Creating the infrastructure

### S3 Bucket

Create an S3 bucket and the `BUCKET_NAME` environment variable accordingly.

The bucket will have the following layout (created by the scripts/jobs):

- `params`: parameter files
- `jars`: application JARs
- `results`: results of successful runs
- `logs`: logs of the jobs

### AWS Roles

In AWS IAM, add the following roles with **Create Role** | **AWS service** | **EMR**:

* **EMR** a.k.a. `AmazonElasticMapReduceRole`, name it `EMR_DefaultRole`
* **EMR Role for EC2** a.k.a. `AmazonElasticMapReduceforEC2Role`, name it `EMR_EC2_DefaultRole`

## Install the required libraries

Install the required libraries as described in the [main README](../../README.md#install-python-tools).

## Submitting a job

1. Upload the JAR to S3. (We don't version the JARs yet, so you can only make sure that you run the intended code this way :( ) 

```bash
PLATFORM_VERSION=2.12_spark3.1
DATAGEN_VERSION=0.5.0-SNAPSHOT
aws s3 cp target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}-jar-with-dependencies.jar s3://${BUCKET_NAME}/jars/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}-jar-with-dependencies.jar
```

1. Submit the job. Run with `--help` for customization options.

```bash
JOB_NAME=MyTest
SCALE_FACTOR=10
./tools/emr/submit_datagen_job.py \
    --bucket ${BUCKET_NAME} \
    ${JOB_NAME} \
    ${SCALE_FACTOR} \
    csv \
    raw
```

Note: scale factors below 1 are not supported.

### Using spot instances

To use spot instances, add the `--use-spot` argument:

```bash
./tools/emr/submit_datagen_job.py \
    --use-spot \
    --bucket ${BUCKET_NAME} \
    ${JOB_NAME} \
    ${SCALE_FACTOR} \
    csv \
    raw
```

### Sample command

Generate the BI data set with the following configuration:

* use spot instances
* in the `csv-composite-projected-fk` format (`--explode-edges`)
* compress CSVs with `gzip`, and
* generate factors.

```bash
./tools/emr/submit_datagen_job.py \
    --use-spot \
    --bucket ${BUCKET_NAME} \
    --az us-east-2c \
    --copy-all \
    ${JOB_NAME} \
    ${SCALE_FACTOR} \
    csv \
    bi \
    -- \
    --explode-edges \
    --format-options compression=gzip \
    --generate-factors
```

### Using a different Spark / EMR version

We use EMR 6.3.0 by default, which packages Spark 3.1. You can use a different version by specifying it with the `--emr-version` option.
Make sure that you have uploaded the right JAR first.

```bash
PLATFORM_VERSION=2.12_spark3.1
./tools/emr/submit_datagen_job.py \
    --bucket ${BUCKET_NAME} \
    --platform-version ${PLATFORM_VERSION} \
    --emr-release emr-6.2.0 \
    ${JOB_NAME} \
    ${SCALE_FACTOR} \
    csv \
    raw
```

### Using a parameter file

The generator allows the use of an optional parameter file. To use a parameter file, upload it as follows.

```bash
aws s3 cp params-csv-basic-sf10000.ini s3://${BUCKET_NAME}/params/params-csv-basic-sf10000.ini
```
