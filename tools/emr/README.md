# Running Datagen on EMR

We provide support scripts for running LDBC Datagen on EMR and storing the results
on S3.

## Creating the infrastructure
Create an S3 bucket. This bucket will have the following layout:

- `params`: parameter files
- `jars`: application JARs
- `results`: results of successful runs
- `logs`: logs of the jobs

## Install the required libraries

1. From the repository root, run:

```
pip install -r tools/requirements.txt
```

1. Package the JAR. Make sure you use Java 8:

```bash
./tools/build.sh
```
## Submitting a job

1. Upload the parameter file to S3 (if it does not exist).

```bash
aws s3 cp params-csv-basic-sf10000.ini s3://${BUCKET_NAME}/params/params-csv-basic-sf10000.ini
```

1. Upload the JAR to S3. (We don't version the JARs yet, so you can only make sure that you run the intended code this way :( ) 

```bash
aws s3 cp target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar s3://${BUCKET_NAME}/jars/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar
```

1. Submit the job. Run with `--help` for customization options.

```bash
./tools/emr/submit_datagen_job.py --bucket ${BUCKET_NAME} params-csv-basic-sf1.ini 1
```
