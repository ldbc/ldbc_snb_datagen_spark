# Running datagen on EMR

## Install required scripts

1. From the repository root, run:

```
pip install -r tools/emr/requirements.txt
```

1. Package JAR. Make sure you use Java 8 for Spark 2.x:

```bash
man package -DskipTests
```
## Submitting a job

1. Upload parameter file to S3 (if it does not exist).

```bash
aws s3 cp params-csv-basic-sf10000.ini s3://ldbc-snb-datagen-store/params/params-csv-basic-sf10000.ini
```

1. Upload JAR to S3. (We don't version the JARs yet, so you can only make sure that you run the intended code this way :( ) 

```bash
aws s3 cp target/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar s3://ldbc-snb-datagen-store/jars/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar
```

1. Submit job. Run with `--help` for customization options.

```bash
./tools/emr/submit_datagen_job.py params-csv-basic-sf1.ini 1
```
