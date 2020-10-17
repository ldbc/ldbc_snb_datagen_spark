# Distribution Analysis #

This directory includes the scripts, data and plots for the various distributions used in Datagen.

```
# produces the CDF used when assigning comments a creation date
./comment_cdf.sh

# produces the CDF used when assiging posts in flashmob events a creation date
./flashmob_posts_cdf.sh

# produces a histogram of the knows degree distribution for SF1 and prints the average degree for each scale factor
./fb_degree_analysis.sh

# produces the analysis of the Facebook friend removal data
./fb_dump_analysis.sh

# delete operation counts
./delete_operation_counts.sh
```

Note, raw data of Facebook user's friend removal is not included. However, plots `friends_vs_removed`, `hist_num_friends` and `friends_vs_removed.pdf` can be found in `/graphics/`.

# Running datagen on EMR

1. Upload parameter file to S3 (if it does not exist).

```shell
aws s3 cp params-csv-basic-sf10000.ini s3://ldbc-snb-datagen-store/params/params-csv-basic-sf10000.ini
```

2. Upload JAR to S3. (We don't version the JARs yet, so you can only make sure that you run the intended code this way :( ) 

``` shell
aws s3 cp target/ldbc_snb_datagen-0.4.0-SNAPSHOT.jar s3://ldbc-snb-datagen-store/jars/ldbc_snb_datagen-0.4.0-SNAPSHOT-jar-with-dependencies.jar
```

3. Submit job. Run with `--help` for customization options.

``` shell
./tools/emr/submit_datagen_job.py params-csv-basic-sf10000.ini 10000
```
