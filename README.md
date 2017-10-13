![LDBC_LOGO](https://raw.githubusercontent.com/wiki/ldbc/ldbc_snb_datagen/images/ldbc-logo.png)
LDBC-SNB Data Generator
----------------------

[![Build Status](https://travis-ci.org/ldbc/ldbc_snb_datagen.svg?branch=master)](https://travis-ci.org/ldbc/ldbc_snb_datagen)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5b0c677c9c4c4de3b6af15f118c9212c)](https://www.codacy.com/app/ArnauPrat/ldbc_snb_datagen?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ldbc/ldbc_snb_datagen&amp;utm_campaign=Badge_Grade)

The LDBC-SNB Data Generator (DATAGEN) is the responsible of providing the data sets used by all the LDBC benchmarks. This data generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs).


ldbc_snb_datagen is part of the [LDBC project](http://www.ldbc.eu/).
ldbc_snb_datagen is GPLv3 licensed, to see detailed information about this license read the `LICENSE.txt` file.

* **[Releases](https://github.com/ldbc/ldbc_snb_datagen/releases)**
* **[Configuration](https://github.com/ldbc/ldbc_snb_datagen/wiki/Configuration)**
* **[Compilation and Execution](https://github.com/ldbc/ldbc_snb_datagen/wiki/Compilation_Execution)**
* **[Advanced Configuration](https://github.com/ldbc/ldbc_snb_datagen/wiki/Advanced_Configuration)**
* **[Output](https://github.com/ldbc/ldbc_snb_datagen/wiki/Data-Output)**
* **[Troubleshooting](https://github.com/ldbc/ldbc_snb_datagen/wiki/Troubleshooting)**

<!-- **Datasets** -->

<!-- Publicly available datasets can be found at the LDBC-SNB Amazon Bucket. These datasets are the official SNB datasets and were  generated using version 0.2.6. They are available in the three official supported serializers: CSV, CSVMergeForeign and TTL. The bucket is configured in "Requester Pays" mode, thus in order to access them you need a properly set up AWS client.
* http://ldbc-snb.s3.amazonaws.com/ -->

**Community provided tools**

* **[Apache Flink Loader:](https://github.com/s1ck/ldbc-flink-import)** A loader of LDBC datasets for Apache Flink.
