
![LDBC_LOGO](https://raw.github.com/wiki/ldbc/ldbc_socialnet_bm/images/ldbc-logo.png)
LDBC-SNB Data Generator
----------------------

The LDBC-SNB Data Generator (DATAGEN) is the responsible of providing the data sets used by all the LDBC benchmarks. This data generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs)


ldbc_snb_datagen is part of the LDBC project (http://www.ldbc.eu/).
ldbc_snb_datagen is GPLv3 licensed, to see detailed information about this license read the LICENSE.txt.

* **[Releases](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/releases)**
* **[Configuration](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Configuration)**
* **[Compilation and Execution](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Compilation_Execution)**
* **[Advanced Configuration](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Advanced_Configuration)**
* **[Output](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Data-Output)**
* **[Troubleshooting](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Throubleshooting)**

**Datasets**

Publicly available datasets can be found at the LDBC-SNB Amazon Bucket. These datasets are the official SNB datasets and were generated using version 0.2.6. They are available in the three official supported serializers: CSV, CSVMergeForeign and TTL. The bucket is configured in "Requester Pays" mode, thus in order to access them you need a properly set up AWS client.
* http://ldbc-snb.s3.amazonaws.com/

**Community provided tools**

* **[Apache Flink Loader:] (https://github.com/s1ck/ldbc-flink-import)** A loader of LDBC datasets for Apache Flink
