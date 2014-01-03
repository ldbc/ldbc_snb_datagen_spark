# Introduction

The LDBC Social Network Dataset Generator (SNDG) is the responsible of providing the data sets used by all the LDBC benchmarks. This dataset generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the generator can be found in the following pages:

* In **[Data Schema](https://github.com/ldbc/ldbc_socialnet_bm/wiki/Data-Schema)**, a description of the schema of the data produced by the generator.
* In **[Data Generation Process](https://github.com/ldbc/ldbc_socialnet_bm/wiki/Data-Generation)**, information about the generation process of the data.
* In **[Data Output](https://github.com/ldbc/ldbc_socialnet_bm/wiki/Data-Output)**, a description of the contents and the format of the files produced by the generator.


ldbc_socialnet_dbgen is part of the LDBC project (http://www.ldbc.eu/).
ldbc_socialnet_dbgen is GPLv3 licensed, to see detailed information about this license read the LICENSE.txt.


## Requirements

This software is build using Apache hadoop version 1.0.3 and we not guarantee compatibility with newer releases.
You can download hadoop 1.0.3 from [here](http://archive.apache.org/dist/hadoop/core/hadoop-1.0.3/). To Configure your hadoop machine or cluster, please visit [here](http://hadoop.apache.org/docs/stable/index.html).


## Compilation

The compilation uses [Apache Maven](http://maven.apache.org) to automatically detect and download the necessary dependencies. Make sure you are in your ldbc_socialnet_bm/ldbc_socialnet_dbgen/ project folder.
To generate the jar containing all the dependencies, type

```
mvn assembly:assembly
```

This can lead to the generation of two jars in the target folder: the default one called ldbc_socialnet_dbgen-\<Version-Number\>.jar or the one containing all the dependencies inside the jar called ldbc_socialnet_dbgen.jar.


## Configuration

The SNDG is configured by means of the ldbc\_socialnet\_bm/ldbc\_socialnet\_dbgen/_params.init_ file. Set the parameters properly to meet your needs. This file has the following format.

```
	numtotalUser: #The number of users the social network will have. It shoud be bigger than 1000.
	startYear: #The first year.
	numYears: #The period of years.
	serializerType: #The serializer type has to be one of this three values: ttl (Turtle format), n3 (N3 format), csv (coma separated value).
	rdfOutputFileName: #The base name for the files generated in rdf format (Turtle and N3)
```
	
This configuration will generate a database for the startYear-01-01 to the (startYear+numYears)-01-01 period activity in the simulated social network for the amount of users configurated.


## Execution
To execute ldbc_socialnet_dbgen hadoop is required. And the assumption that the ldbc_socialnet_dbgen.jar is at your ldbc_socialnet_bm/ldbc_socialnet_dbgen/

Terminology:

* $HADOOP_HOME is used to refer to the hadoop-1.0.3 folder in your system.
* $LDBC_SOCIALNET_DBGEN_HOME is used to refer to the ldbc_socialnet_dbgen folder in your system.

To execute the generator, please type:

```
$HADOOP_HOME/bin/hadoop jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar hadoop_input_folder hadoop_output_folder Num_machines_ldbc_will_use  $LDBC_SOCIALNET_DBGEN_HOME/ Final_output_folder
```

You can refer to the run.sh script to see a clearer example of how to run it.

## Output
The generator can create data in three formats: CSV, TTL and N3. For more information please check the [wiki](https://github.com/ldbc/ldbc_socialnet_bm/wiki/Data-Output)
