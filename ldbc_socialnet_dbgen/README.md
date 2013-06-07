# Introduction

ldbc_socialnet_dbgen is part of the LDBC project (http://www.ldbc.eu/).
ldbc_socialnet_dbgen is GPLv3 licensed, to see detailed information about this license read the LICENSE.txt.

This software was build using Apache hadoop version 1.0.3 and we not guarantee compatibility with newer releases.
You can download hadoop 1.0.3 from http://archive.apache.org/dist/hadoop/core/hadoop-1.0.3/


## Compilation

The compilation uses Apache Maven to automatically detect and download the necessary dependencies. See maven.apache.org.

To generate the jar containing all the dependencies the following maven instruction is used:

mvn assembly:assembly

This can lead to the generation of two jars the default one called ldbc_socialnet_dbgen-<Version-Number>.jar or the one containing all the dependencies inside the jar called ldbc_socialnet_dbgen.jar.


## Configuration

* Configure your hadoop machine or cluster. For more information on how to do it, please refer its official page http://hadoop.apache.org/docs/stable/index.html

* Configure the params.ini to your needs. This file contains:
	- numtotalUser: The number of users the social network will have. It shoud be bigger than 1000.
	- startYear: The first year.
	- numYears: The period of years.
	- serializerType: The serializer type has to be one of this three values: ttl (Turtle format), nt (N-Triples format), csv (coma separated value).
	- rdfOutputFileName: The base name for the files generated in rdf format (Turtle and N-Triples)
	
This configuration will generate for the startYear-01-01 to the (startYear+numYears)-01-01 period activity in the simulated social network for the amount of users configurated.


##Execution
To execute ldbc_socialnet_dbgen hadoop is required.

Terminology:

* $HADOOP_HOME is used to refer to the hadoop-1.0.3 folder in your system.
* $LDBC_SOCIALNET_DBGEN_HOME is used to refer to the ldbc_socialnet_dbgen folder in your system.

The execution instruction is:

$HADOOP_HOME/bin/hadoop jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar hadoop_input_folder hadoop_output_folder Num_machines_ldbc_will_use  $LDBC_SOCIALNET_DBGEN_HOME/ Final_output_folder
