#!/bin/bash
HADOOP_HOME=/home/aprat/Programs/hadoop-1.2.1 #change to your hadoop folder
LDBC_SOCIALNET_DBGEN_HOME=/home/aprat/Projects/LDBC/generator/ldbc_socialnet_bm/ldbc_socialnet_dbgen #change to your ldbc_socialnet_dbgen folder 
NUM_MACHINES=1 #the number of threads to use.
OUTPUT_DIR=/home/aprat/output #change to the folder where the generated data should be written

export HADOOP_HOME
export LDBC_SOCIALNET_DBGEN_HOME
export NUM_MACHINES 
export OUTPUT_DIR 

# FOR INTERNAL USAGE
# WARNING: This folders are removed upon execution start. Be careful not to point to anything important
export HADOOP_TMP_DIR=$OUTPUT_DIR/hadoop
export DATA_OUTPUT_DIR=$OUTPUT_DIR/social_network

mvn clean
mvn assembly:assembly

cp $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar $LDBC_SOCIALNET_DBGEN_HOME/
rm $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar

$HADOOP_HOME/bin/hadoop jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar $HADOOP_TMP_DIR $NUM_MACHINES $LDBC_SOCIALNET_DBGEN_HOME $DATA_OUTPUT_DIR


#parameter generation
PARAM_GENERATION=1

if [ $PARAM_GENERATION -eq 1 ]
then
	if [ -f m0factors.txt ]
	then
		rm m0factors.txt
	fi
	if [ -f m0friendList0.csv ]
	then
		rm m0friendList0.csv
	fi
	$HADOOP_HOME/bin/hadoop fs -copyToLocal $DATA_OUTPUT_DIR/m0factors.txt .
	$HADOOP_HOME/bin/hadoop fs -copyToLocal $DATA_OUTPUT_DIR/m0friendList* .
	mkdir -p substitution_parameters	
	python paramgenerator/generateparams.py m0factors.txt m0friendList0.csv substitution_parameters/
	rm -f m0factors.txt
	rm -f m0friendList0.csv
fi
