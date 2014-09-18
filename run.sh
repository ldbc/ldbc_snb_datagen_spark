#!/bin/bash
HADOOP_HOME=/home/user/hadoop-1.2.1 #change to your hadoop folder
LDBC_SNB_DATAGEN_HOME=/home/user/ldbc_snb_datagen #change to your ldbc_socialnet_dbgen folder 

export HADOOP_HOME
export LDBC_SNB_DATAGEN_HOME

mvn clean
mvn assembly:assembly

cp $LDBC_SNB_DATAGEN_HOME/target/ldbc_socialnet_dbgen.jar $LDBC_SNB_DATAGEN_HOME/
rm $LDBC_SNB_DATAGEN_HOME/target/ldbc_socialnet_dbgen.jar

$HADOOP_HOME/bin/hadoop  jar $LDBC_SNB_DATAGEN_HOME/ldbc_socialnet_dbgen.jar $LDBC_SNB_DATAGEN_HOME/params.ini 

#parameter generation
PARAM_GENERATION=1

if [ $PARAM_GENERATION -eq 1 ]
then
	mkdir -p substitution_parameters
	python paramgenerator/generateparams.py $LDBC_SNB_DATAGEN_HOME substitution_parameters/
  	rm -f m0factors.txt
	rm -f m0friendList*
fi
