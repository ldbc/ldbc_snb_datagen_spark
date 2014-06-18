#!/bin/bash
HADOOP_HOME=/home/user/hadoop-1.2.1 #change to your hadoop folder
LDBC_SOCIALNET_DBGEN_HOME=/home/user/ldbc_socialnet_bm/ldbc_socialnet_dbgen #change to your ldbc_socialnet_dbgen folder 

export HADOOP_HOME
export LDBC_SOCIALNET_DBGEN_HOME

mvn clean
mvn assembly:assembly

cp $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar $LDBC_SOCIALNET_DBGEN_HOME/
rm $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar

$HADOOP_HOME/bin/hadoop  jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar $LDBC_SOCIALNET_DBGEN_HOME/params.ini 

#parameter generation
PARAM_GENERATION=1

if [ $PARAM_GENERATION -eq 1 ]
then
	mkdir -p substitution_parameters
	python paramgenerator/generateparams.py $LDBC_SOCIALNET_DBGEN_HOME substitution_parameters/
  	rm -f m0factors.txt
	rm -f m0friendList*
fi
