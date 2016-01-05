#!/bin/bash
DEFAULT_HADOOP_HOME=/home/user/hadoop-2.6.0 #change to your hadoop folder
DEFAULT_LDBC_SNB_DATAGEN_HOME=/home/user/ldbc_snb_datagen #change to your ldbc_socialnet_dbgen folder
PARAM_GENERATION=1 #param generation

# allow overriding configuration from outside via environment variables
# i.e. you can do
#     HADOOP_HOME=/foo/bar LDBC_SNB_DATAGEN_HOME=/baz/quux ./run.sh
# instead of changing the contents of this file
HADOOP_HOME=${HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
LDBC_SNB_DATAGEN_HOME=${LDBC_SNB_DATAGEN_HOME:-$DEFAULT_LDBC_SNB_DATAGEN_HOME}

export HADOOP_HOME
export LDBC_SNB_DATAGEN_HOME

mvn clean
mvn -DskipTests assembly:assembly 

cp $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen.jar $LDBC_SNB_DATAGEN_HOME/
rm $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen.jar

$HADOOP_HOME/bin/hadoop  jar $LDBC_SNB_DATAGEN_HOME/ldbc_snb_datagen.jar $LDBC_SNB_DATAGEN_HOME/params.ini


if [ $PARAM_GENERATION -eq 1 ]
then
	mkdir -p substitution_parameters
	python paramgenerator/generateparams.py $LDBC_SNB_DATAGEN_HOME substitution_parameters/
	python paramgenerator/generateparamsbi.py $LDBC_SNB_DATAGEN_HOME substitution_parameters/
  rm -f m*personFactors*
  rm -f .m*personFactors*
  rm -f m*activityFactors*
  rm -f .m*activityFactors*
  rm -f m0friendList*
  rm -f .m0friendList*
fi
