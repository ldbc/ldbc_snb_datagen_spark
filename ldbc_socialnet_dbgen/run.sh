#!/bin/bash
export HADOOP_HOME=/home/user/hadoop-1.0.3 #change to your hadoop folder
export LDBC_SOCIALNET_DBGEN_HOME=/home/user/ldbc_socialnet_bm/ldbc_socialnet_dbgen #change to your ldbc_socialnet_dbgen folder 
export NUM_MACHINES=1

export SOCIALNET_DIR=/home/user/datasetFolder #change to the folder where the generated data should be written
export OUTPUT_DIR=$SOCIALNET_DIR/output
export SIB_INPUT_DIR=input/sib
export SIB_OUTPUT_DIR=$OUTPUT_DIR/sib
export DATA_OUTPUT_DIR=$SOCIALNET_DIR/outputDir/ #FIX: this trailing "/" should not be necessary

rm -rf $OUTPUT_DIR
rm -rf $DATA_OUTPUT_DIR
mkdir $OUTPUT_DIR
mkdir $DATA_OUTPUT_DIR
mvn clean
mvn assembly:assembly

cp $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar $LDBC_SOCIALNET_DBGEN_HOME/
rm $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar

$HADOOP_HOME/bin/hadoop jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar $SIB_INPUT_DIR $SIB_OUTPUT_DIR $NUM_MACHINES $LDBC_SOCIALNET_DBGEN_HOME/ $DATA_OUTPUT_DIR