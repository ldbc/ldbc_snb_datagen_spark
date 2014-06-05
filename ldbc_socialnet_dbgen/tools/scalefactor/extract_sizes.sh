#!/bin/bash

DATA_GENERATOR=/scratch/aprat/generador/ldbc_socialnet_bm/ldbc_socialnet_dbgen
#NUM_USERS="10000 20000 50000 80000 100000 250000 500000"
NUM_USERS="80000 100000 250000 500000"
SIZE_STATISTICS_DIR=/home/aprat
HADOOP_DIR=/home/aprat/hadoop-1.2.1
OUTPUT_DIR=/scratch/aprat/generador


cd $DATA_GENERATOR 
#rm $SIZE_STATISTICS_DIR/size_statistics.txt
for num_users in $NUM_USERS
do 
	echo "numPersons:$num_users" > $DATA_GENERATOR/params.ini
	echo "startYear:2010" >> $DATA_GENERATOR/params.ini
	echo "numYears:3" >> $DATA_GENERATOR/params.ini
	echo "serializerType:csv" >> $DATA_GENERATOR/params.ini
	echo "enableCompression:false" >> $DATA_GENERATOR/params.ini
	sh run.sh
	$HADOOP_DIR/bin/hadoop fs -rm $OUTPUT_DIR/social_network/*.json
	SIZE=$($HADOOP_DIR/bin/hadoop fs -dus $OUTPUT_DIR/social_network | cut -f 2)
	echo "$num_users  $SIZE" >> $SIZE_STATISTICS_DIR/size_statistics.txt
done 




