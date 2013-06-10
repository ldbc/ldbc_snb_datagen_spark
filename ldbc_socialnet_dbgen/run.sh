export HADOOP_HOME=/home/user/hadoop-1.0.3 #change to your hadoop folder
export LDBC_SOCIALNET_DBGEN_HOME=/home/user/ldbc_socialnet_bm/ldbc_socialnet_dbgen #change to your ldbc_socialnet_dbgen folder
export NUM_MACHINES=1

mkdir $LDBC_SOCIALNET_DBGEN_HOME/outputDir
mvn clean
mvn assembly:assembly

# para borrar ficheros temporales
cp $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar $LDBC_SOCIALNET_DBGEN_HOME/
rm $LDBC_SOCIALNET_DBGEN_HOME/target/ldbc_socialnet_dbgen.jar

$HADOOP_HOME/bin/hadoop jar $LDBC_SOCIALNET_DBGEN_HOME/ldbc_socialnet_dbgen.jar input/sib output/sib $NUM_MACHINES $LDBC_SOCIALNET_DBGEN_HOME/ $LDBC_SOCIALNET_DBGEN_HOME/outputDir/

