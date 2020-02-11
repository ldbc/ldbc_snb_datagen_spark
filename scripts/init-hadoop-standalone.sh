export HDFS_DIR=~/hadoopdata/hdfs
rm -rf $HDFS_DIR
mkdir -p $HDFS_DIR



# core-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>fs.default.name</name><value>hdfs://localhost:9000</value></property>|" etc/hadoop/core-site.xml

# hdfs-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>dfs.replication</name><value>1</value></property>|"                      etc/hadoop/hdfs-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>dfs.name.dir</name><value>file://$HDFS_DIR/namenode</value></property>|" etc/hadoop/hdfs-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>dfs.data.dir</name><value>file://$HDFS_DIR/datanode</value></property>|" etc/hadoop/hdfs-site.xml

# mapred-site.xml
#sed -i "s|<configuration>|<configuration>\n  <property><name>mapred.child.java.opts</name><value>-Xmx12G</value></property>|" etc/hadoop/mapred-site.xml
#sed -i "s|<configuration>|<configuration>\n  <property><name>mapreduce.map.log.level</name><value>OFF</value></property>|"    etc/hadoop/mapred-site.xml
#sed -i "s|<configuration>|<configuration>\n  <property><name>mapreduce.reduce.log.level</name><value>OFF</value></property>|" etc/hadoop/mapred-site.xml

sed -i "s|<configuration>|<configuration>\n  <property><name>mapreduce.framework.name</name><value>yarn</value></property>|"                             etc/hadoop/mapred-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>|" etc/hadoop/mapred-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>|"         etc/hadoop/mapred-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>|"      etc/hadoop/mapred-site.xml

# yarn-site.xml
sed -i "s|<configuration>|<configuration>\n  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>|" etc/hadoop/yarn-site.xml
