#!/bin/bash 

export MY_HDFS_DIR=~/hadoopdata/hdfs
rm -rf $MY_HDFS_DIR
mkdir -p $MY_HDFS_DIR


# CAREFUL WHEN RUNNING LOCALLY
cd

rm -rf hadoop
tar xf hadoop-3.2.1.tar.gz

mv hadoop-3.2.1 hadoop
cd hadoop

# core-site.xml
sed -i "s|</configuration>|  <property><name>fs.default.name</name><value>hdfs://localhost:9000</value></property>\n</configuration>|" etc/hadoop/core-site.xml

# hdfs-site.xml
sed -i "s|</configuration>|  <property><name>dfs.replication</name><value>1</value></property>\n</configuration>|"                      etc/hadoop/hdfs-site.xml
sed -i "s|</configuration>|  <property><name>dfs.name.dir</name><value>file://$MY_HDFS_DIR/namenode</value></property>\n</configuration>|" etc/hadoop/hdfs-site.xml
sed -i "s|</configuration>|  <property><name>dfs.data.dir</name><value>file://$MY_HDFS_DIR/datanode</value></property>\n</configuration>|" etc/hadoop/hdfs-site.xml

# mapred-site.xml
#sed -i "s|</configuration>|  <property><name>mapred.child.java.opts</name><value>-Xmx12G</value></property>\n</configuration>|" etc/hadoop/mapred-site.xml
#sed -i "s|</configuration>|  <property><name>mapreduce.map.log.level</name><value>OFF</value></property>\n</configuration>|"    etc/hadoop/mapred-site.xml
#sed -i "s|</configuration>|  <property><name>mapreduce.reduce.log.level</name><value>OFF</value></property>\n</configuration>|" etc/hadoop/mapred-site.xml

sed -i "s|</configuration>|  <property><name>mapreduce.framework.name</name><value>yarn</value></property>\n</configuration>|"                             etc/hadoop/mapred-site.xml
sed -i "s|</configuration>|  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n</configuration>|" etc/hadoop/mapred-site.xml
sed -i "s|</configuration>|  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n</configuration>|"         etc/hadoop/mapred-site.xml
sed -i "s|</configuration>|  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n</configuration>|"      etc/hadoop/mapred-site.xml

# yarn-site.xml
sed -i "s|</configuration>|  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>\n</configuration>|" etc/hadoop/yarn-site.xml


# fill configuration files
# hadoop-env.sh
sed -i "s|# export JAVA_HOME=|export JAVA_HOME=$JAVA_HOME|"       etc/hadoop/hadoop-env.sh

#optional if you're /tmp is low on space
#mkdir hadoop-tmp
#sed -i "s|</configuration>|  <property><name>hadoop.tmp.dir</name><value>`pwd`/hadoop-tmp</value></property>\n</configuration>|" etc/hadoop/core-site.xml

sbin/stop-yarn.sh
sbin/stop-dfs.sh
sudo rm -rf /tmp/hadoop-*

echo 'Y' | bin/hdfs namenode -format
sbin/start-dfs.sh
sbin/start-yarn.sh
cd ..

# reduce clutter in the Hadoop output (n.b. setting log levels in mapred-conf.xml does not yield the expected result)
export HADOOP_LOGLEVEL=ERROR
