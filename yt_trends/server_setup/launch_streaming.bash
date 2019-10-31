#! /usr/bin/env bash

# Hadoop
export HADOOP_CONFIG="$HADOOP_HOME/etc/hadoop"
export HADOOP_HOME="/home/datta/lab/_KTH_ACADEMIA/id2210/hadoop-3.1.2"
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# HBase
export HBASE_HOME="/home/datta/lab/_KTH_ACADEMIA/id2210/hbase-1.4.10"
export HBASE_CONF="$HBASE_HOME/conf"
export PATH=$HBASE_HOME/bin:$PATH

# Cassandra EXPERIMENT NOT USED
export CASSANDRA_HOME="/path/to/the/cassandra/folder"
export PYTHONPATH="/path/to/the/python/folder"
export PATH=$PYTHONPATH/bin:$CASSANDRA_HOME/bin:$PATH

# Spark
export SPARK_HOME="/home/datta/lab/_KTH_ACADEMIA/id2210/spark-2.4.3-bin-hadoop2.7"
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

CURR_DIR_PATH="$(realpath "${0}" | xargs dirname)"
ROOT="$(dirname "$CURR_DIR_PATH")"
echo "PATH analytivs-->"$ROOT

# Integrate pyspark and kafka
# Make sure the version numbers are right. Python is supported by kafka 8 and installed spark is 2.4.3
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 $ROOT/dashboard/analytics.py localhost:2181 youtube_stream
