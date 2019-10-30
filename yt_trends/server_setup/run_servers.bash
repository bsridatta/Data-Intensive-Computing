#! /usr/bin/env bash

# Can have all these in .bashrc or .bash_profile instead

# Kafka Path
export KAFKA_HOME="/home/datta/lab/_KTH_ACADEMIA/id2210/kafka_2.11-2.3.0"
export PATH=$KAFKA_HOME/bin:$PATH

# Start ZooKeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties


# create topic 
# $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic youtube_stream

# Restart the servers
# replace start with stop, if stop does not work 
# Killing Kafka  
# sudo kill -9 $(sudo lsof -t -i:9092)

# start Kafka producer
# $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic youtube_stream

# consumer
# $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic youtube_stream --from-beginning

# Hive
# export HIVE_HOME="/home/datta/lab/_KTH_ACADEMIA/id2210/apache-hive-3.1.2-bin"
# export PATH=$PATH:$HIVE_HOME/bin
