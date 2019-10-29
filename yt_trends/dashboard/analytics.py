"""
Integrate Kafka and Spark, launching the app by running
./yt_trends/youtube/launch_streaming.bash  
"""

import subprocess
from os.path import dirname
import time
import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession


def getTrendingTags(videos):
    if videos.count != 0:
        df = spark.read.json(videos)
        df.printSchema()
        df.write.saveAsTable('YT_Data.Stream')
    
    else:
        # Could also prevent at streaming
        # Either query is empty or quota is expired
        print("Empty RDD")


if __name__ == "__main__":

    zkQuorum = "localhost:2181"
    topic = "youtube_stream"

    sc = SparkContext("local[*]", appName="yt_trends")
    sc.setLogLevel("ERROR")

    # batch interval
    # Note: The youtube data is updated every n seconds as well
    ssc = StreamingContext(sc, 1)
    
    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .getOrCreate())

    print("*Streaming*")
    # Only the json is send as message without key hence just take the value
    kafka_messages = KafkaUtils.createStream(
        ssc, zkQuorum, "youtube_stream_consumer", {topic: 1}).map(lambda x: x[1])

    kafka_messages.foreachRDD(getTrendingTags)

    ssc.start()
    ssc.awaitTermination()
