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

def getTrendingTags(videos):
    print("videos")
    pass

def function_to_split_rows(records):
    if records.count() !=0:
        spark_dataframe = json.loads(records)
        spark_dataframe.show(2)
        spark_dataframe.write.insertInto('default.youtube_data', overwrite=False)
        print("its done")
    else:
        print("Empty  RDD")

if __name__ == "__main__":

    zkQuorum = "localhost:2181"
    topic = "youtube_stream"

    sc = SparkContext("local[*]", appName="yt_trends")
    # sc.setLogLevel("ERROR")
    # batch interval
    # Note: The youtube data is updated every n seconds as well
    ssc = StreamingContext(sc, 19)

    kafka_messages = KafkaUtils.createStream(ssc, zkQuorum, "youtube_stream_consumer", {topic: 1}).map(lambda x: x[1])
    kafka_messages.foreachRDD(function_to_split_rows)

    ssc.start()
    ssc.awaitTermination()