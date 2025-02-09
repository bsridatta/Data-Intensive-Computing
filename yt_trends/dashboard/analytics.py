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

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType


def saveData(videos):

    if videos.count != 0:
        df = spark.read.json(videos)
        # Empty RDD or some error leads to missing columns
        if (len(df.head(1)) > 0 and len(df.columns) == 11):
            # Just taking likes for now
            df = df.select("channelTitle", "likeCount") 
            df = df.withColumn("likeCount", df["likeCount"].cast(IntegerType()))
            print("new feeds :", df.count()," Elements",len(df.columns))
            df.printSchema()
            # Save to spark warehouse to be used for visualization
            df.write.mode("append").saveAsTable("default.yt_viz")

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
    ssc = StreamingContext(sc, 10)
    
    spark = (SparkSession
             .builder
             .getOrCreate())

    # Essential to start writing to the table already created
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    print("*Streaming*")

    # Only the json is send as message without key hence just take the value
    kafka_messages = KafkaUtils.createStream(
        ssc, zkQuorum, "youtube_stream_consumer", {topic: 1}).map(lambda x: x[1])

    # As the data is serialized we need to parse back from JSON
    kafka_messages.foreachRDD(saveData)

    ssc.start()
    ssc.awaitTermination()
