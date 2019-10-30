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

global df_stream

def getTrendingTags(videos):
    if videos.count != 0:
        df = spark.read.json(videos)
      
        if (len(df.head(1)) > 0):
            df_new = df.exceptAll(df_stream)
            df_stream = df_stream.union(df)
            print("new feeds :", df_new.count())
            df_new.write.mode("append").saveAsTable("default.yt_viz")

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
    ssc = StreamingContext(sc, 2)
    
    spark = (SparkSession
             .builder
             .getOrCreate())

    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    print("*Streaming*")

    stream_schema = StructType([
                    StructField("categoryId",StringType(),True),
                    StructField("channelId",StringType(),True),
                    StructField("channelTitle",StringType(),True),
                    StructField("commentCount",StringType(),True),
                    StructField("dislikeCount",StringType(),True),
                    StructField("favoriteCount",StringType(),True),
                    StructField("id",StringType(),True),
                    StructField("likeCount",StringType(),True),
                    StructField("publsedAt",StringType(),True),
                    StructField("title",StringType(),True),
                    StructField("viewCount",StringType(),True)
    ])
    
    df_stream = spark.createDataFrame(sc.emptyRDD(), stream_schema)

    # Only the json is send as message without key hence just take the value
    kafka_messages = KafkaUtils.createStream(
        ssc, zkQuorum, "youtube_stream_consumer", {topic: 1}).map(lambda x: x[1])

    kafka_messages.foreachRDD(getTrendingTags)
    # kafka_messages.mapWithState(StateSpe)

    ssc.start()
    ssc.awaitTermination()
