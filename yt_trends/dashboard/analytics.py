from youtube.bridge import YouTubeHandler

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == __main__:

    zkQuorum = "localhost:2181"
    topic = "youtube_stream"

    sc = SparkContext("local[*]", appName="yt_trends")
    # batch interval
    # Note: The youtube data is updated every n seconds as well
    ssc = StreamingContext(sc, 3)

    kafka_messages = KafkaUtils.createStream(ssc, zkQuorum, "youtube_stream_consumer", {topic: 1})

    print(kafka_messages)