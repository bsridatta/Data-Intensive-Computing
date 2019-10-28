# Code inspiration and snippets from:
# spark streaming with youtube api
# https://github.com/Drathore0007/Spark-Streaming-with-YouTube-Data-API
import subprocess
import os
import sys
import time

import json

from youtube.bridge import *
from kafka import SimpleProducer, KafkaClient

def send_to_kafka(parsed_response):
    try:
        # all produce message payloads must be null or type bytes
        serialized_response = json.dumps(parsed_response)
        producer.send_messages('youtube_stream', serialized_response.encode('utf-8'))    
        return True

    except BaseException as e:
            print("Error Kafka producer: %s" % str(e))
    
    return True

if __name__ == '__main__':

    current_path = os.path.dirname(sys.argv[0])
    # TODO debug - Causing problems, producer does not work
    # Why? Automate Kafka and Zookeeper initialization
    subprocess.call(current_path+"/run_servers.bash")

    # Wait for the processes to start
    time.sleep(5)
    
    topic = 'youtube_stream'
    kafka = KafkaClient('localhost:9092')
    producer = SimpleProducer(kafka)

    yh = YouTubeHandler()

    while True:
        response = yh.request_videos()
        parsed_response = yh.parse_response(response)
        send_to_kafka(parsed_response)
        print("streaming..", len(parsed_response))
        time.sleep(3)
        
