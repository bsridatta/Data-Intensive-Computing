# Code inspiration from:
# spark streaming with youtube api
# https://github.com/Drathore0007/Spark-Streaming-with-YouTube-Data-API
import subprocess
from os.path import dirname
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

    # Automate Kafka and Zookeeper initialization
    root = dirname(dirname(sys.argv[0]))
    subprocess.call(root+"/server_setup/run_servers.bash")

    # Wait for the processes to start
    time.sleep(5)
    
    topic = 'youtube_stream'
    kafka = KafkaClient('localhost:9092')
    producer = SimpleProducer(kafka)

    yh = YouTubeHandler()

    while True:
        response = yh.request_videos(maxResults=1)
        if len(response) != 0:
            parsed_response = yh.parse_response(response)
            send_to_kafka(parsed_response)
            print("streaming...", len(parsed_response))
        else:
            print("streaming...", len(response))
            #send_to_kafka(response)

        # Remember only 10k requests per day
        time.sleep(30)
        
