"""
 Counts the messages received per second

 Note:  This script will only work outside of the Bluemix environment and
        with Kafka v 0.8.

 To execute locally:
    - From the Spark root directory...
    - The spark streaming kafka jar needs to in the "external" directory
    \> bin/spark-submit --jars \
          external/spark-streaming-kafka-assembly_2.10-1.6.0.jar \
          <path to this script>/message_count.py
"""
from __future__ import print_function

import os
import ConfigParser

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Config file example content:
#
# [kafka]
# topic=sample-topic
# zookeeper=localhost:2181

config_file = os.path.expanduser('~/.config')
config = ConfigParser.RawConfigParser()
config.read(config_file)

zookeeper = config.get('kafka', 'zookeeper')
topic = config.get('kafka', 'topic')

if not zookeeper or not topic:
    raise Exception('Cannot connect with Kafka.')

sc = SparkContext(appName='MessageCount')
ssc = StreamingContext(sc, 1)

stream = KafkaUtils.createStream(ssc, zookeeper, 'mc-consumer', {topic: 1})
lines = stream.map(lambda x: x[1])
counts = lines.count()
counts.pprint()

ssc.start()
ssc.awaitTermination()
