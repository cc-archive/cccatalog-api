import os
from collections import namedtuple

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
KAFKA_HOSTS = os.getenv('KAFKA_HOSTS', 'kafka:9092')
ZOOKEEPER_HOST = os.getenv('ZOOKEEPER_HOST', 'zookeeper:2181')

# How frequently to dump stats to logs
LOG_FREQUENCY_SECONDS = 5

# Number of events to split into separate topics at once
SPLIT_SIZE = 200000
