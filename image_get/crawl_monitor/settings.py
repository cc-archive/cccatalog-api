import os
from collections import namedtuple

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')

# How frequently to dump stats to logs
LOG_FREQUENCY_SECONDS = 5
