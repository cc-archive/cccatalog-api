import os
from collections import namedtuple

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')

# How frequently to dump stats to logs
LOG_FREQUENCY_SECONDS = 5

# Number of events to split into separate topics at once
SPLIT_SIZE = 20000

# Number of crawl events to schedule at once
SCHEDULE_SIZE = 20000