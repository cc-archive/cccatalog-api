import os
from collections import namedtuple

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')

# How frequently to dump stats to logs
LOG_FREQUENCY_SECONDS = 5

# How rapidly rate limits should be adjusted to track the target.
# Ex: RateLimitDelta(1, 1) means 1 per second, while (1, 10) would be
# increasing once per 10 seconds.
RateLimitDelta = namedtuple(
    'RateLimitDelta', ['rate', 'time']
)
RATE_LIMIT_DELTA = RateLimitDelta(
    rate=os.getenv('rate_limit_delta_numerator', 1),
    time=os.getenv('rate_limit_delta_time', 1)
)
