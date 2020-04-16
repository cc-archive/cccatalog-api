import time
"""
Reports the status of image processing jobs by publishing statistics to Redis.

Description of keys:

We want to know what proportion of requests failed over several time intervals.
These are tracked through window functions:
status60s:{domain} - Events that occurred in the last 60 seconds
status1hr:{domain} - Events that occurred in the last hour
status12hr:{domain} - Events that occurred in the last 12 hours
statuslast50req:{domain} - The last 50 events over any time interval.

We also want to know the overall progress of the crawl and have a general
idea of which domains are having problems:
resize_errors - Number of errors that have occurred across all domains
resize_errors:{domain} - Number of errors that have occurred for a domain
num_resized - Number of successfully resized images
num_resized:{domain} - Number of successfully resized images for a domain

Domains are formatted as the TLD and suffix.
Valid example: status60s:staticflickr.com
Invalid example: status60s:https://staticflickr.com
"""

STATUS_60s = 'status60s:'
STATUS_1HR = 'status1hr:'
STATUS_12HR = 'status12hr:'
LAST_50_REQUESTS = 'statuslast50req:'

# Window intervals in seconds
ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
TWELVE_HOURS = ONE_HOUR * 12

WINDOW_PAIRS = [
    (STATUS_60s, ONE_MINUTE),
    (STATUS_1HR, ONE_HOUR),
    (STATUS_12HR, TWELVE_HOURS)
]

ERROR_COUNT = 'resize_errors'
SPECIFIC_ERRORS = 'resize_errors:'

SUCCESS_COUNT = 'num_resized'
SPECIFIC_SUCCESS = 'num_resized:'

KNOWN_SOURCES = 'known_sources'


class StatsManager:
    def __init__(self, redis):
        self.redis = redis
        self.known_sources = set()

    @staticmethod
    async def _record_window_samples(pipe, source, status):
        """ Insert a status into all sliding windows. """
        now = time.monotonic()
        # Time-based sliding windows
        for stat_key, interval in WINDOW_PAIRS:
            key = f'{stat_key}{source}'
            await pipe.zadd(key, now, f'{status}:{time.monotonic()}')
            # Delete events from outside the window
            await pipe.zremrangebyscore(key, '-inf', now - interval)
        # "Last n requests" window
        await pipe.rpush(f'{LAST_50_REQUESTS}{source}', status)
        await pipe.ltrim(f'{LAST_50_REQUESTS}{source}', -50, -1)

    async def record_error(self, source, code):
        """
        :param tld: The domain key for the associated URL.
        :param code: An optional status code.
        """
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(ERROR_COUNT)
            await pipe.incr(f'{SPECIFIC_ERRORS}{source}')
            await pipe.incr(f'{SPECIFIC_ERRORS}{source}:{code}')
            await self._record_window_samples(pipe, source, code)
            await pipe.execute()

    async def record_success(self, source):
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(SUCCESS_COUNT)
            await pipe.incr(f'{SPECIFIC_SUCCESS}{source}')
            await self._record_window_samples(pipe, source, status=200)
            await pipe.execute()
