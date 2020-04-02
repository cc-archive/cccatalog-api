import time


"""
Reports the status of image processing jobs by publishing statistics to Redis.

Description of keys:

err60s:{domain} - Number of errors that occurred in the last 60 seconds
err1hr:{domain} - Number of errors that occurred in the last hour
err12hr:{domain} - Number of errors that occurred in the last 12 hours

resize_errors - Number of errors that have occurred across all domains
resize_errors:{domain} - Number of errors that have occurred for a domain

num_resized - Number of successfully resized images
num_resized:{domain} - Number of successfully resized images for a domain

Domains are formatted as the TLD and suffix.
Valid example: err60s:staticflickr.com
Invalid example: err60s:https://staticflickr.com
"""

ERRS_60s = 'err60s:'
ERRS_1hr = 'err1hr:'
ERRS_12hr = 'err12hr:'
ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
TWELVE_HOURS = ONE_HOUR * 12

WINDOW_PAIRS = [
    (ERRS_60s, ONE_MINUTE),
    (ERRS_1hr, ONE_HOUR),
    (ERRS_12hr, TWELVE_HOURS)
]

ERROR_COUNT = 'resize_errors'
TLD_ERRORS = 'resize_errors:'

SUCCESS = 'num_resized'
SUCCESS_TLD = 'num_resized:'


class StatsManager:
    def __init__(self, redis):
        self.redis = redis

    async def record_error(self, tld, code=None):
        now = time.monotonic()
        domain = f'{tld.domain}.{tld.suffix}'
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(ERROR_COUNT)
            await pipe.incr(f'{TLD_ERRORS}{domain}')
            if code:
                await pipe.incr(f'{TLD_ERRORS}{domain}:{code}')
            for err_key, interval in WINDOW_PAIRS:
                key = f'{err_key}{domain}'
                await pipe.zadd(key, now, now)
                await pipe.zremrangebyscore(key, '-inf', now - interval)
            await pipe.execute()

    async def record_success(self, tld):
        domain = f'{tld.domain}.{tld.suffix}'
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(SUCCESS)
            await pipe.incr(f'{SUCCESS_TLD}{domain}')
            await pipe.execute()
