import time

# Error windows
ERRS_60s = 'err60s:'
ERRS_1hr = 'err1hr:'
ERRS_12hr = 'err12hr:'
# Total number of errors in resize tasks
ERROR_COUNT = 'resize_errors'
# Errors per domain
TLD_ERRORS = 'resize_errors:'

# Total number of successful resizes
SUCCESS = 'num_resized'
# Successful resizes per domain
SUCCESS_TLD = 'num_resized:'


class StatsManager:
    def __init__(self, redis):
        self.redis = redis

    async def record_error(self, tld):
        now = time.monotonic()
        async with await self.redis.pipeline() as pipe:
            pipe.incr(ERROR_COUNT)
            pipe.incr(f'{TLD_ERRORS}{tld.domain}.{tld.suffix}')
            pipe.zadd(f'{ERRS_60s}{tld.domain}.{tld.suffix}', now, 1)
            pipe.zadd(f'{ERRS_1hr}{tld.domain}.{tld.suffix}', now, 1)
            pipe.zadd(f'{ERRS_12hr}{tld.domain}.{tld.suffix}', now, 1)
            await pipe.execute()

    async def record_success(self, tld):
        async with await self.redis.pipeline() as pipe:
            pipe.incr(SUCCESS)
            pipe.incr(f'{SUCCESS_TLD}{tld.domain}.{tld.suffix}')
            await pipe.execute()
