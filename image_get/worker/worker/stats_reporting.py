import time

# Track errors that occurred in the last N {seconds | hours}
ERRS_60s = 'err60s:'
ERRS_1hr = 'err1hr:'
ERRS_12hr = 'err12hr:'
ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
TWELVE_HOURS = ONE_HOUR * 12

# Total number of errors in resize tasks
ERROR_COUNT = 'resize_errors'
# Errors per domain
TLD_ERRORS = 'resize_errors:'

# Total number of successful resizes
SUCCESS = 'num_resized'
# Successful resizes per domain
SUCCESS_TLD = 'num_resized:'

# Pair error window keys with intervals
WINDOW_PAIRS = [
    (ERRS_60s, ONE_MINUTE),
    (ERRS_1hr, ONE_HOUR),
    (ERRS_12hr, TWELVE_HOURS)
]


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
