import time

# Error windows
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


class StatsManager:
    def __init__(self, redis):
        self.redis = redis

    async def _record_window(self, pipe, key, score, window):
        """
        Maintain a sliding window of error counts (e.g. errors that occurred
        in the last 60s, the last hour, and so on)
        :param pipe:  The pipe to execute Redis commands against.
        :param key: The name of the sliding window.
        :param score: The time the error occurred.
        :param window: Length of the window in seconds.
        :return:
        """
        await pipe.zadd(key, score, score)
        await pipe.zremrangebyscore(key, '-inf', score - window)

    async def record_error(self, tld, code=None):
        now = time.monotonic()
        domain = f'{tld.domain}.{tld.suffix}'
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(ERROR_COUNT)
            await pipe.incr(f'{TLD_ERRORS}{domain}')
            if code:
                await pipe.incr(f'{TLD_ERRORS}{domain}:{code}')
            await self._record_window(pipe, f'{ERRS_60s}{domain}', now, ONE_MINUTE)
            await self._record_window(pipe, f'{ERRS_1hr}{domain}', now, ONE_HOUR)
            await self._record_window(pipe, f'{ERRS_12hr}{domain}', now, TWELVE_HOURS)
            await pipe.execute()

    async def record_success(self, tld):
        domain = f'{tld.domain}.{tld.suffix}'
        async with await self.redis.pipeline() as pipe:
            await pipe.incr(SUCCESS)
            await pipe.incr(f'{SUCCESS_TLD}{domain}')
            await pipe.execute()
