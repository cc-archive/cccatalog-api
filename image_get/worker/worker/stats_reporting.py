import time

# Error windows
ERRS_60s = 'err60s:'
ERRS_1hr = 'err1hr:'
ERRS_12hr = 'err12hr:'
# Total number of errors in resize tasks
ERROR_COUNT = 'resize_errors'
TLD_ERRORS = 'resize_errors_{}'


async def register_error(redis, tld):
    now = time.monotonic()
    async with await redis.pipeline() as pipe:
        pipe.incr(f'{ERROR_COUNT}')
        pipe.incr(TLD_ERRORS.format(tld))
        pipe.rpush(f'{ERRS_60s}{tld.domain}.{tld.suffix}', now)
        pipe.rpush(f'{ERRS_1hr}{tld.domain}.{tld.suffix}', now)
        pipe.rpush(f'{ERRS_12hr}{tld.domain}.{tld.suffix}', now)
        await pipe.execute()
