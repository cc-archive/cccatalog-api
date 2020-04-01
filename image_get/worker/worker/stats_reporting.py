import time

# Error windows
ERRS_60s = 'err60s:'
ERRS_1hr = 'err1hr:'
ERRS_12hr = 'err12hr:'
# Total number of errors in resize tasks
ERROR_COUNT = 'resize_errors'
TLD_ERRORS = 'resize_errors_{}'

# Number of successful resizes
SUCCESS = 'num_resized'
SUCCESS_TLD = 'num_resized_{}'


async def record_error(redis, tld):
    now = time.monotonic()
    async with await redis.pipeline() as pipe:
        pipe.incr(ERROR_COUNT)
        pipe.incr(TLD_ERRORS.format(tld))
        pipe.rpush(f'{ERRS_60s}{tld.domain}.{tld.suffix}', now)
        pipe.rpush(f'{ERRS_1hr}{tld.domain}.{tld.suffix}', now)
        pipe.rpush(f'{ERRS_12hr}{tld.domain}.{tld.suffix}', now)
        await pipe.execute()


async def record_success(redis, tld):
    async with await redis.pipeline() as pipe:
        pipe.incr(SUCCESS)
        pipe.incr(SUCCESS_TLD.format(tld))
        await pipe.execute()
