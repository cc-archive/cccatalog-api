import json
import logging as log
import asyncio
import traceback
import crawl_monitor.settings as settings
from collections import Counter
from crawl_monitor.rate_limit import SOURCES


ERROR_COUNT = 'resize_errors'
SUCCESS_COUNT = 'num_resized'


def json_log(state):
    _json = json.dumps(state)
    log.info(_json)


def _decode_bytes_list(l):
    return [str(x, 'utf-8') for x in l]


def _decode(b):
    return str(b, 'utf-8')


def _parse_redis_int(n):
    if n is None:
        return 0
    else:
        return int(n)


async def log_state(redis):
    last_success_count = 0
    last_error_count = 0
    while True:
        sources = _decode_bytes_list(await redis.smembers(SOURCES))
        success_count = _parse_redis_int(await redis.get('num_resized'))
        error_count = _parse_redis_int(await redis.get('resize_errors'))
        success_delta = success_count - last_success_count
        error_delta = error_count - last_error_count
        success_rate = success_delta / settings.LOG_FREQUENCY_SECONDS
        error_rate = error_delta / settings.LOG_FREQUENCY_SECONDS
        last_success_count = success_count
        last_error_count = error_count
        state = {
            'general': {
                'num_resized': success_count,
                'resize_errors': error_count,
                'success_rate': success_rate,
                'error_rate': error_rate,
            },
            'specific': {}
        }
        # Add source-specific stats
        for source in sources:
            successful = _parse_redis_int(await redis.get(f'num_resized:{source}'))
            error = _parse_redis_int(await redis.get(f'resize_errors:{source}'))
            if successful <= 0:
                continue
            last_50 = Counter(_decode_bytes_list(
                await redis.lrange(f'statuslast50req:{source}', 0, -1)
            ))
            source_specifics = dict()
            source_specifics['successful'] = successful
            source_specifics['error'] = error
            source_specifics['last_50_statuses'] = last_50
            state['specific'][source] = source_specifics
        json_log(state)
        await asyncio.sleep(settings.LOG_FREQUENCY_SECONDS)
