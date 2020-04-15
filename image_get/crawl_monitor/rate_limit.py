import asyncio
import datetime
import json
import time
import logging as log
from collections import Counter
from aiohttp.client import ClientSession

# Crawl sizes below the minimum get the minimum crawl rate.
# Crawl sizes above the maximum get the maximum crawl rate.
# Everything between is interpolated.

HALF_HOUR_SEC = 60 * 30
MIN_CRAWL_SIZE = 5000
MAX_CRAWL_SIZE = 500000000

MIN_CRAWL_RPS = 0.2
MAX_CRAWL_RPS = 200

# Key for token replenishment
CURRTOKEN_PREFIX = 'currtokens:'
# The override rate limit for a domain, which takes precedence over all
# other rate limit settings besides error detection.
OVERRIDE_PREFIX = 'override_rate:'

# Redis set containing all sources which we have halted crawling.
# Once put into HALTED_SET, an operator must manually remove it
# before crawling resumes.
HALTED_SET = 'halted'

# Redis set containing temporarily halted crawl sources. Once error
# thresholds drop, crawling resumes.
TEMP_HALTED_SET = 'temp_halted'
# Crawls get temporarily halted if they exceed this threshold.
ERROR_TOLERANCE_PERCENT = 10
# Statuses that we expect to happen during normal operation that should not
# trip error circuit breakers.
EXPECTED_STATUSES = {
    '200',
    '404',
    '301',
    'UnidentifiedImageError'
}

# Set tracking the domains we are crawling
SOURCES = 'sources'


def compute_crawl_rate(crawl_size):
    """
    Set crawl rate in proportion to the size of a source.

    :param crawl_size: The size (in terms of pages/images) of the domain
    :return: The requests per second to crawl
    """
    if crawl_size >= MAX_CRAWL_SIZE:
        crawl_rate = MAX_CRAWL_RPS
    elif crawl_size <= MIN_CRAWL_SIZE:
        crawl_rate = MIN_CRAWL_RPS
    else:
        # Interpolate between min and max crawl rate
        size_diff = MAX_CRAWL_SIZE - MIN_CRAWL_SIZE
        rate_diff = MAX_CRAWL_RPS - MIN_CRAWL_RPS
        size_percent = crawl_size / size_diff
        crawl_rate = MIN_CRAWL_RPS + (rate_diff * size_percent)
    return min(MAX_CRAWL_RPS, crawl_rate)


async def recompute_crawl_rates(session: ClientSession):
    """
    Query our API to find out how many items each source has. The idea is that
    sources with more content have better capabilities to handle heavy request
    loads.
    """
    log.info('Updating crawl rate')
    endpoint = 'https://api.creativecommons.engineering/v1/sources'
    results = await session.get(endpoint)
    if results.status != 200:
        log.warning(
            'Failed to update crawl sizes: could not reach CC Catalog API. '
            'The last known crawl rates will be used.'
        )
        return None
    sources = await results.json()
    crawl_rates = {}
    for src in sources:
        source_name = src['source_name'].lower()
        size = src['image_count']
        rate = compute_crawl_rate(size)
        crawl_rates[source_name] = rate
        crawl_days = (size / rate) / 60 / 60 / 24
        log.debug(f'source, crawl days {source_name}, {crawl_days}')
    return crawl_rates


def _within_error_window_threshold(window):
    """
    Check that the response codes in an error window do not
    exceed our threshold.
    :param window: A list of response codes
    :return A boolean
    """
    if len(window) <= 5:
        # Not enough samples to measure
        return True
    errors = 0
    successful = 0
    for status in window:
        status, _ = str(status, 'utf-8').split(':')
        if status not in EXPECTED_STATUSES:
            errors += 1
        else:
            successful += 1
    tolerance = ERROR_TOLERANCE_PERCENT / 100
    if not successful or errors / successful > tolerance:
        return False
    else:
        return True


def _every_request_failed(statuses):
    errors = 0
    successful = 0
    for status in statuses:
        if str(status, 'utf-8') not in EXPECTED_STATUSES:
            errors += 1
        else:
            successful += 1
    return not bool(successful)


async def check_error_thresholds(sources, redis):
    """
    If crawlers are reporting too many errors, halt the crawl.
    """
    log.debug('Checking error thresholds')
    now = time.monotonic()
    for source in sources:
        one_minute_window_key = f'status60s:{source}'
        last_50_statuses_key = f'statuslast50req:{source}'
        await redis.zremrangebyscore(one_minute_window_key, '-inf', now - 60)
        one_minute_window = await redis.zrangebyscore(
            one_minute_window_key, '-inf', 'inf'
        )
        last_50_statuses = await redis.lrange(last_50_statuses_key, 0, -1)

        if _within_error_window_threshold(one_minute_window):
            await redis.srem(TEMP_HALTED_SET, source)
        else:
            await redis.sadd(TEMP_HALTED_SET, source)
            responses = [str(res).split(':')[0] for res in one_minute_window]
            response_counts = dict(Counter(responses))
            msg = f'{source} tripped temporary halt.' \
                  f' Response codes: {response_counts}'
            _log_halt_event(source, 'temporary', msg)

        status_samples = len(last_50_statuses)
        if status_samples >= 50 and _every_request_failed(last_50_statuses):
            await redis.sadd(HALTED_SET, source)
            response_counts = dict(Counter(last_50_statuses))
            msg = f'{source} tripped serious halt circuit breaker;' \
                  f' manual intervention required. ' \
                  f'Response codes: {response_counts}'
            _log_halt_event(source, 'permanent', msg)
    log.debug(f'Checked error thresholds in {time.monotonic() - now}')


async def get_overrides(sources, redis):
    """ Check if any rate limit overrides have been set. """
    log.debug('Checking overrides')
    sources = list(sources.keys())
    async with await redis.pipeline() as pipe:
        for source in sources:
            await pipe.get(f'{OVERRIDE_PREFIX}{source}')
        res = await pipe.execute()
        overrides = {}
        for idx, source in enumerate(sources):
            if res[idx]:
                overrides[source] = float(res[idx])
        return overrides


async def replenish_tokens(replenish_later, rates: dict, redis):
    """
    Replenish the token bucket for each domain in the `rates`
    dictionary.
    :param replenish_later: A dictionary used to determine when we need
    to replenish sub-1rps token buckets.
    :param rates: A dictionary mapping a source name to its rate limit.
    :param redis: A redis instance
    """
    now = time.monotonic()
    halted = {str(x, 'utf-8') for x in await redis.smembers(HALTED_SET)}
    temp_halted = \
        {str(x, 'utf-8') for x in await redis.smembers(TEMP_HALTED_SET)}
    if halted or temp_halted:
        log.debug(f'halted: {halted}')
        log.debug(f'temp_halted: {temp_halted}')
    async with await redis.pipeline() as pipe:
        for source, rate in rates.items():
            tokens = rate
            token_key = f'{CURRTOKEN_PREFIX}{source}'

            # Rates below 1rps need replenishment deferred due to assorted
            # implementation details with crawl workers.
            if 0 < rate < 1:
                if source not in replenish_later:
                    replenish_later[source] = now + (1 / rate)
                    tokens = 0
                elif replenish_later[source] > now:
                    tokens = 0
                else:
                    del replenish_later[source]
                    tokens = 1
            if source in halted or source in temp_halted:
                tokens = 0
            log.debug(f'source, ratelimit, tokens:'
                      f' {source},'
                      f' {rate},'
                      f' {tokens}')
            if tokens:
                await redis.set(token_key, int(tokens))
        await pipe.execute()


async def rate_limit_regulator(session, redis, info=None):
    """
    Regulate the rate limit of each data source.

    Rate limits are determined by crawl size. Optionally, an operator
    can override the automatically generated rate limits. If a spike in errors
    occurs on a domain, halt the crawl and report the event.

    Rate limits are enforced through token buckets for each domain.
    """
    last_crawl_size_check = float('-inf')
    crawl_size_check_frequency = HALF_HOUR_SEC

    last_override_check = float('-inf')
    override_check_frequency = 10
    auto_rate_limits = {}
    overridden_rate_limits = {}
    replenish_later = {}
    while True:
        now = time.monotonic()

        time_since_crawl_size_check = now - last_crawl_size_check
        if time_since_crawl_size_check > crawl_size_check_frequency:
            auto_rate_limits_check = await recompute_crawl_rates(session)
            if auto_rate_limits_check:
                auto_rate_limits = auto_rate_limits_check
            overrides = await get_overrides(auto_rate_limits, redis)
            overridden_rate_limits.update(auto_rate_limits)
            overridden_rate_limits.update(overrides)
            last_crawl_size_check = now
            last_override_check = now

        time_since_override_check = now - last_override_check
        if time_since_override_check > override_check_frequency:
            overrides = await get_overrides(auto_rate_limits, redis)
            overridden_rate_limits.update(auto_rate_limits)
            overridden_rate_limits.update(overrides)
            last_override_check = now

        await check_error_thresholds(overridden_rate_limits, redis)
        if info is not None:
            info['rates'] = overridden_rate_limits
        await replenish_tokens(replenish_later, overridden_rate_limits, redis)
        await asyncio.sleep(1)


def _log_halt_event(source, halt_type, msg):
    """

    :param source: The source being halted
    :param halt_type: 'temporary' or 'permanent'
    :param msg: Explanation for the operator
    """
    out = {
        'event': 'crawl_halted',
        'time': str(datetime.datetime.now().isoformat()),
        'msg': msg,
        'type': halt_type,
        'source': source
    }
    log.error(json.dumps(out))