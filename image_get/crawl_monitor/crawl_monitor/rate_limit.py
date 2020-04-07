import asyncio
import time
import json
import logging as log
from aiohttp.client import ClientSession

# Crawl sizes below the minimum get the minimum crawl rate.
# Crawl sizes above the maximum get the maximum crawl rate.
# Everything inbetween is interpolated between the min and max.
HALF_HOUR_SEC = 60 * 30
MIN_CRAWL_SIZE = 5000
MAX_CRAWL_SIZE = 500000000

MIN_CRAWL_RPS = 0.1
MAX_CRAWL_RPS = 200

# Key for token replenishment
CURRTOKEN_PREFIX = 'currtokens:'
# The override rate limit for a domain, which takes precedence over all
# other rate limit settings.
OVERRIDE_PREFIX = 'override_rate:'

# Redis set containing all sources which we have halted crawling.
HALTED_SET = 'halted'


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


async def get_crawl_sizes(session: ClientSession):
    """
    Query our API to find out how many items each source has. The idea is that
    sources with more content have better capabilities to handle heavy request
    loads.
    """
    log.info('Updating crawl rate')
    endpoint = 'https://api.creativecommons.engineering/v1/sources'
    results = await session.get(endpoint)
    sources = await results.json()
    crawl_rates = {}
    for src in sources:
        source_name = src['source_name']
        size = src['image_count']
        crawl_rates[source_name] = compute_crawl_rate(size)

    return crawl_rates


async def get_overrides(sources, redis):
    """ Check if any rate limit overrides have been set. """
    sources = list(sources.keys())
    with redis.pipeline() as pipe:
        for source in sources:
            await pipe.get(f'{OVERRIDE_PREFIX}{source}')
        res = await pipe.execute()
    overrides = {}
    for idx, source in enumerate(sources):
        overrides[source] = res[idx]
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
    with redis.pipeline() as pipe:
        for source, rate in rates.items():
            token_key = f'{CURRTOKEN_PREFIX}{source}'
            # Rates below 1rps need replenishment deferred due to assorted
            # implementation details with crawl workers.
            if rate < 1:
                if source not in replenish_later:
                    replenish_later[source] = now + (1 / rate)
                    continue
                elif replenish_later[source] > now:
                    continue
                else:
                    del replenish_later[source]
                    await redis.set(token_key, 1)
                    continue
            await redis.set(token_key, rate)
        await pipe.execute()


async def rate_limit_regulator(session, redis):
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
    override_check_frequency = 30
    rate_limits = {}
    replenish_later = {}
    while True:
        now = time.monotonic()

        time_since_crawl_size_check = now - last_crawl_size_check
        if time_since_crawl_size_check > crawl_size_check_frequency:
            rate_limits = await get_crawl_sizes(session)
            overrides = await get_overrides(rate_limits, redis)
            rate_limits.update(overrides)
            last_crawl_size_check = now
            last_override_check = now

        time_since_override_check = now - last_override_check
        if time_since_override_check > override_check_frequency:
            overrides = await get_overrides(rate_limits, redis)
            rate_limits.update(overrides)
            last_override_check = now

        await replenish_tokens(replenish_later, rate_limits, redis)
        await asyncio.sleep(1)
