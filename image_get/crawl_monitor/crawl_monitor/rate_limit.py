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


def get_crawl_rate(crawl_size):
    """
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
    return min(MAX_CRAWL_SIZE, crawl_rate)


async def update_crawl_sizes(session: ClientSession):
    log.info('Updating crawl rate')
    endpoint = 'https://api.creativecommons.engineering/v1/sources'
    results = await session.get(endpoint)
    sources = await results.json()
    crawl_rates = {}
    for src in sources:
        source_name = src['source_name']
        size = src['image_count']
        crawl_rates[source_name] = get_crawl_rate(size)

    return crawl_rates


async def get_overrides(sources, redis):
    """ Check if any rate limit overrides have been set."""
    sources = set(sources.keys())
    with redis.pipeline() as pipe:
        await pipe.get()


async def replenish_tokens(rates: dict, redis):
    """
    Replenish the token bucket for each domain in the `rates`
    dictionary.

    :param rates: A dictionary mapping a source name to its suggested
    rate limit.
    """
    with redis.pipeline() as pipe:
        for source, rate in rates.items():
            token_key = f'{CURRTOKEN_PREFIX}{source}'
            await redis.set(token_key, rate)
        await pipe.execute()


async def rate_limit_regulator(session, redis):
    """
    Regulate the rate limit of each data source.

    Rate limits are determined by crawl size. Optionally, an operator
    can override the automatically generated rate limits. If a spike in errors
    occurs on a domain, halt the crawl and report the event.

    Rate limits are enforced via token buckets for each domain.
    """
    # The last time we checked crawl sizes.
    last_rate_check = float('-inf')
    crawl_size_check_frequency = HALF_HOUR_SEC
    # The last time we checked rate limit overrides.
    last_override_check = float('-inf')
    override_check_frequency = 30
    target_rates = None
    while True:
        now = time.monotonic()

        time_since_rate_check = now - last_rate_check
        if time_since_rate_check > crawl_size_check_frequency:
            target_rates = await update_crawl_sizes(session)

        time_since_override_check = now - last_override_check
        if time_since_override_check > override_check_frequency:
            overrides = await get_overrides(target_rates, redis)
        await replenish_tokens(target_rates, redis)
        await asyncio.sleep(1)
