import asyncio
import aredis
import aiohttp
import logging as log
import crawl_monitor.settings as settings
from crawl_monitor.rate_limit import rate_limit_regulator
from crawl_monitor.structured_logging import log_state


async def monitor():
    session = aiohttp.ClientSession()
    redis = aredis.StrictRedis(host=settings.REDIS_HOST)
    # For sharing information between rate limit regulator and monitoring system
    info = {}
    regulator = asyncio.create_task(rate_limit_regulator(session, redis, info))
    structured_logger = asyncio.create_task(log_state(redis, info))
    await asyncio.wait([regulator, structured_logger])


if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    asyncio.run(monitor())
