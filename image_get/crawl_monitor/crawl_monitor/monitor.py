import asyncio
import aredis
import aiohttp
import logging as log
import crawl_monitor.settings as settings
from crawl_monitor.rate_limit import rate_limit_regulator


async def log_state():
    while True:
        await asyncio.sleep(settings.LOG_FREQUENCY_SECONDS)


async def monitor():
    session = aiohttp.ClientSession()
    redis = aredis.StrictRedis(host=settings.REDIS_HOST)
    regulator = asyncio.create_task(rate_limit_regulator(session, redis))
    structured_logger = asyncio.create_task(log_state())
    await asyncio.wait([regulator, structured_logger])


if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG)
    asyncio.run(monitor())
