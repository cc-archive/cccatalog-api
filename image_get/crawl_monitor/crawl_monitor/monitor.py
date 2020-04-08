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
    await rate_limit_regulator(session, redis)
    await log_state()


if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG)
    asyncio.run(monitor())
