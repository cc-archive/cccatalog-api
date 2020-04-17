import asyncio
import aredis
import aiohttp
import logging as log
import crawl_monitor.settings as settings
from crawl_monitor.rate_limit import rate_limit_regulator
from crawl_monitor.structured_logging import log_state
from crawl_monitor.source_splitter import SourceSplitter
from worker.util import kafka_connect
from multiprocessing import Process


async def monitor():
    session = aiohttp.ClientSession()
    redis = aredis.StrictRedis(host=settings.REDIS_HOST)
    # For sharing information between rate limit regulator and monitoring system
    info = {}
    regulator = asyncio.create_task(rate_limit_regulator(session, redis, info))
    structured_logger = asyncio.create_task(log_state(redis, info))
    await asyncio.wait([regulator, structured_logger])


def run_splitter():
    """
    Takes messages from the inbound_images topic and divides each source
    into its own queue for scheduling.
    """
    kafka_client = kafka_connect()
    inbound_images = kafka_client.topics['inbound_images']
    consumer = inbound_images.get_balanced_consumer(
        consumer_group='splitter',
        auto_commit_enable=True,
        zookeeper_connect=settings.ZOOKEEPER_HOST,
        use_rdkafka=True
    )
    splitter = SourceSplitter(kafka_client, consumer)
    splitter.split()


if __name__ == '__main__':
    p = Process(target=run_splitter)
    p.start()
    log.basicConfig(level=log.INFO)
    asyncio.run(monitor())
