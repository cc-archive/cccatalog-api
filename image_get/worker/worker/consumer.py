import worker.settings as settings
import logging as log
import asyncio
import aiohttp
import aredis
import boto3
import botocore.client
import time
import statistics
from functools import partial
from timeit import default_timer as timer
from worker.util import kafka_connect, parse_message, save_thumbnail_s3,\
    process_image
from worker.rate_limit import RateLimitedClientSession


def poll_consumer(consumer, batch_size):
    """
    Poll the Kafka consumer for a batch of messages and parse them.
    :param consumer:
    :param batch_size: The number of events to return from the queue.
    :return:
    """
    batch = []
    # Consume messages until either batch_size has been reached or the max
    # wait time has occurred.
    max_wait_seconds = 3
    elapsed_time = 0
    last_msg_time = timer()
    msg_count = 0
    while msg_count < batch_size and elapsed_time < max_wait_seconds:
        message = consumer.consume(block=False)
        if message:
            parsed = parse_message(message)
            batch.append(parsed)
            last_msg_time = timer()
            msg_count += 1
        elapsed_time = timer() - last_msg_time
    return batch


async def monitor_task_list(tasks):
    last_time = time.monotonic()
    total_samples = 0
    total_rate = 0
    while True:
        now = time.monotonic()
        num_completed = sum([t.done() for t in tasks])
        task_delta = num_completed - last_count
        last_count = num_completed
        time_delta = now - last_time
        resize_rate = task_delta / time_delta
        last_time = now
        if resize_rate > 0:
            total_rate += resize_rate
            total_samples += 1
            mean = total_rate / total_samples
            log.info(f'resize_rate_1s={round(resize_rate, 2)}/s, '
                     f'avg_resize_rate={round(mean, 2)}/s, '
                     f'num_completed={num_completed}')
        await asyncio.sleep(1)


async def consume(consumer, image_processor, terminate=False):
    """
    Listen for inbound image URLs and process them.
    :param consumer: A Kafka consumer listening to the inbound images topic.
    :param image_processor: A partial function that handles an image.
    :param terminate: Whether to terminate when there are no more messages.
    """
    total = 0
    semaphore = asyncio.BoundedSemaphore(settings.BATCH_SIZE)
    scheduled = []
    asyncio.create_task(monitor_task_list(scheduled))
    while True:
        start = timer()
        messages = poll_consumer(consumer, settings.SCHEDULE_SIZE)
        # Schedule resizing tasks
        tasks = []
        for msg in messages:
            tasks.append(
                image_processor(
                    url=msg['url'],
                    identifier=msg['uuid'],
                    semaphore=semaphore
                )
            )
        if tasks:
            batch_size = len(tasks)
            total += batch_size
            for task in tasks:
                t = asyncio.create_task(task)
                scheduled.append(t)
            total_time = timer() - start
            log.info(f'event_processing_rate={batch_size/total_time}/s')
            consumer.commit_offsets()
        else:
            if terminate:
                await asyncio.gather(*scheduled)
                return
            await asyncio.sleep(10)


async def replenish_tokens(redis):
    """ """
    # Todo XXX delete this function; we need to automatically learn the rate limit
    while True:
        await redis.set('currtokens:staticflickr.com', 60)
        await redis.set('currtokens:example.gov', 60)
        await asyncio.sleep(1)


async def setup_consumer():
    """
    Set up all IO used by the consumer.
    """
    kafka_client = kafka_connect()
    s3 = boto3.client(
        's3',
        settings.AWS_DEFAULT_REGION,
        config=botocore.client.Config(max_pool_connections=settings.BATCH_SIZE)
    )
    inbound_images = kafka_client.topics['inbound_images']
    consumer = inbound_images.get_balanced_consumer(
        consumer_group='image_resizers',
        auto_commit_enable=True,
        zookeeper_connect=settings.ZOOKEEPER_HOST
    )

    # Todo: clean this up
    redis_client = aredis.StrictRedis(
        host=settings.REDIS_HOST
    )
    loop = asyncio.get_event_loop()
    loop.create_task(replenish_tokens(redis_client))

    aiosession = RateLimitedClientSession(
        aioclient=aiohttp.ClientSession(),
        redis=redis_client
    )
    image_processor = partial(
        process_image, session=aiosession,
        persister=partial(save_thumbnail_s3, s3_client=s3)
    )
    return consume(consumer, image_processor)


async def listen():
    """
    Listen for image events forever.
    """
    consumer = await setup_consumer()
    await consumer

if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    asyncio.run(listen())
