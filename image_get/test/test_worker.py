import json
import pytest
import asyncio
import logging as log
from worker.util import AsyncProducer
from test.mocks import FakeConsumer, FakeAioSession, FakeRedis,\
    AioNetworkSimulatingSession, FakeProducer
from worker.consumer import poll_consumer, consume
from worker.stats_reporting import StatsManager
from worker.image import process_image
from worker.rate_limit import RateLimitedClientSession
from PIL import Image
from functools import partial


log.basicConfig(level=log.DEBUG)


def test_poll():
    """ Test message polling and parsing."""
    consumer = FakeConsumer()
    msgs = [
        {
            'url': 'http://example.org',
            'uuid': 'c29b3ccc-ff8e-4c66-a2d2-d9fc886872ca',
            'source': 'example'
        },
        {
            'url': 'https://creativecommons.org/fake.jpg',
            'uuid': '4bbfe191-1cca-4b9e-aff0-1d3044ef3f2d',
            'source': 'example'
        }
    ]
    encoded_msgs = [json.dumps(msg) for msg in msgs]
    for msg in encoded_msgs:
        consumer.insert(msg)
    res = poll_consumer(consumer=consumer, batch_size=2)
    assert len(res) == 2


def validate_thumbnail(img, identifier):
    """ Check that the image was resized. """
    i = Image.open(img)
    width, height = i.size
    assert width <= 640 and height <= 480


@pytest.mark.asyncio
async def test_pipeline():
    """ Test that the image processor completes with a fake image. """
    # validate_thumbnail callback performs the actual assertions
    redis = FakeRedis()
    stats = StatsManager(redis)
    await process_image(
        persister=validate_thumbnail,
        session=RateLimitedClientSession(FakeAioSession(), redis),
        url='https://example.gov/hello.jpg',
        identifier='4bbfe191-1cca-4b9e-aff0-1d3044ef3f2d',
        stats=stats,
        source='example',
        semaphore=asyncio.BoundedSemaphore(1000)
    )
    assert redis.store['num_resized'] == 1
    assert redis.store['num_resized:example'] == 1
    assert len(redis.store['status60s:example']) == 1


@pytest.mark.asyncio
async def test_handles_corrupt_images_gracefully():
    redis = FakeRedis()
    stats = StatsManager(redis)
    await process_image(
        persister=validate_thumbnail,
        session=RateLimitedClientSession(FakeAioSession(corrupt=True), redis),
        url='fake_url',
        identifier='4bbfe191-1cca-4b9e-aff0-1d3044ef3f2d',
        stats=stats,
        source='example',
        semaphore=asyncio.BoundedSemaphore(1000)
    )


@pytest.mark.asyncio
async def test_records_errors():
    redis = FakeRedis()
    stats = StatsManager(redis)
    session = RateLimitedClientSession(FakeAioSession(status=403), redis)
    await process_image(
        persister=validate_thumbnail,
        session=session,
        url='https://example.gov/image.jpg',
        identifier='4bbfe191-1cca-4b9e-aff0-1d3044ef3f2d',
        stats=stats,
        source='example',
        semaphore=asyncio.BoundedSemaphore(1000)
    )
    expected_keys = [
        'resize_errors',
        'resize_errors:example',
        'resize_errors:example:403',
        'status60s:example',
        'status1hr:example',
        'status12hr:example'
    ]
    for key in expected_keys:
        val = redis.store[key]
        assert val == 1 or len(val) == 1


async def _replenish_tokens_10rps(redis):
    """ Replenish rate limit tokens at 10 requests per second. """
    while True:
        await redis.set('currtokens:flickr', 10)
        await redis.set('currtokens:example', 10)
        await asyncio.sleep(1)


async def get_mock_consumer(msg_count=1000, max_rps=10):
    """ Create a mock consumer with a bunch of fake messages in it. """
    consumer = FakeConsumer()
    msgs = [
        {
            'url': 'https://example.gov/hewwo.jpg',
            'uuid': '96136357-6f32-4174-b4ca-ae67e963bc55',
            'source': 'example'
        }
    ]*msg_count
    encoded_msgs = [json.dumps(msg) for msg in msgs]
    for msg in encoded_msgs:
        consumer.insert(msg)

    redis = FakeRedis()
    loop = asyncio.get_event_loop()
    loop.create_task(_replenish_tokens_10rps(redis))

    aiosession = RateLimitedClientSession(
        AioNetworkSimulatingSession(
            max_requests_per_second=max_rps,
            fail_if_overloaded=True
        ),
        redis=redis
    )
    stats = StatsManager(redis)
    producer = AsyncProducer(FakeProducer())
    image_processor = partial(
        process_image, session=aiosession,
        persister=validate_thumbnail,
        stats=stats,
        semaphore=asyncio.BoundedSemaphore(1000),
        metadata_producer=producer
    )
    return consume(consumer, image_processor, terminate=True)


async def mock_listen():
    consumer = await get_mock_consumer(msg_count=100, max_rps=11)
    log.debug('Starting consumer')
    await consumer


@pytest.mark.asyncio
async def test_rate_limiting():
    """
    Fails if we crawl aggressively enough to kill the simulated server.
    """
    await mock_listen()
