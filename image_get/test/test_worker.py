import json
import pytest
import asyncio
import logging as log
import concurrent.futures
from worker.util import MetadataProducer
from test.mocks import FakeConsumer, FakeAioSession, FakeRedis,\
    AioNetworkSimulatingSession, FakeProducer
from worker.stats_reporting import StatsManager
from worker.image import process_image
from worker.rate_limit import RateLimitedClientSession
from PIL import Image
from functools import partial


log.basicConfig(level=log.DEBUG)


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


@pytest.fixture
@pytest.mark.asyncio
async def producer_fixture():
    # Run a processing task and capture the metadata results in a mock kafka
    # producer
    redis = FakeRedis()
    stats = StatsManager(redis)
    kafka = FakeProducer()
    producer = MetadataProducer(kafka)
    await process_image(
        persister=validate_thumbnail,
        session=RateLimitedClientSession(FakeAioSession(), redis),
        url='https://example.gov/hello.jpg',
        identifier='4bbfe191-1cca-4b9e-aff0-1d3044ef3f2d',
        stats=stats,
        source='example',
        semaphore=asyncio.BoundedSemaphore(1000),
        metadata_producer=producer
    )
    producer_task = asyncio.create_task(producer.listen())
    try:
        await asyncio.wait_for(producer_task, 0.01)
    except concurrent.futures.TimeoutError:
        pass
    return kafka


def test_resolution_messaging(producer_fixture):
    resolution_msg = producer_fixture.messages[0]
    parsed = json.loads(str(resolution_msg, 'utf-8'))
    expected_fields = ['height', 'width', 'identifier']
    for field in expected_fields:
        assert field in parsed


def test_exif_messaging(producer_fixture):
    exif_msg = producer_fixture.messages[1]
    parsed = json.loads(str(exif_msg, 'utf-8'))
    artist_key = '0x13b'
    assert parsed['exif'][artist_key] == 'unknown'
