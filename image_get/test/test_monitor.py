from test.mocks import FakeRedis
from crawl_monitor.rate_limit import rate_limit_regulator, compute_crawl_rate,\
    MAX_CRAWL_RPS, MIN_CRAWL_RPS, MAX_CRAWL_SIZE
from test.mocks import FakeRedis, FakeAioSession, FakeAioResponse
import pytest
import asyncio


def test_crawl_rates():
    low_crawl = compute_crawl_rate(1)
    assert low_crawl == MIN_CRAWL_RPS
    big_crawl = compute_crawl_rate(1000000000)
    assert big_crawl == MAX_CRAWL_RPS
    medium_crawl = compute_crawl_rate(MAX_CRAWL_SIZE / 2)
    tolerance = 1
    assert abs(medium_crawl - (MAX_CRAWL_RPS / 2)) < tolerance


@pytest.mark.asyncio
async def test_rate_regulation():
    sources_endpoint_mock = [
        {
            "source_name": "example",
            "image_count": 10000,
            "display_name": "Example",
            "source_url": "example.com"
        }
    ]
    response = FakeAioResponse(status=200, body=sources_endpoint_mock)
    session = FakeAioSession(response=response)
    redis = FakeRedis()
    regulator_task = asyncio.create_task(rate_limit_regulator(session, redis))
    await asyncio.wait_for(regulator_task, timeout=2)
    assert 'currtokens:example' in redis.store
