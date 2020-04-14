import concurrent.futures
import pytest
import time
import asyncio
from crawl_monitor.rate_limit import rate_limit_regulator, compute_crawl_rate,\
    MAX_CRAWL_RPS, MIN_CRAWL_RPS, MAX_CRAWL_SIZE
from test.mocks import FakeRedis, FakeAioSession, FakeAioResponse


def test_crawl_rates():
    low_crawl = compute_crawl_rate(1)
    assert low_crawl == MIN_CRAWL_RPS
    big_crawl = compute_crawl_rate(1000000000)
    assert big_crawl == MAX_CRAWL_RPS
    medium_crawl = compute_crawl_rate(MAX_CRAWL_SIZE / 2)
    tolerance = 1
    assert abs(medium_crawl - (MAX_CRAWL_RPS / 2)) < tolerance


@pytest.fixture
def source_fixture():
    """ Mocks the /v1/sources endpoint response. """
    return [
        {
            "source_name": "example",
            "image_count": 5000000,
            "display_name": "Example",
            "source_url": "example.com"
        },
        {
            "source_name": "another",
            "image_count": 1000000,
            "display_name": "Another",
            "source_url": "whatever"
        }
    ]


async def run_regulator(regulator_task):
    try:
        await asyncio.wait_for(regulator_task, timeout=1)
    except concurrent.futures.TimeoutError:
        # expected
        pass


def regulator_with_api_not_reachable():
    response = FakeAioResponse(status=500, body=None)
    session = FakeAioSession(response=response)
    redis = FakeRedis()
    regulator_task = asyncio.create_task(rate_limit_regulator(session, redis))
    return redis, regulator_task


def create_mock_regulator(sources):
    response = FakeAioResponse(status=200, body=sources)
    session = FakeAioSession(response=response)
    redis = FakeRedis()
    regulator_task = asyncio.create_task(rate_limit_regulator(session, redis))
    return redis, regulator_task


@pytest.mark.asyncio
async def test_rate_override(source_fixture):
    sources = source_fixture
    redis, regulator_task = create_mock_regulator(sources)
    redis.store['override_rate:another'] = 10
    await run_regulator(regulator_task)
    assert redis.store['currtokens:example'] > 1
    assert redis.store['currtokens:another'] == 10


@pytest.mark.asyncio
async def test_handles_api_downtime_gracefully():
    # If we can't reach the API, we should continue crawling with only
    # overridden rates or the last known rates.
    redis, regulator_task = regulator_with_api_not_reachable()
    await run_regulator(regulator_task)
    assert True


@pytest.mark.asyncio
async def test_error_circuit_breaker(source_fixture):
    sources = source_fixture
    redis, regulator_task = create_mock_regulator(sources)
    redis.store['statuslast50req:example'] = [b'500'] * 50
    redis.store['statuslast50req:another'] = [b'200'] * 50
    await run_regulator(regulator_task)
    assert b'example' in redis.store['halted']
    assert b'another' not in redis.store['halted']


@pytest.mark.asyncio
async def test_temporary_halts(source_fixture):
    sources = source_fixture
    redis, regulator_task = create_mock_regulator(sources)
    one_second_ago = time.monotonic() - 1
    # 'example' should trip a temporary halt, but 'another' should not
    error_key = 'status60s:example'
    no_error_key = 'status60s:another'
    redis.store[error_key] = []
    redis.store[no_error_key] = []
    error_response = (one_second_ago, bytes(f'500:{one_second_ago}', 'utf-8'))
    successful_response = (one_second_ago, bytes(f'200:{one_second_ago}', 'utf-8'))
    for _ in range(3):
        redis.store[error_key].append(error_response)
    for _ in range(8):
        redis.store[error_key].append(error_response)
        redis.store[no_error_key].append(successful_response)
    await run_regulator(asyncio.shield(regulator_task))
    assert b'example' in redis.store['temp_halted']
    assert b'another' not in redis.store['temp_halted']
