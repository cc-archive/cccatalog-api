from test.mocks import FakeRedis
from crawl_monitor.rate_limit import rate_limit_regulator
import pytest
import asyncio

@pytest.mark.asyncio
async def test_monitor():
    await asyncio.sleep(0.1)
    assert True
