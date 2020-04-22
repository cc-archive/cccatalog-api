import asyncio
import logging as log
import random
import time
from collections import deque
from enum import Enum, auto
from functools import partial


class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    def __init__(self):
        self.messages = []

    def insert(self, message):
        self.messages.append(
            FakeMessage(bytes(message, 'utf-8'))
        )

    def consume(self, block=True):
        if self.messages:
            return self.messages.pop()
        else:
            return None

    def commit_offsets(self):
        pass


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, msg):
        log.info(f'producing {msg}')
        self.messages.append(msg)


class FakeImageResponse:
    def __init__(self, status=200, corrupt=False):
        self.status = status
        self.corrupt = False

    async def read(self):
        # 1024 x 768 sample image with exif tag 'artist = unknown'
        if self.corrupt:
            location = 'test/test_worker.py'
        else:
            location = 'test/test_image.jpg'
        with open(location, 'rb') as f:
            return f.read()


class FakeAioResponse:
    def __init__(self, status, body):
        self.status = status
        self.body = body

    async def json(self):
        return self.body


class FakeAioSession:
    def __init__(self, corrupt=False, status=200, response=None):
        self.corrupt = corrupt
        self.status = status
        self.response = response

    async def get(self, url):
        if self.response:
            return self.response
        else:
            return FakeImageResponse(self.status, self.corrupt)


class FakeRedisPipeline:
    def __init__(self, redis):
        self.redis = redis
        # Deferred pipeline tasks
        self.todo = []

    async def rpush(self, key, value):
        self.todo.append(partial(self.redis.rpush, key, value))

    async def ltrim(self, key, start, end):
        self.todo.append(partial(self.redis.ltrim, start, end))

    async def incr(self, key):
        self.todo.append(partial(self.redis.incr, key))

    async def zadd(self, key, score, value):
        self.todo.append(partial(self.redis.zadd, key, score, value))

    async def zrangebyscore(self, key, start, end):
        self.todo.append(partial(self.redis.zrangebyscore, key, start, end))

    async def zremrangebyscore(self, key, start, end):
        self.todo.append(partial(self.redis.zremrangebyscore, key, start, end))

    async def get(self, key):
        return self.todo.append(partial(self.redis.get, key))

    async def __aexit__(self, exc_type, exc, tb):
        return self

    async def __aenter__(self):
        return self

    async def execute(self):
        results = []
        for task in self.todo:
            results.append(await task())
        return results


class FakeRedis:
    def __init__(self, *args, **kwargs):
        self.store = {}

    async def set(self, key, val):
        self.store[key] = val

    async def get(self, key):
        try:
            return self.store[key]
        except KeyError:
            return None

    async def decr(self, key):
        if key in self.store:
            self.store[key] -= 1
        else:
            self.store[key] = 1
        return self.store[key]

    async def rpush(self, key, value):
        if key not in self.store:
            self.store[key] = []
        self.store[key].append(value)

    async def incr(self, key):
        if key in self.store:
            self.store[key] += 0
        self.store[key] = 1
        return self.store[key]

    async def zadd(self, key, score, value):
        if key not in self.store:
            self.store[key] = []
        self.store[key].append((score, value))

    async def sadd(self, key, value):
        if key not in self.store:
            self.store[key] = set()
        self.store[key].add(bytes(value, 'utf8-'))

    async def srem(self, key, value):
        try:
            if key in self.store:
                self.store[key].remove(value)
        except KeyError:
            pass

    async def lrange(self, key, start, end):
        try:
            # Redis slices are slightly different than Python's
            if end == -1:
                return self.store[key][start:]
            else:
                return self.store[key][start:end]
        except KeyError:
            return []

    async def ltrim(self, key, start, end):
        pass

    async def smembers(self, key):
        try:
            return list(self.store[key])
        except KeyError:
            return []

    async def zremrangebyscore(self, key, start, end):
        if key not in self.store:
            return None
        # inefficiency tolerated because this is a mock
        start = float(start)
        end = float(end)
        delete_idxs = []
        for idx, tup in enumerate(self.store[key]):
            score, f = tup
            if start < score < end:
                delete_idxs.append(idx)
        for idx in reversed(delete_idxs):
            del self.store[key][idx]

    async def zrangebyscore(self, key, start, end):
        try:
            res = []
            for score, value in self.store[key]:
                res.append(value)
            return res
        except KeyError:
            return []

    async def pipeline(self):
        return FakeRedisPipeline(self)


class AioNetworkSimulatingSession:
    """
    It's a FakeAIOSession, but it can simulate network latency, errors,
    and congestion. At 80% of its max load, it will start to slow down and occasionally
    throw an error. At 100%, error rates become very high and response times slow.
    """

    class Load(Enum):
        LOW = auto()
        HIGH = auto()
        OVERLOADED = auto()

    # Under high load, there is a 1/5 chance of an error being returned.
    high_load_status_choices = [403, 200, 200, 200, 200]
    # When overloaded, there's a 4/5 chance of an error being returned.
    overloaded_status_choices = [500, 403, 501, 400, 200]

    def __init__(self, max_requests_per_second=10, fail_if_overloaded=False):
        self.max_requests_per_second = max_requests_per_second
        self.requests_last_second = deque()
        self.load = self.Load.LOW
        self.fail_if_overloaded = fail_if_overloaded
        self.tripped = False

    def record_request(self):
        """ Record a request and flush out expired records. """
        if self.requests_last_second:
            while (self.requests_last_second
                   and time.time() - self.requests_last_second[0] > 1):
                self.requests_last_second.popleft()
        self.requests_last_second.append(time.time())

    def update_load(self):
        original_load = self.load
        rps = len(self.requests_last_second)
        utilization = rps / self.max_requests_per_second
        if utilization <= 0.8:
            self.load = self.Load.LOW
        elif 0.8 < utilization < 1:
            self.load = self.Load.HIGH
        else:
            self.load = self.Load.OVERLOADED
            if self.fail_if_overloaded:
                assert False, f"You DDoS'd the server! Utilization: " \
                              f"{utilization}, reqs/s: {rps}"
        if self.load != original_load:
            log.debug(f'Changed simulator load status to {self.load}')

    def lag(self):
        """ Determine how long a request should lag based on load. """
        if self.load == self.Load.LOW:
            wait = random.uniform(0.05, 0.15)
        elif self.load == self.Load.HIGH:
            wait = random.uniform(0.15, 0.6)
        # Overloaded
        else:
            wait = random.uniform(2, 10)
        return wait

    async def get(self, url):
        self.record_request()
        self.update_load()
        await asyncio.sleep(self.lag())
        if self.load == self.Load.HIGH:
            status = random.choice(self.high_load_status_choices)
        elif self.load == self.Load.OVERLOADED:
            status = random.choice(self.overloaded_status_choices)
        else:
            status = 200
        return FakeImageResponse(status)