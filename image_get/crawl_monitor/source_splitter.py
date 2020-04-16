import json
import logging as log
import redis
import crawl_monitor.settings as settings


NUM_SPLIT = 'num_split'
SOURCE_SET = 'inbound_sources'


def parse_message(message):
    try:
        decoded = json.loads(str(message.value, 'utf-8'))
        decoded['source'] = decoded['source'].lower()
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        log.error(f'Failed to parse message {message}')
        return None
    return decoded


class SourceSplitter:
    """
    Split URLs into different Kafka topics by source for scheduling purposes.
    Intended to be run in a dedicated process.

    Example input:

    topic: inbound_urls
    {source: 'example1', uuid: xxxxxx, 'url': "xxxxx.org}
    {source: 'example2', uuid: xxxxxx, 'url': "xxxxx.org}

    Output:
    topic: example1_urls:
    {uuid: xxxxxx, 'url': "xxxxx.org}
    topic: example2_urls:
    {uuid: xxxxxx, 'url': "xxxxx.org}

    Each time a new topic is created, the name of the source gets
    put into the 'inbound_sources' set in Redis.
    """

    def __init__(self, kafka_client, consumer):
        self.kafka_client = kafka_client
        # Map source to producer topic
        self.producers = {}
        self.consumer = consumer

    def split(self):
        redis_client = redis.StrictRedis(settings.REDIS_HOST)
        while True:
            msg_count = 0
            for msg in self.consumer:
                parsed = parse_message(msg)
                if parsed:
                    source = parsed['source']
                    if source not in self.producers:
                        redis_client.sadd(SOURCE_SET, source)
                        source_producer = self.kafka_client \
                            .topics[f'{source}_urls'] \
                            .get_producer(use_rdkafka=True)
                        self.producers[source] = source_producer
                    producer = self.producers[source]
                    del parsed['source']
                    encoded_msg = bytes(json.dumps(parsed), 'utf-8')
                    producer.produce(encoded_msg)
                    msg_count += 1
                if msg_count % 1000 == 0:
                    redis_client.incrby(NUM_SPLIT, 1000)
