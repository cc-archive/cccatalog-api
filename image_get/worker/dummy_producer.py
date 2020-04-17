import json
import time
import logging as log
from pykafka import KafkaClient
from itertools import cycle, islice

"""
This is a sample image resize event producer.
"""


slow_msgs = [
    {
        'url': 'https://farm9.staticflickr.com/8116/8606654389_e56c706e2c_b.jpg',
        'uuid': 'c29b3ccc-ff8e-4c66-a2d2-d9fc886872ca',
        'source': 'phylopic'
    }
] * 9

slow_msgs .extend([
    {
        'url': 'https://farm9.staticflickr.com/8116/8606654389_e56c706e2c_b.jpg',
        'uuid': 'c29b3ccc-ff8e-4c66-a2d2-d9fc886872ca',
        'source': 'animaldiversity'
    }
] * 9)

fast_msgs = [
    {
        'url': 'https://farm9.staticflickr.com/8116/8606654389_e56c706e2c_b.jpg',
        'uuid': 'c29b3ccc-ff8e-4c66-a2d2-d9fc886872ca',
        'source': 'flickr'
    }
] * 5000
msgs = []
msgs.extend(slow_msgs)
msgs.extend(fast_msgs)
encoded_msgs = [json.dumps(msg) for msg in msgs]

client = KafkaClient(hosts='kafka:9092')
topic = client.topics['inbound_images']

counter = 0
start = time.monotonic()
with topic.get_producer(use_rdkafka=True, max_queued_messages=5000000) as producer:
    for msg in encoded_msgs:
        producer.produce(bytes(msg, 'utf-8'))
        counter += 1
        if counter % 100000 == 0:
            print(f'{counter}/{len(encoded_msgs)}')
print(f'produced {len(encoded_msgs) / (time.monotonic() - start)} per second')
