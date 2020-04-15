import asyncio
import json
import logging as log
import time
import pykafka
import worker.settings as settings


def kafka_connect():
    try:
        client = pykafka.KafkaClient(hosts=settings.KAFKA_HOSTS)
    except pykafka.exceptions.NoBrokersAvailableError:
        log.info('Retrying Kafka connection. . .')
        time.sleep(3)
        return kafka_connect()
    return client


def parse_message(message):
    decoded = json.loads(str(message.value, 'utf-8'))
    decoded['source'] = decoded['source'].lower()
    return decoded


async def monitor_task_list(tasks):
    # For computing average requests per second
    num_samples = 0
    resize_rate_sum = 0

    last_time = time.monotonic()
    last_count = 0
    while True:
        now = time.monotonic()
        num_completed = sum([t.done() for t in tasks])
        task_delta = num_completed - last_count
        time_delta = now - last_time
        resize_rate = task_delta / time_delta

        last_time = now
        last_count = num_completed
        if resize_rate > 0:
            resize_rate_sum += resize_rate
            num_samples += 1
            mean = resize_rate_sum / num_samples
            log.info(f'resize_rate_1s={round(resize_rate, 2)}/s, '
                     f'avg_resize_rate={round(mean, 2)}/s, '
                     f'num_completed={num_completed}')
        await asyncio.sleep(1)


class MetadataProducer:
    """
    When we scrape an image, we often times want to collect additional
    information about it (such as the resolution) and incorporate it into our
    database. This is accomplished by encoding the discovered metadata into
    a Kafka message and publishing it into the corresponding topic.

    pykafka is not asyncio-friendly, so we need to batch our messages together
    and intermittently send them to Kafka synchronously. Launch
    `AsyncProducer.listen` as an asyncio task to do this.
    """
    def __init__(self, producer, frequency=60):
        """
        :param producer: A pykafka producer.
        :param frequency: How often to publish queued events.
        """
        self.frequency = frequency
        self._image_metadata_producer = producer
        self._metadata_messages = []

    def notify_image_size_update(self, height, width, identifier):
        """ Enqueue an image size update. """
        msg = json.dumps(
            {
                'height': height,
                'width': width,
                'identifier': identifier
            }
        )
        msg_bytes = bytes(msg, 'utf-8')
        self._metadata_messages.append(msg_bytes)

    async def listen(self):
        """ Intermittently publish queued events to Kafka. """
        while True:
            queue_size = len(self._metadata_messages)
            if queue_size:
                log.info(f'Publishing {queue_size} image size metadata events')
                start = time.monotonic()
                for msg in self._metadata_messages:
                    self._image_metadata_producer.produce(msg)
                rate = queue_size / (time.monotonic() - start)
                self._metadata_messages = []
                log.info(f'publish_rate={rate}/s')
                await asyncio.sleep(self.frequency)
