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
    return decoded


class MetadataProducer:
    """
    When we scrape an image, we often times want to collect additional
    information about it (such as the resolution) and incorporate it into our
    database. This is accomplished by encoding the discovered metadata into
    a Kafka message and publishing it into the corresponding topic.

    pykafka is not asyncio-friendly, so we need to batch our messages together
    and intermittently send them to Kafka synchronously. Launch
    `MetadataProducer.listen` as an asyncio task to do this.
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

    def notify_exif_update(self, identifier, exif):
        msg = json.dumps(
            {
                'exif': exif,
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
                log.info(f'Publishing {queue_size} metadata events')
                start = time.monotonic()
                for msg in self._metadata_messages:
                    self._image_metadata_producer.produce(msg)
                rate = queue_size / (time.monotonic() - start)
                self._metadata_messages = []
                log.info(f'publish_rate={rate}/s')
            await asyncio.sleep(self.frequency)
