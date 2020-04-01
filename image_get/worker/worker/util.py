import asyncio
import json
import logging as log
import time
import pykafka
import tldextract
import worker.settings as settings
import worker.stats_reporting as stats
from functools import partial
from io import BytesIO
from PIL import Image, UnidentifiedImageError
from worker.stats_reporting import register_error


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


def thumbnail_image(img: Image):
    img.thumbnail(size=settings.TARGET_RESOLUTION, resample=Image.NEAREST)
    output = BytesIO()
    img.save(output, format="JPEG", quality=30)
    output.seek(0)
    return output


def save_thumbnail_s3(s3_client, img: BytesIO, identifier):
    s3_client.put_object(
        Bucket='cc-image-analysis',
        Key=f'{identifier}.jpg',
        Body=img
    )


async def process_image(persister, session, url, identifier, semaphore, redis=None):
    """
    Get an image, resize it, and persist it.
    :param redis: If included, record successfully resized images here.
    :param semaphore: A shared semaphore limiting concurrent execution.
    :param identifier: Our identifier for the image at the URL.
    :param persister: The function defining image persistence. It
    should do something like save an image to disk, or upload it to
    S3.
    :param session: An aiohttp client session.
    :param url: The URL of the image.
    """
    async with semaphore:
        tld = tldextract.extract(url)
        loop = asyncio.get_event_loop()
        img_resp = await session.get(url)
        if img_resp.status >= 400:
            await stats.register_error(redis, tld)
            return
        buffer = BytesIO(await img_resp.read())
        try:
            img = await loop.run_in_executor(None, partial(Image.open, buffer))
        except UnidentifiedImageError:
            await redis.incr('resize_errors')
            return
        thumb = await loop.run_in_executor(
            None, partial(thumbnail_image, img)
        )
        await loop.run_in_executor(
            None, partial(persister, img=thumb, identifier=identifier)
        )
        if redis:
            await redis.incr('successfully_resized')
