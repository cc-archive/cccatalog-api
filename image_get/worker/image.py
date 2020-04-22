import asyncio
from functools import partial
from io import BytesIO
from PIL import Image, UnidentifiedImageError
from worker import settings as settings
from worker.stats_reporting import StatsManager


async def process_image(
        persister, session, url, identifier, stats: StatsManager, source,
        semaphore, metadata_producer=None):
    """
    Get an image, collect dimensions metadata, thumbnail it, and persist it.
    :param stats: A StatsManager for recording task statuses.
    :param source: Used to determine rate limit policy. Example: flickr, behance
    :param semaphore: Limits concurrent execution of process_image tasks
    :param identifier: Our identifier for the image at the URL.
    :param persister: The function defining image persistence. It
    should do something like save an image to disk, or upload it to
    S3.
    :param session: An aiohttp client session.
    :param url: The URL of the image.
    :param metadata_producer: The outbound message queue for dimensions
    metadata.
    """
    async with semaphore:
        loop = asyncio.get_event_loop()
        img_resp = await session.get(url, source)
        if img_resp.status >= 400:
            await stats.record_error(source, code=img_resp.status)
            return
        buffer = BytesIO(await img_resp.read())
        try:
            img = await loop.run_in_executor(None, partial(Image.open, buffer))
        except UnidentifiedImageError:
            await stats.record_error(
                source,
                code="UnidentifiedImageError"
            )
            return
        if metadata_producer:
            notify_resolution(img, identifier, metadata_producer)
            notify_exif(img, identifier, metadata_producer)
        thumb = await loop.run_in_executor(
            None, partial(thumbnail_image, img)
        )
        await loop.run_in_executor(
            None, partial(persister, img=thumb, identifier=identifier)
        )
        await stats.record_success(source)


def thumbnail_image(img: Image):
    img.thumbnail(size=settings.TARGET_RESOLUTION, resample=Image.NEAREST)
    output = BytesIO()
    img.save(output, format="JPEG", quality=30)
    output.seek(0)
    return output


def notify_resolution(img: Image, identifier, metadata_producer):
    """ Collect dimensions metadata. """
    height, width = img.size
    metadata_producer.notify_image_size_update(height, width, identifier)


def notify_exif(img: Image, identifier, metadata_producer):
    if 'exif' in img.info:
        exif = {hex(k): v for k, v in img.getexif().items()}
        if exif:
            metadata_producer.notify_exif_update(identifier, exif)


def save_thumbnail_s3(s3_client, img: BytesIO, identifier):
    s3_client.put_object(
        Bucket='cc-image-analysis',
        Key=f'{identifier}.jpg',
        Body=img
    )
