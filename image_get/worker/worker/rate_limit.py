import asyncio
import tldextract

"""
Every TLD (e.g. flickr.com, metmuseum.org) gets a token bucket. Before a worker
crawls an image from a domain, it must acquire a token from the right bucket.
If there aren't enough tokens, the request will block until it has been
replenished.
"""

# Prefix for keys tracking TLD rate limits
# ex: currtokens:staticflickr.com
CURRTOKEN_PREFIX = 'currtokens:'


class RateLimitedClientSession:
    """
    Wraps aiohttp.ClientSession and enforces rate limits.
    """
    def __init__(self, aioclient, redis):
        self.client = aioclient
        self.redis = redis

    async def _get_token(self, tld):
        """
        Get a rate limiting token for a URL.
        :param tld: The domain of the item.
        :return: whether a token was successfully obtained
        """
        token_key = f'{CURRTOKEN_PREFIX}{tld.domain}.{tld.suffix}'
        tokens = int(await self.redis.decr(token_key))
        if tokens >= 0:
            token_acquired = True
        else:
            # Out of tokens
            await asyncio.sleep(1)
            token_acquired = False
        return token_acquired

    async def get(self, url):
        tld = tldextract.extract(url)
        token_acquired = False
        while not token_acquired:
            token_acquired = await self._get_token(tld)
        return await self.client.get(url)
