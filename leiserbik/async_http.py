from aiohttp import ClientSession, TCPConnector
from pypeln import asyncio_task as aio

from leiserbik import *


async def _fetch(url, session):
    header = {"User:Agent": GENERATED_USER_AGENT}

    # import ipdb; ipdb.set_trace()
    if 'ROTATE_HTTP_PROXY' in globals() and ROTATE_HTTP_PROXY is not None:
        logger.debug("🌐 Async Proxy Enabled")
        proxy = ROTATE_HTTP_PROXY
    else:
        logger.debug("🛑 Async Proxy NOT Enabled")
        proxy = None

    async with session.get(url, headers=header ,proxy=proxy ) as response:
        logger.debug(f"🌐 Async Fetching: {url}")
        response = await response.read()
        return response

        if response is None:
            logger.debug(f"Empty 🌐: {url}")
            return None
        else:
            return response

def fetch_all(urls):

    stage = aio.map(_fetch,urls,
        workers = MAX_WORKERS,
        on_start = lambda: ClientSession(connector=TCPConnector(limit=None)),
        on_done = lambda _status, session: session.close())
    return stage
