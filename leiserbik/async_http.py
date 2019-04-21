from leiserbik import *
from aiohttp import ClientSession, TCPConnector, ServerDisconnectedError
from pypeln import asyncio_task as aio

async def _fetch(url, session):
    header = {"User:Agent": GENERATED_USER_AGENT}

    if 'HTTPS_PROXY' in globals():
        proxy = "http://127.0.0.1:5566"
    else:
        proxy = None

    async with session.get(url, headers=header ,proxy=proxy ) as response:
        logger.debug(f"Async Fetching 🌐: {url}")
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
