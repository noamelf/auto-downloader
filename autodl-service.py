import asyncio
import logging
import timeit
from urllib.parse import urlparse

import aiohttp
import async_timeout
import click as click
from aiohttp import web

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')


def _get_filename(url):
    parsed_url = urlparse(url)
    return parsed_url.path.rsplit('/', 1)[-1] if parsed_url.path else parsed_url.netloc


async def _write_content_to_file(filename, content):
    with open(filename, 'wb') as fd:
        while True:
            chunk = await content.read()
            if not chunk:
                break
            fd.write(chunk)


async def _request(url):
    async with aiohttp.ClientSession() as session:
        with async_timeout.timeout(10):
            async with session.get(url) as resp:
                # await asyncio.sleep(2)
                return resp


async def _timed_request(url):
    start_time = timeit.default_timer()
    resp = await _request(url)
    elapsed = timeit.default_timer() - start_time
    return resp, elapsed


async def _download_url(semaphore, url):
    resp, time_elapsed = await _timed_request(url)
    logger.info(f'Finish download: {url} {resp.status} {time_elapsed}s')

    await _write_content_to_file(_get_filename(url), resp.content)

    semaphore.release()


async def _consume(queue, concurrent_downloads):
    semaphore = asyncio.Semaphore(value=concurrent_downloads)
    while True:
        await semaphore.acquire()
        url = await queue.get()
        logger.info(f'Pop from queue: {url}')
        asyncio.ensure_future(_download_url(semaphore, url))


class Handler:
    def __init__(self, queue):
        self.q = queue

    async def handle(self, request):
        data = await request.json()

        url = data['url']
        logger.info(f'Add to queue: {url}')

        await self.q.put(url)

        return web.Response()


@click.command()
@click.argument('concurrent_downloads', default=10)
def main(concurrent_downloads):
    downloads_queue = asyncio.Queue()

    app = web.Application()
    handler = Handler(downloads_queue)
    app.router.add_post('/', handler.handle)

    asyncio.ensure_future(_consume(downloads_queue, concurrent_downloads))

    web.run_app(app, host='127.0.0.1', port=6736)


if __name__ == '__main__':
    main()
