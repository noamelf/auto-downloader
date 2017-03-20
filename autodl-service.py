import asyncio
import logging
import timeit
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import click as click
from aiohttp import web

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')


def _get_filename(url):
    parsed_url = urlparse(url)
    return parsed_url.path.rsplit('/', 1)[-1] if parsed_url.path else parsed_url.netloc


def _write_content_to_file(filename, content):
    with filename.open('wb') as fd:
        fd.write(content)


async def _request(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return resp.status, await resp.content.read()


async def _timed_request(url):
    start_time = timeit.default_timer()
    status, content = await _request(url)
    elapsed = timeit.default_timer() - start_time
    return status, content, elapsed


async def _download_url(semaphore, url, filepath):
    status, content, time_elapsed = await _timed_request(url)
    logger.info(f'Finish download: {url} {status} {time_elapsed}s')

    if 200 <= status < 300:
        _write_content_to_file(filepath, content)
        logger.info(f'Write to file: {url} {filepath}')

    semaphore.release()


async def _consume(queue, concurrent_downloads):
    semaphore = asyncio.Semaphore(value=concurrent_downloads)
    while True:
        await semaphore.acquire()
        url, filepath = await queue.get()
        logger.info(f'Pop from queue: {url}')
        asyncio.ensure_future(_download_url(semaphore, url, filepath))


class Handler:
    def __init__(self, queue):
        self.q = queue

    async def add(self, request):
        data = await request.json()

        url = data['url']
        dirpath = Path(data['dirpath'])

        if not dirpath.is_dir():
            return web.Response(body=f'No such directory location {dirpath}'.encode(), status=400)

        filename = dirpath / _get_filename(url)
        logger.info(f'Add to queue: {url}')

        await self.q.put((url, filename))

        return web.Response()

    async def clear(self, _):
        # this is somewhat of a hack since for some reason there is no `clear` method on a asyncio
        # Queue.
        while not self.q.empty():
            self.q.get_nowait()

        return web.Response()


@click.command()
@click.argument('concurrent_downloads', default=10)
def main(concurrent_downloads):
    downloads_queue = asyncio.Queue()

    app = web.Application()
    handler = Handler(downloads_queue)
    app.router.add_post('/add', handler.add)
    app.router.add_post('/clear', handler.clear)

    asyncio.ensure_future(_consume(downloads_queue, concurrent_downloads))

    web.run_app(app, host='127.0.0.1', port=6736)


if __name__ == '__main__':
    main()
