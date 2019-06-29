import abc
import asyncio
import logging
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Iterable, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import aiofiles
from requests_html import AsyncHTMLSession, HTMLSession

logger = logging.getLogger(__name__)


@asynccontextmanager
async def aclosing(thing):
    try:
        yield thing
    finally:
        await thing.close()


class Error(Exception):
    pass


class Downloader(abc.ABC):
    concurrent: int

    def __init__(self, *, concurrent: int = 3) -> None:
        self.concurrent = concurrent

    async def run(self) -> None:
        num_download_task = self.concurrent
        # TODO: add num_save_task parameter
        num_save_task = num_download_task

        download_queue: asyncio.Queue = asyncio.Queue()
        save_queue: asyncio.Queue = asyncio.Queue(num_save_task)

        self.prepare(download_queue)

        download_tasks = {asyncio.create_task(self.download(download_queue, save_queue)) for _ in range(num_download_task)}
        save_tasks = {asyncio.create_task(self.save(save_queue)) for _ in range(num_save_task)}
        download_queue_join = asyncio.create_task(download_queue.join())
        save_queue_join = asyncio.create_task(save_queue.join())

        done, pending = await asyncio.wait(download_tasks | save_tasks | {download_queue_join}, return_when=asyncio.FIRST_COMPLETED)
        if download_queue_join not in done:
            raise Error('raise Exception for download or save method. stop run method.')
        for task in download_tasks:
            task.cancel()
        await save_queue.join()
        done, pending = await asyncio.wait(save_tasks | {save_queue_join}, return_when=asyncio.FIRST_COMPLETED)
        if save_queue_join not in done:
            raise Error('raise Exception for save method. stop run method.')
        for task in save_tasks:
            task.cancel()
        await asyncio.gather(*(download_tasks | save_tasks), return_exceptions=True)

    @abc.abstractmethod
    def prepare(self, download_queue: asyncio.Queue) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def download(self, download_queue: asyncio.Queue, save_queue: asyncio.Queue) -> None:
        async with aclosing(AsyncHTMLSession()) as session:
            while True:
                url = await download_queue.get()

                logger.debug('download: %s', url)
                res = await session.get(url, timeout=10.0)
                res.raise_for_status()
                await save_queue.put((url, res.content))
                logger.info('downloaded: %s', url)

                download_queue.task_done()
                await asyncio.sleep(0)

    @abc.abstractmethod
    async def save(self, save_queue: asyncio.Queue) -> None:
        while True:
            url, data = await save_queue.get()
            logger.info('%s (%d bytes)', url, len(data))

            save_queue.task_done()
            await asyncio.sleep(0)


class ImgDownloader(Downloader):
    save_dir: Path
    base_urls: Iterable[str]
    dry_run: bool

    def __init__(self, *, save_dir: Path, concurrent: int = 3,
                 base_urls: Iterable[str], dry_run: bool = False) -> None:
        super().__init__(concurrent=concurrent)
        self.save_dir = save_dir
        self.base_urls = base_urls
        self.dry_run = dry_run

    def prepare(self, download_queue: asyncio.Queue) -> None:
        image_src_set = set()
        with closing(HTMLSession()) as session:
            for base_url in self.base_urls:
                res = session.get(base_url)

                for img in res.html.find('img'):
                    src = img.attrs['src']
                    parsed = urlparse(src)
                    if not parsed.netloc:
                        src = urljoin(base_url, src)
                    elif not parsed.scheme:
                        parsed.scheme = urlparse(base_url).scheme
                        src = urlunparse(parsed)
                    image_src_set.add(src)

        for image_src in image_src_set:
            download_queue.put_nowait(image_src)

    async def download(self, download_queue: asyncio.Queue, save_queue: asyncio.Queue) -> None:
        if self.dry_run:
            while True:
                url = await download_queue.get()
                path = self.url2path(url)
                print(f'{url} -> {path}')
                download_queue.task_done()
                await asyncio.sleep(0)
        else:
            return await super().download(download_queue, save_queue)

    def url2path(self, url):
        # FIXME
        name = url.rsplit('/')[-1]
        path = self.save_dir / Path(name)
        return path

    async def save(self, save_queue: asyncio.Queue) -> None:
        while True:
            url, data = await save_queue.get()
            path = self.url2path(url)
            path.parent.mkdir(exist_ok=True)

            logger.debug('save: %s', path)
            async with aiofiles.open(path, 'wb') as f:
                await f.write(data)
            logger.info('saved: %s', path)

            save_queue.task_done()
            await asyncio.sleep(0)


async def main():
    import argparse
    import sys
    import time

    logging.basicConfig(level=logging.INFO, format='{asctime}:{name}:{threadName} {message}', style='{')

    parser = argparse.ArgumentParser()
    parser.add_argument('urls', nargs='+')
    parser.add_argument('--concurrent', type=int, default=3)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--save-dir', type=lambda p: Path(p).resolve(), default=Path(sys.argv[0]).resolve().parent)
    args = parser.parse_args()

    urls: Iterable[str] = args.urls
    concurrent: int = args.concurrent
    save_dir: Path = args.save_dir
    dry_run: bool = args.dry_run

    started_at = time.monotonic()
    await ImgDownloader(save_dir=save_dir,
                        concurrent=concurrent,
                        base_urls=urls,
                        dry_run=dry_run).run()
    logger.info('time: %.2f sec', time.monotonic() - started_at)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
