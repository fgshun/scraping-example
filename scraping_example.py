import abc
import asyncio
import logging
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Iterable, Tuple, Union
from urllib.parse import urljoin, urlparse, urlunparse

import aiofiles
import aiosqlite
from requests_html import AsyncHTMLSession, HTMLSession

__version__ = '0.1.0'


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
    _concurrent: int
    download_queue: asyncio.Queue
    save_queue: asyncio.Queue
    timeout: Union[float, Tuple[float, float]]

    def __init__(self, *, concurrent: int = 3, save_concurrent: int = 3, timeout: Union[float, Tuple[float, float]] = (3.05, 27)) -> None:
        self._concurrent = concurrent
        self.download_queue: asyncio.Queue = asyncio.Queue()
        self.save_queue: asyncio.Queue = asyncio.Queue(save_concurrent)
        self.timeout = timeout

    @property
    def concurrent(self) -> int:
        return self._concurrent

    @property
    def save_concurrent(self) -> int:
        return self.save_queue.maxsize

    async def run(self) -> None:
        await self.prepare()

        download_tasks = {asyncio.create_task(self.download()) for _ in range(self.concurrent)}
        save_tasks = {asyncio.create_task(self.save()) for _ in range(self.save_concurrent)}
        download_queue_join = asyncio.create_task(self.download_queue.join())

        done, pending = await asyncio.wait(download_tasks | save_tasks | {download_queue_join}, return_when=asyncio.FIRST_COMPLETED)
        if done - {download_queue_join}:
            raise Error('raise Exception for download or save method. stop run method.')
        for task in download_tasks:
            task.cancel()
        await asyncio.gather(*download_tasks, return_exceptions=True)

        save_queue_join = asyncio.create_task(self.save_queue.join())
        done, pending = await asyncio.wait(save_tasks | {save_queue_join}, return_when=asyncio.FIRST_COMPLETED)
        if done - {save_queue_join}:
            raise Error('raise Exception for save method. stop run method.')
        for task in save_tasks:
            task.cancel()
        await asyncio.gather(*save_tasks, return_exceptions=True)

    @abc.abstractmethod
    async def prepare(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def download(self) -> None:
        async with aclosing(AsyncHTMLSession()) as session:
            while True:
                url = await self.download_queue.get()

                logger.debug('download: %s', url)
                res = await session.get(url, timeout=self.timeout)
                res.raise_for_status()
                await self.save_queue.put((url, res.content))
                logger.info('downloaded: %s', url)

                self.download_queue.task_done()
                await asyncio.sleep(0)

    @abc.abstractmethod
    async def save(self) -> None:
        while True:
            url, data = await self.save_queue.get()
            logger.info('%s (%d bytes)', url, len(data))

            self.save_queue.task_done()
            await asyncio.sleep(0)


class ImgDownloader(Downloader):
    save_dir: Path
    base_urls: Iterable[str]
    dry_run: bool

    def __init__(self, *,
                 concurrent: int = 3,
                 save_concurrent: int = 3,
                 save_dir: Path,
                 base_urls: Iterable[str], dry_run: bool = False) -> None:
        super().__init__(concurrent=concurrent, save_concurrent=save_concurrent)
        self.save_dir = save_dir
        self.base_urls = base_urls
        self.dry_run = dry_run

    async def prepare(self) -> None:
        image_src_set = set()
        async with aclosing(AsyncHTMLSession()) as session:
            for base_url in self.base_urls:
                res = await session.get(base_url)

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
            self.download_queue.put_nowait(image_src)

    async def download(self) -> None:
        if self.dry_run:
            while True:
                url = await self.download_queue.get()
                print(url)
                self.download_queue.task_done()
                await asyncio.sleep(0)
        else:
            return await super().download()


class ImgFileDownloader(ImgDownloader):
    def _url2path(self, url):
        # FIXME
        name = url.rsplit('/')[-1]
        path = self.save_dir / Path(name)
        return path

    async def run(self) -> None:
        self.save_dir.mkdir(exist_ok=True)

        await super().run()

    async def save(self) -> None:
        while True:
            url, data = await self.save_queue.get()
            path = self._url2path(url)

            logger.debug('save: %s', path)
            async with aiofiles.open(path, 'wb') as f:
                await f.write(data)
            logger.info('saved: %s', path)

            self.save_queue.task_done()
            await asyncio.sleep(0)


class ImgSQLiteDownloader(ImgDownloader):
    def __init__(self, *,
                 concurrent: int = 3,
                 save_concurrent: int = 3,
                 save_dir: Path,
                 base_urls: Iterable[str],
                 dry_run: bool = False) -> None:
        super().__init__(concurrent=concurrent,
                         save_concurrent=save_concurrent,
                         save_dir=save_dir,
                         base_urls=base_urls,
                         dry_run=dry_run)

    def conn(self):
        db_path = str(self.save_dir / 'img.sqlite')
        return aiosqlite.connect(db_path)

    async def run(self) -> None:
        self.save_dir.mkdir(exist_ok=True)

        async with self.conn() as conn:
            await conn.execute('CREATE TABLE IF NOT EXISTS img (url TEXT PRIMARY KEY, data BLOB NOT NULL)')
            await conn.commit()

        await super().run()

    async def save(self) -> None:
        async with self.conn() as conn:
            while True:
                url, data = await self.save_queue.get()

                logger.debug('save: %s', url)
                await conn.execute('INSERT OR REPLACE INTO img (url, data) VALUES (?, ?)', (url, data))
                await conn.commit()
                logger.info('saved: %s', url)

                self.save_queue.task_done()
                await asyncio.sleep(0)


async def main():
    import argparse
    import sys
    import time

    logging.basicConfig(level=logging.INFO, format='{asctime}:{name}:{threadName} {message}', style='{')

    parser = argparse.ArgumentParser()
    parser.add_argument('urls', nargs='+')
    parser.add_argument('--concurrent', type=int, default=3)
    parser.add_argument('--save-concurrent', type=int, default=3)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--save-dir', type=lambda p: Path(p).resolve(), default=Path(sys.argv[0]).resolve().parent)

    modes: DICT[str, ImgDownloader] = {
        'file': ImgFileDownloader,
        'sqlite': ImgSQLiteDownloader,
    }
    parser.add_argument('--mode', choices=list(modes), default='file')
    args = parser.parse_args()

    urls: Iterable[str] = args.urls
    concurrent: int = args.concurrent
    save_concurrent: int = args.save_concurrent
    save_dir: Path = args.save_dir
    dry_run: bool = args.dry_run
    downloader: ImgDownloader = modes[args.mode]

    started_at = time.monotonic()
    await downloader(save_dir=save_dir,
                     concurrent=concurrent,
                     save_concurrent=save_concurrent,
                     base_urls=urls,
                     dry_run=dry_run).run()
    logger.info('time: %.2f sec', time.monotonic() - started_at)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
