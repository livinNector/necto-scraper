import bs4
import aiohttp
import asyncio
import functools
import requests
from concurrent.futures import ThreadPoolExecutor
import os
import yarl


class Scraper:
    """
    Web Scraper using ThreadpoolExecutor and requests library
    """

    def __init__(
        self,
        max_workers=200,
        timeout=10,
        headers=None,
        scrape_func=None,
        scrape_source="soup",
        save_to="txt",
        out_dir=".",
    ) -> None:
        self.max_workers = max_workers
        self.timeout = timeout
        self.headers = headers
        self.scrape_func = scrape_func
        self.scrape_source = scrape_source
        self.save_to = save_to
        self.out_dir = out_dir

    def auto_session(func):
        def wrapper(*args, session=None, **kwargs):
            if session == None:
                with requests.Session() as session:
                    return func(*args, session=session, **kwargs)
            else:
                return func(*args, session=session, **kwargs)

        return wrapper

    def map_with_session(
        self, func, *items, return_results=True, **kwargs
    ):
        with ThreadPoolExecutor(self.max_workers) as executor:
            with requests.Session() as session:
                result = executor.map(
                    lambda item: func(item, session=session, **kwargs),
                    *items,
                )
                if not return_results:
                    for i in result:
                        result
                else:
                    return list(result)

    @auto_session
    def get_head(self, url, session: requests.Session = None, **kwargs):
        return session.head(url, **kwargs)

    @auto_session
    def get_text(self, url, session: requests.Session = None, **kwargs):
        return session.get(url, **kwargs).text

    @auto_session
    def get_soup(self, url, session: requests.Session = None, **kwargs):
        return bs4.BeautifulSoup(self.get_text(url, session, **kwargs))

    @auto_session
    def get_json(self, url, session: requests.Session = None, **kwargs):
        return session.get(url, **kwargs).json()

    def get_source_func(self, source):
        return {
            "text": self.get_text,
            "soup": self.get_soup,
            "json": self.get_json,
        }[source]

    def gets(self, source, urls, **kwargs):
        return self.map_with_session(
            self.get_source_func(source), urls, **kwargs
        )

    def save_as(self, content, path):
        try:
            with open(path, "w") as f:
                f.write(content)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(path),exist_ok=True)
            with open(path, "w") as f:
                f.write(content)

    def scrape_and_save_urls(self, urls, **kwargs):

        def scrape_and_save(url, **kwargs):
            source = self.get_source_func(self.scrape_source)(
                url, **kwargs
            )
            result = self.scrape_func(source)
            path = yarl.URL(url).path[1:]
            dirname = os.path.dirname(os.path.join(self.out_dir,path))
            basename = os.path.basename(path).split(".")[0]
            filename = os.path.join(dirname,f"{basename}.{self.save_to}")
            self.save_as(result,filename)

        self.map_with_session(
            scrape_and_save, urls, return_results=False, **kwargs
        )


class FailedRequestError(Exception):
    pass


# async def async_map(coroutine, items, *args, **kwargs, discard_exceptions=True ):
#     return await asyncio.gather(
#         *(coroutine(item, *args, **kwargs) for item in items),return_exceptions=True
#     )


class AsyncScraper:
    """
    scraper class for scraping asynchronously
    """

    def __init__(self, timeout=10, headers=None) -> None:
        self.headers = headers
        self.timeout = aiohttp.ClientTimeout(
            total=None, sock_connect=timeout, sock_read=timeout
        )

    def auto_session(async_func):
        """Decorator to create sessions as needed for functions that run in sessions"""

        @functools.wraps(async_func)
        async def wrapper(self, *args, session=None, **kwargs):
            if session is None:
                async with aiohttp.ClientSession(
                    timeout=self.timeout, raise_for_status=True
                ) as session:
                    return await async_func(
                        self, *args, session=session, **kwargs
                    )
            else:
                return await async_func(
                    self, *args, session=session, **kwargs
                )

        return wrapper

    @auto_session
    async def get(self, url, session, **kwargs):
        """Make asynchronous http GET request and get the response"""

        # use headers if available in kwargs else use self.headers
        kwargs["headers"] = kwargs.get("headers") or self.headers

        async with session.get(
            url,
            allow_redirects=self.allow_redirects,
            **kwargs,
        ) as res:
            return res

    async def _get_text(self, url, session):
        async with session.get(
            url, allow_redirects=self.allow_redirects, headers=self.headers
        ) as res:
            return await res.text()

    @auto_session
    async def get_text(self, url, session):
        return await self._get_text(url, session)

    async def map_with_session(
        self, async_func, *items, discard_nones=True, **kwargs
    ):
        def remove_nones(xs):
            return [x for x in xs if x is not None]

        connector = aiohttp.TCPConnector(limit_per_host=self.n_parallel)
        async with aiohttp.ClientSession(
            timeout=self.timeout,
            connector=connector,
            raise_for_status=True,
        ) as session:
            results = await asyncio.gather(
                *[
                    async_func(*item, session=session, **kwargs)
                    for item in zip(*items)
                ]
            )
            if discard_nones:
                results = remove_nones(results)
            return results

    @auto_session
    # @ignore_and_log_errors
    async def get_soup(
        self, url, parse_type="lxml", return_url=False, session=None
    ):
        text = await self.get_text(url, session=session)
        if text:
            soup = await asyncio.to_thread(
                bs4.BeautifulSoup, text, features=parse_type
            )
            if return_url:
                return soup, url
            else:
                return soup

    async def get_texts(self, urls):
        return await self.map_with_session(self.get_text, urls)

    async def get_soups(self, urls, parse_type="lxml", return_urls=False):
        return await self.map_with_session(
            self.get_soup,
            urls,
            parse_type=parse_type,
            return_url=return_urls,
        )

    @auto_session
    async def get_text(self, url, session=None):
        res = await self.client.get(url)
        if res.is_success:
            return res.text
        else:
            raise FailedRequestError

    async def get_soup(self, url):
        text = await self.get_text(url)
        return bs4.BeautifulSoup(text, features="lxml")

    async def get_texts(self, urls):
        return await asyncio.gather(
            *(self.get_text(url) for url in urls), return_exceptions=True
        )

    async def get_soups(self, urls):
        return await asyncio.gather(
            *(self.get_soup(url) for url in urls), return_exceptions=True
        )
