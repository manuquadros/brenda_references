import time
from asyncio import Semaphore, sleep
from collections.abc import Callable
from functools import wraps
from types import TracebackType
from typing import Any, Self

import httpx
from aiotinydb.middleware import AIOMiddlewareMixin
from tinydb.middlewares import CachingMiddleware as SyncCachingMiddleware


class CachingMiddleware(SyncCachingMiddleware, AIOMiddlewareMixin):
    """Adding async powers to CachingMiddleware."""


def retry_if_too_many_requests(func: Callable) -> Callable:
    async def handler(exception: Exception):
        if isinstance(exception, httpx.HTTPError):
            if hasattr(exception, "response") and exception.response.status_code == 429:
                print("HTTP Error 429: Too Many Requests. Retrying...")
            else:
                print(f"HTTP error {exception}, retrying...")

            await sleep(300)

            return True

        return False

    @wraps(func)
    async def wrapped(*args, **kwargs):
        retry_count = 0
        while True:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if not await handler(e) or retry_count > 5:
                    raise

                retry_count += 1

    return wrapped


class APIAdapter:
    """General context manager for API connections.

    Subclasses can initialize the headers parameter of the parent.
    """

    def __init__(self, headers: dict[str, str] = {}, rate_limit: int = 3) -> None:
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=30.0,
            follow_redirects=True,
        )
        self.semaphore = Semaphore(rate_limit)
        self.last_request_time = {}
        self.min_delay = 0.4

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.client.aclose()

    @retry_if_too_many_requests
    async def request(self, url: str) -> Any:
        domain = str(httpx.URL(url).host)

        async with self.semaphore:
            now = time.time()
            last_req = self.last_request_time.get(domain, 0)
            if now - last_req < self.min_delay:
                await sleep(self.min_delay - (now - last_req))
            self.last_request_time[domain] = time.time()

            return await self.client.get(url)
