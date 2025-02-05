import time
from functools import wraps
from typing import Callable, Self, Any

from retrying import retry
import httpx
from aiotinydb.middleware import AIOMiddlewareMixin
from tinydb.middlewares import CachingMiddleware as SyncCachingMiddleware


class CachingMiddleware(SyncCachingMiddleware, AIOMiddlewareMixin):
    """Adding async powers to CachingMiddleware"""


def retry_if_too_many_requests(func: Callable) -> Callable:
    def handler(exception: Exception):
        if isinstance(exception, httpx.HTTPError):
            if exception.response.status_code == 429:
                print("HTTP Error 429: Too Many Requests. Retrying...")
            else:
                print(f"HTTP error {exception.response.status_code}, retrying...")

            return True

        return False

    @wraps(func)
    def wrapped(*args, **kwargs):
        return retry(
            retry_on_exception=handler, wait_random_min=30000, wait_random_max=300000
        )(func)(*args, **kwargs)

    return wrapped


class APIAdapter:
    """General context manager for API connections

    Subclasses can initialize the headers parameter of the parent.
    """

    def __init__(self, headers: dict[str, str] = {}) -> None:
        self.client = httpx.AsyncClient(
            headers=headers, timeout=30.0, follow_redirects=True
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        self.client.aclose()

    @retry_if_too_many_requests
    async def request(self, url: str) -> Any:
        return await self.client.get(url)
