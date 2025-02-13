import string
import time
from asyncio import Semaphore, sleep
from collections.abc import Callable
from functools import wraps
from types import TracebackType
from typing import Any, Self

import httpx
import nltk
from aiotinydb.middleware import AIOMiddlewareMixin
from rapidfuzz import fuzz
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


def ratio(a: str, b: str) -> float:
    """Compute the normalized Indel similarity of `a` and `b`.

    :returns: average of the similarity computation over two conditions: lower-casing
              the arguments vs not doing so.
    """
    return (fuzz.ratio(a, b, processor=lambda s: s.lower()) + fuzz.ratio(a, b)) / 2


def fuzzy_find_all(
    text: str,
    pattern: str,
    threshold: int = 83,
    *,
    try_abbrev: bool = False,
) -> list[tuple[int, int]]:
    """Find all fuzzy matches of `pattern` in `text` with given `threshold`."""
    matches = []

    if not pattern.strip():
        return []

    if text:
        words = text.split()

        for i, group in enumerate(nltk.ngrams(words, len(pattern.split()))):
            test_str = " ".join(group).strip(string.punctuation)
            ratio_pass = ratio(test_str, pattern) >= threshold
            abbrev_ratio_pass = (
                ratio(test_str, abbreviate_bacteria(pattern)) >= threshold
                if try_abbrev
                else False
            )
            if ratio_pass or abbrev_ratio_pass:
                start = sum(len(w) + 1 for w in words[:i])
                end = start + len(test_str)
                matches.append((start, end))

    return matches


def abbreviate_bacteria(name: str) -> str:
    """Abbreviate the genus component of `name`."""
    if name:
        parts = name.split()
        parts[0] = parts[0][0] + "."

        return " ".join(parts)

    return name
