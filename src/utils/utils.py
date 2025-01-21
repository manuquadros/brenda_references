import time
from functools import wraps
from typing import Callable, Self, Any
from urllib.error import HTTPError

import requests
from requests.exceptions import Timeout
from retrying import retry
from urllib3 import Retry


def retry_if_too_many_requests(func: Callable) -> Callable:
    def handler(exception: Exception):
        if isinstance(exception, HTTPError):
            if exception.code == 429:
                print(
                    "HTTP Error 429: Too Many Requests... We are retrying in a few seconds."
                )
            else:
                print(exception, "retrying")

            return True

        if isinstance(exception, Timeout):
            print(f"{func.__name__} timed out.")

            return True

        return False

    @wraps(func)
    def wrapped(*args, **kwargs):
        return retry(
            retry_on_exception=handler, wait_random_min=30000, wait_random_max=300000
        )(func)(*args, **kwargs)

    return wrapped


class APIAdapter:
    def __init__(self, headers: dict[str, str] = {}) -> None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=Retry(connect=4, backoff_factor=0.5)
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update(headers)

        self.session = session

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self.session.close()

    @retry_if_too_many_requests
    def request(self, url: str) -> Any:
        return self.session.get(url, timeout=1)
