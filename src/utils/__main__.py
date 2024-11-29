from retrying import retry
from typing import Callable
from functools import wraps
import urllib


def retry_if_too_many_requests(func: Callable) -> Callable:
    def handler(exception: Exception):
        if isinstance(exception, urllib.error.HTTPError):
            if exception.code == 429:
                print(
                    "HTTP Error 429: Too Many Requests... We are retrying in a few seconds."
                )
            else:
                print(exception, "retrying")

            return True

        return False

    @wraps(func)
    def wrapped(*args, **kwargs):
        return retry(
            retry_on_exception=handler, wait_random_min=30000, wait_random_max=300000
        )(func)(*args, **kwargs)

    return wrapped
