from retrying import retry
from typing import Callable
from functools import wraps
from urllib.error import HTTPError
from requests.exceptions import Timeout
import time


def retry_if_too_many_requests(func: Callable) -> Callable:
    def handler(exception: Exception):
        if isinstance(exception, HTTPError | Timeout):
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


last_call = {}


def maybe_wait(func: Callable) -> Callable:
    @wraps(func)
    def wrapped(*args, **kwargs):
        caller = func.__name__

        while time.time() - last_call.setdefault(caller, 0) < 1:
            time.sleep(0.001)

        last_call[caller] = time.time()
        return func(*args, **kwargs)

    return wrapped
