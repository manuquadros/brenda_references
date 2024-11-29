from collections.abc import Iterable, Sequence
from functools import singledispatch, lru_cache
from typing import Any, cast

import requests
from log import logger
from pydantic import ValidationError
from utils import retry_if_too_many_requests


from .brenda_types import Strain

api_root = "https://api.straininfo.dsmz.de/v1/"


@singledispatch
def strain_info_api_url(query: Any):
    raise TypeError("<query> must be a str | int | Iterable[str] | Iterable[int]")


@strain_info_api_url.register(Iterable)
def _(query: Iterable[str] | Iterable[int]) -> str:
    for item in query:
        match type(item).__name__:
            case "str":
                root = api_root + "search/strain/str_des/"
            case "int":
                root = api_root + "data/strain/max/"
            case _:
                raise requests.exceptions.InvalidURL(
                    "Unknown API function (StrainInfo v1)"
                )
        break

    return root + ",".join(map(str, query))


@strain_info_api_url.register
def _(query: str | int) -> str:
    return strain_info_api_url([query])


@retry_if_too_many_requests
def response(url: str) -> list[dict] | list[int]:
    with requests.get(
        url,
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-store",
            "Accept-Encoding": "gzip, deflate",
        },
        timeout=1,
    ) as r:
        match r.status_code:
            case 200:
                return r.json()
            case 404:
                logger().error("%s not found on StrainInfo.", url.split("/")[-1])
                return []
            case 503:
                raise requests.HTTPError("StrainInfo is unavailable.")
            case code:
                raise requests.HTTPError("Failed with HTTP Status %s" % code)


@lru_cache(maxsize=1024)
def get_strain_ids(query: str | Sequence[str]) -> list[int]:
    resp = response(strain_info_api_url(query))

    if resp and isinstance(resp[0], int):
        return cast(list[int], resp)

    return []


@lru_cache(maxsize=1024)
def get_strain_data(query: int | Sequence[int]) -> Iterable[Strain]:
    data = cast(list[dict], response(strain_info_api_url(query))) if query else []

    try:
        return (
            Strain(
                **item["strain"],
                cultures=item["strain"]["relation"].get("culture", frozenset()),
                designations=item["strain"]["relation"].get("designation", frozenset()),
            )
            for item in data
        )
    except ValidationError as e:
        print(data)
        raise e
