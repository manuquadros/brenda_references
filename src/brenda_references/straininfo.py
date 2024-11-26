import requests
from log import logger
from typing import Any, cast
from collections.abc import Iterable, Sequence
from functools import singledispatch
from pydantic import BaseModel, Field, ValidationError, TypeAdapter
from debug import print
from typing import Optional
from .brenda_types import Strain


api_root = "https://api.straininfo.dsmz.de/v1/"


@singledispatch
def strain_info_api_url(query: Any):
    raise TypeError("<query> must be a str | int | Sequence[str] | Sequence[int]")


@strain_info_api_url.register(list)
def _(query: Sequence[str] | Sequence[int]) -> str:
    match type(query[0]).__name__:
        case "str":
            root = api_root + "search/strain/str_des/"
        case "int":
            root = api_root + "data/strain/max/"
        case _:
            raise requests.exceptions.InvalidURL("Unknown API function (StrainInfo v1)")

    return root + ",".join(map(str, query))


@strain_info_api_url.register
def _(query: str | int) -> str:
    return strain_info_api_url([query])


def response(url: str) -> list[dict] | list[int]:
    with requests.get(
        url,
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-store",
            "Accept-Encoding": "gzip, deflate",
        },
    ) as r:
        match r.status_code:
            case 200:
                return r.json()
            case 404:
                logger().error(f"{url.split("/")[-1]} not found on StrainInfo.")
                return []
            case 503:
                raise requests.HTTPError("StrainInfo is unavailable.")
            case code:
                raise requests.HTTPError("Failed with HTTP Status {code}")


def get_strain_ids(query: str | Sequence[str]) -> list[int]:
    resp = response(strain_info_api_url(query))

    if resp and isinstance(resp[0], int):
        return cast(list[int], resp)

    return []


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
