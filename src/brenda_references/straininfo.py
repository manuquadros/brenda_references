import requests
from log import logger
from typing import Any, cast
from collections.abc import Iterable

api_root = "https://api.straininfo.dsmz.de/v1/"


def strain_info_api_url(query: list[int | str]) -> str:
    match type(query[0]).__name__:
        case "str":
            root = api_root + "search/strain/str_des/"
        case "int":
            root = api_root + "data/strain/max/"
        case _:
            raise requests.exceptions.InvalidURL("Unknown API function (StrainInfo v1)")

    return root + ",".join(map(str, query))


def response(url: str) -> list[dict] | None:
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
                return None
            case 503:
                raise requests.HTTPError("StrainInfo is unavailable.")
            case code:
                raise requests.HTTPError("Failed with HTTP Status {code}")


def get_strain_ids(query: str | list[str]) -> list[int] | None:
    if not isinstance(query, list):
        query = [query]
    return response(strain_info_api_url(query))


def get_strain_data(
    query: int | str | list[str | int],
) -> Iterable[dict[str, Any]] | None:
    def subset_fields(d: dict | int):
        if isinstance(d, int):
            return d

        return {
            "straininfo_id": d["strain"]["id"],
            "taxon": d["strain"]["taxon"]["name"],
            "cultures": frozenset(
                culture["strain_number"]
                for culture in d["strain"]["relation"]["culture"]
            ),
            "synonyms": frozenset(frozenset(d["strain"]["relation"]["designation"])),
        }

    if not isinstance(query, list):
        query = [query]

    if isinstance(query[0], str):
        query = get_strain_ids(query)

    data = cast(list[dict], response(strain_info_api_url(query)))

    if isinstance(data, list) and len(data):
        return (subset_fields(d) for d in data)
    else:
        logger().error(f"failed with {query}")
        return None
