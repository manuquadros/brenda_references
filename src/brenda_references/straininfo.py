import requests
from log import logger
from typing import Any, cast
from collections.abc import Iterable

api_root = "https://api.straininfo.dsmz.de/v1/"


def response(api_fn: str, query: str | int | list[int]) -> list[Any] | None:
    match api_fn:
        case "str_des":
            url = api_root + "search/strain/str_des/" + cast(str, query)
        case "data_strain_max":
            id_query = (
                str(query) if isinstance(query, int) else ",".join(map(str, query))
            )
            url = api_root + "data/strain/max/" + id_query
        case _:
            raise requests.exceptions.InvalidURL("Unknown API function (StrainInfo v1)")

    with requests.get(
        url,
        headers={
            "accept": "application/json",
            "cache-control": "no-store",
            "accept-encoding": "gzip, deflate",
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


def get_strain_ids(query: str) -> list[int] | None:
    return response("str_des", query)


def get_strain_data(query: int | list[int]) -> Iterable[dict[str, Any]] | None:
    def subset_fields(d: dict):
        return {
            "straininfo_id": d["strain"]["id"],
            "taxon": d["strain"]["taxon"]["name"],
            "cultures": frozenset(
                culture["strain_number"]
                for culture in d["strain"]["relation"]["culture"]
            ),
            "synonyms": frozenset(frozenset(d["strain"]["relation"]["designation"])),
        }

    data = cast(list[dict], response("data_strain_max", query))

    if isinstance(data, list) and len(data):
        return (subset_fields(d) for d in data)
    else:
        logger().error(f"failed with {query}")
        return None
