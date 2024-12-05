from collections.abc import Iterable, Sequence
from functools import lru_cache, singledispatchmethod
from typing import Any, cast

import requests
import tinydb
from log import logger
from pydantic import ValidationError
from tinydb import Query, TinyDB
from utils import APIAdapter

from .brenda_types import Strain

api_root = "https://api.straininfo.dsmz.de/v1/"


class StrainInfoAdapter(APIAdapter):
    def __init__(self) -> None:
        super().__init__(
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            }
        )

        self.buffer: set[str] = set()
        self.storage: TinyDB

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.__flush_buffer()

    def __flush_buffer(self) -> None:
        ids = self.get_strain_ids(tuple(self.buffer))
        straininfo_data = self.get_strain_data(ids)

        for si in straininfo_data:
            self.storage.table("strains").upsert(
                tinydb.table.Document(
                    si.model_dump(exclude="siid", mode="json"), doc_id=si.siid
                )
            )

    @staticmethod
    def __response_handler(
        url: str, response: requests.Response
    ) -> list[dict] | list[int]:
        match response.status_code:
            case 200:
                return response.json()
            case 404:
                logger().error("%s not found on StrainInfo.", url.split("/")[-1])
                return []
            case 503:
                raise requests.HTTPError("StrainInfo is unavailable.")
            case code:
                raise requests.HTTPError(f"Failed with HTTP Status {code}")

    def request(self, url: str) -> list[dict] | list[int]:
        return self.__response_handler(url, super().request(url))

    @singledispatchmethod
    @staticmethod
    def strain_info_api_url(query: Any):
        raise TypeError("<query> must be a str | int | Iterable[str] | Iterable[int]")

    @strain_info_api_url.register(Iterable)
    @staticmethod
    def _(query: Iterable[str] | Iterable[int]) -> str:
        if not query:
            raise ValueError("No query specified")

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
    @staticmethod
    def _(query: str | int) -> str:
        return StrainInfoAdapter.strain_info_api_url([query])

    def get_strain_ids(self, query: str | Sequence[str]) -> list[int]:
        if not query:
            return []

        response = self.request(self.strain_info_api_url(query))

        if response and isinstance(response[0], int):
            return cast(list[int], response)

        return []

    def get_strain_data(self, query: int | Sequence[int]) -> tuple[Strain, ...]:
        """Retrieve StrainInfo data for the strain IDs given in the argument.

        :param query: IDs to be queried through the API.

        :return: Tuple containing Strain models encapsulating the information
            retrieved from StrainInfo.
        """
        try:
            data = self.request(self.strain_info_api_url(query))
        except ValueError:
            return ()

        try:
            return tuple(
                Strain(
                    **item["strain"],
                    cultures=item["strain"]["relation"].get("culture", frozenset()),
                    designations=item["strain"]["relation"].get(
                        "designation", frozenset()
                    ),
                )
                for item in data
            )
        except ValidationError as e:
            print(data)
            raise e
