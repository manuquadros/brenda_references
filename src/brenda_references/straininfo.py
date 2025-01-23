import re
from collections.abc import Collection, Iterable, Sequence
from functools import singledispatchmethod
from typing import Any, cast

import requests
import tinydb
from pydantic import ValidationError
from tinydb import TinyDB

from log import logger
from utils import APIAdapter

from .brenda_types import Strain
from .db import _Strain

api_root = "https://api.straininfo.dsmz.de/v1/"


def normalize_strain_names(strain_names: str | Collection[str]) -> set[str]:
    """Attempt to normalize a collection of strain designations.

    This function is needed because some strains are identified in BRENDA

    :param strain_names: string or iterable containing (possibly non-standard) strain
        designations.
    :return: set containing :py:data:`strain_names` plus standardized versions of the
        designations included in :py:data:`strain_names`.
    """
    if isinstance(strain_names, str):
        strain_names = (strain_names,)

    standardized: set[str] = set()

    def apply_substitutions(w: str) -> tuple[str, int]:
        substitutions = (
            (r"(NRRL)(B | B)(\d+)", r"\1 B-\3"),
            (r"([a-zA-Z]+ \w*\d+)[Tt]", r"\1"),
        )

        number_of_subs = 0
        for sub in substitutions:
            w, n = re.subn(sub[0], sub[1], w)
            number_of_subs += n

        return w, number_of_subs

    for name in strain_names:
        new_name, number_of_subs = apply_substitutions(name)
        substrings = new_name.split("/")

        if len(substrings) > 1 or number_of_subs > 0:
            standardized.update(map(lambda w: w.strip(), substrings))

    return set(strain_names) | standardized


class StrainInfoAdapter(APIAdapter):
    def __init__(self) -> None:
        super().__init__(
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            }
        )

        self.buffer: set[_Strain] = set()
        self.storage: TinyDB

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.__flush_buffer()

    def __flush_buffer(self) -> None:
        """Store _Strain models into self.storage

        Strain models might have unnormalized strain designations, like
        'HBB / ATCC 27634 / DSM 579'. The method will extract the normalized
        designations from such a name and try to retrieve data about them from
        StrainInfo.
        """
        print("Flushing strain buffer")

        indexed_buffer: dict[int, Strain] = {
            model.id: Strain(designations=normalize_strain_names(model.name))
            for model in self.buffer
        }

        # Map each possible strain designation from the normalized name of the model
        # to the id of the model.
        names_in_brenda = {
            name: model.id for name in model.designations for model in self.buffer
        }

        ids = self.get_strain_ids(list(names_in_brenda.keys()))
        straininfo_data = (model for model in self.get_strain_data(ids))

        # Update the _Strain models with Straininfo information if available
        for entry in straininfo_data:
            names = entry.designations | frozenset(
                cult.strain_number for cult in entry.cultures
            )
            try:
                keyname = next(filter(lambda w: w in names_in_brenda, names))
                indexed_buffer[keyname] = entry.model_copy()
            except StopIteration:
                pass

        for key, strain in indexed_buffer.items():
            self.storage.table("strains").upsert(
                tinydb.table.Document(strain.model_dump(), doc_id=key)
            )

        self.buffer = set()

    def store_strains(self, strains: Iterable[Strain]) -> None:
        self.buffer.update(strains)

        if len(self.buffer) > 100:
            self.__flush_buffer()

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

        if not isinstance(query, str):
            query = cast(Sequence[str], normalize_strain_names(query))

        response = self.request(self.strain_info_api_url(query))

        if response and isinstance(response[0], int):
            return cast(list[int], response)

        return []

    def get_strain_data(self, query: int | Iterable[int]) -> tuple[Strain, ...]:
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
