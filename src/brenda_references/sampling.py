"""Module providing functions for sampling references from the dataset."""

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd
from gme.gme import GreedyMaximumEntropySampler

pd.options.mode.copy_on_write = True


def relation_records(doc: Mapping[str, Any]) -> list[dict[str, str]]:
    """Build relation records from a document."""
    pmid = doc["pubmed_id"]
    records = []

    if "relations" not in doc:
        return []

    for predicate, argpairs in doc["relations"].items():
        for args in argpairs:
            subj = str(args["subject"])
            obj = str(args["object"])

            if predicate == "HasSpecies":
                subj_prefix = "str_"
                obj_prefix = "bac_"
            else:
                obj_prefix = "enz_"
                if subj in doc["bacteria"]:
                    subj_prefix = "bac_"
                elif subj in doc["strains"]:
                    subj_prefix = "str_"
                else:
                    subj_prefix = "oos_"

            records.append(
                {
                    "pubmed_id": pmid,
                    "predicate": predicate,
                    "subject": subj_prefix + subj,
                    "object": obj_prefix + obj,
                }
            )

    return records


def build_sampling_df(docs: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Build DataFrame where each row is a relation found in the database."""
    rows = (
        relation_record
        for doc in docs
        for relation_record in relation_records(doc)
        if doc["pubmed_id"]
    )

    return pd.DataFrame(rows).astype(
        dtype={
            "pubmed_id": "int32",
            "predicate": "category",
            "subject": "string",
            "object": "string",
        }
    )


class GMESampler:
    def __init__(self, data: Iterable[Mapping[str, Any]]) -> None:
        """Initialize sampler.

        :param data: Iterable containing records to sample from
        """
        self._sampler = GreedyMaximumEntropySampler(
            selector="dutopia", binarised=False
        )
        self._data = data

        self._sampling_df = build_sampling_df(self._data)

    def sample(
        self,
        n: int,
        item_column: str = "pubmed_id",
        on_columns: list[str] | None = None,
        approx: int = 0,
    ) -> pd.DataFrame:
        """Sample `N` items from the dataset, without replacement."""
        if on_columns is None:
            on_columns = ["subject", "object"]

        sample = self._sampler.sample(
            data=self._sampling_df,
            N=min(n, len(self._data)),
            item_column=item_column,
            on_columns=on_columns,
            approx=approx,
        )
        self._data = tuple(
            doc
            for doc in self._data
            if doc["pubmed_id"] not in sample["pubmed_id"]
        )

        return sample
