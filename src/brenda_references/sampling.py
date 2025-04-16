"""Module providing functions for sampling references from the dataset."""

from collections.abc import Mapping
from typing import Any

import pandas as pd


def relation_records(doc: Mapping[str, Any]) -> list[dict[str, str]]:
    """Build relation records from a document."""
    pmid = doc["pubmed_id"]
    records = []

    if "relations" not in doc:
        return []

    for predicate, argpairs in doc["relations"].items():
        for args in argpairs:
            subject = str(args["subject"])
            object = str(args["object"])

            if predicate == "HasSpecies":
                subj_prefix = "str_"
                obj_prefix = "bac_"
            else:
                obj_prefix = "enz_"
                if subject in doc["bacteria"]:
                    subj_prefix = "bac_"
                elif subject in doc["strains"]:
                    subj_prefix = "str_"
                else:
                    subj_prefix = "oos_"

            records.append(
                {
                    "pubmed_id": pmid,
                    "predicate": predicate,
                    "subject": subj_prefix + subject,
                    "object": obj_prefix + object,
                }
            )

    return records


def build_df(docs: tuple[Mapping[str, Any]]) -> pd.DataFrame:
    """Build DataFrame where each row is a relation found in the database."""

    rows = (
        relation_record for doc in docs for relation_record in relation_records(doc)
    )

    return pd.DataFrame(rows)
