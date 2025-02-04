"""Script to pre-annotate Documents with the entities found in them.

Annotations are marked by their offsets in the text. The following, for example,  marks
the string starting at position 17 in the text up to (but not including) position 36 as
an enzyme in the D3O ontology.

    ``EntityMarkup(start=17, end=36, label="d3o:Enzyme")``
"""

import itertools
import string
from pprint import pp
from typing import NamedTuple

import nltk
from rapidfuzz import fuzz
from tinydb import TinyDB, where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb.table import Document
from tqdm import tqdm

from brenda_references.brenda_types import EntityMarkup, Strain
from brenda_references.config import config
from brenda_references.straininfo import StrainInfoAdapter


def ratio(a: str, b: str) -> float:
    return (fuzz.ratio(a, b, processor=lambda s: s.lower()) + fuzz.ratio(a, b)) / 2


def fuzzy_find_all(
    text: str, pattern: str, threshold: int = 83, try_abbrev: bool = False
) -> list[tuple[int, int]]:
    """Find all fuzzy matches of pattern in text with given threshold."""
    matches = []
    words = text.split()

    for i, group in enumerate(nltk.ngrams(words, len(pattern.split()))):
        test_str = " ".join(group).strip(string.punctuation)
        ratio_pass = ratio(test_str, pattern) >= threshold
        abbrev_ratio_pass = (
            ratio(test_str, abbreviate_bacteria(pattern)) >= threshold
            if try_abbrev
            else False
        )
        if ratio_pass or abbrev_ratio_pass:
            start = sum(len(w) + 1 for w in words[:i])
            end = start + len(test_str)
            matches.append((start, end))

    return matches


def abbreviate_bacteria(name: str) -> str:
    if name:
        parts = name.split()
        parts[0] = parts[0][0] + "."

        return " ".join(parts)

    return name


def mark_entities(doc: Document, db: TinyDB) -> Document:
    """Annotate entities found in the abstract field of `doc`.

    The function adds the `annotation` field to `doc`. The value of this field is
    a set of EntityMarkup objects marking where the entities found in doc.bacteria,
    doc.strains, and doc.enzymes are found in doc.abstract.
    """
    text = doc["abstract"]
    annotations = set()

    # Enzymes: Get full enzyme metadata including synonyms
    for ec_id in doc.get("enzymes", []):
        enzyme = db.table("enzymes").get(doc_id=ec_id)
        names = {enzyme["recommended_name"]} | set(enzyme["synonyms"])
        for name in names:
            for start, end in fuzzy_find_all(text, name):
                annotations.add(
                    EntityMarkup(
                        start=start, end=end, entity_id=ec_id, label="d3o:Enzyme"
                    )
                )

    # Bacteria: Check organism name and synonyms
    for bacteria_id, name in doc.get("bacteria", {}).items():
        bacteria = db.table("bacteria").get(doc_id=int(bacteria_id))
        names = {bacteria["organism"]} | set(bacteria["synonyms"])
        for name in names:
            for start, end in fuzzy_find_all(text, name, try_abbrev=True):
                annotations.add(
                    EntityMarkup(
                        start=start,
                        end=end,
                        entity_id=bacteria_id,
                        label="d3o:Bacteria",
                    )
                )

    # Strains: Check designations and culture numbers
    for strain_id in doc.get("strains", []):
        strain = db.table("strains").get(doc_id=strain_id)
        names = set(strain["designations"]) | {
            c["strain_number"] for c in strain["cultures"]
        }
        for name in names:
            for start, end in fuzzy_find_all(text, name):
                annotations.add(
                    EntityMarkup(
                        start=start, end=end, entity_id=strain_id, label="d3o:Strain"
                    )
                )

    return Document(
        {**doc, "entity_spans": [a.model_dump() for a in annotations]},
        doc_id=doc.doc_id,
    )


def main():
    with TinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb:
        documents = docdb.table("documents")
        for doc in tqdm(
            documents.search(
                (where("abstract") != None)
                & ((where("entity_spans") == []) | ~(where("entity_spans").exists()))
            )
        ):
            doc = mark_entities(doc, docdb)
            documents.update(doc, doc_ids=[doc.doc_id])
