"""Script to pre-annotate Documents with the entities found in them.

Annotations are marked by their offsets in the text. The following, for example,  marks
the string starting at position 17 in the text up to (but not including) position 36 as
an enzyme in the D3O ontology.

    ``EntityMarkup(start=17, end=36, label="d3o:Enzyme")``
"""

import asyncio
import itertools
import math
import string
from typing import Sequence

import nltk
from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage
from rapidfuzz import fuzz
from tinydb.table import Document as TDBDocument
from tqdm import tqdm

from brenda_references import add_abstracts
from brenda_references.brenda_types import Document, EntityMarkup
from brenda_references.config import config
from ncbi import NCBIAdapter
from utils import CachingMiddleware


def ratio(a: str, b: str) -> float:
    return (
        fuzz.ratio(a, b, processor=lambda s: s.lower()) + fuzz.ratio(a, b)
    ) / 2


def fuzzy_find_all(
    text: str, pattern: str, threshold: int = 83, try_abbrev: bool = False
) -> list[tuple[int, int]]:
    """Find all fuzzy matches of pattern in text with given threshold."""
    matches = []

    if text:
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


async def mark_entities(doc: Document, db: AIOTinyDB) -> Document:
    """Annotate entities found in the abstract field of `doc`.

    The function adds the `annotation` field to `doc`. The value of this field is
    a set of EntityMarkup objects marking where the entities found in doc.bacteria,
    doc.strains, and doc.enzymes are found in doc.abstract.
    """
    # Enzymes: Get full enzyme metadata including synonyms

    new_spans = frozenset()

    if not getattr(doc, "abstract", None):
        return doc

    for ec_id in getattr(doc, "enzymes", []):
        enzyme = db.table("enzymes").get(doc_id=ec_id)
        if enzyme:
            names = {enzyme["recommended_name"]} | set(enzyme["synonyms"])
            for name in names:
                new_spans = new_spans | frozenset(
                    EntityMarkup(
                        start=start,
                        end=end,
                        entity_id=ec_id,
                        label="d3o:Enzyme",
                    )
                    for start, end in fuzzy_find_all(doc.abstract, name)
                )

    # Bacteria: Check organism name and synonyms
    for bacteria_id, name in getattr(doc, "bacteria", {}).items():
        bacteria = db.table("bacteria").get(doc_id=int(bacteria_id))
        if bacteria:
            names = {bacteria["organism"]} | set(bacteria["synonyms"])
            for name in names:
                new_spans = new_spans | frozenset(
                    EntityMarkup(
                        start=start,
                        end=end,
                        entity_id=bacteria_id,
                        label="d3o:Bacteria",
                    )
                    for start, end in fuzzy_find_all(
                        doc.abstract, name, try_abbrev=True
                    )
                )

    # Strains: Check designations and culture numbers
    for strain_id in getattr(doc, "strains", []):
        strain = db.table("strains").get(doc_id=strain_id)
        if strain:
            names = set(strain["designations"]) | {
                c["strain_number"] for c in strain["cultures"]
            }
            for name in names:
                new_spans = new_spans | frozenset(
                    EntityMarkup(
                        start=start,
                        end=end,
                        entity_id=strain_id,
                        label="d3o:Strain",
                    )
                    for start, end in fuzzy_find_all(doc.abstract, name)
                )

    return doc.model_copy(update={"entity_spans": doc.entity_spans | new_spans})


async def fetch_and_annotate(
    docs: Sequence[TDBDocument], docdb: AIOTinyDB, ncbi: NCBIAdapter
) -> None:
    """Given a sequence of TinyDB Documents, add abstract and/or entity spans to those
    that don't have them. Then return a tuple with the updated documents.
    """
    target_docs = tuple(filter(lambda d: not d.get("entity_spans"), docs))

    processed_docs = await add_abstracts(
        tuple(map(Document.model_validate, target_docs)), ncbi
    )

    marked_docs = await asyncio.gather(
        *[mark_entities(doc, docdb) for doc in processed_docs]
    )

    for doc, marked in zip(target_docs, marked_docs):
        doc.update(
            abstract=marked.abstract,
            entity_spans=[span.model_dump() for span in marked.entity_spans],
        )

    return target_docs


async def run():
    async with AIOTinyDB(
        config["documents"], storage=CachingMiddleware(AIOJSONStorage)
    ) as docdb:
        documents = docdb.table("documents")
        batch_size = 250

        batches = itertools.batched(documents, batch_size)

        async def process(docs: Sequence[TDBDocument]) -> None:
            async with NCBIAdapter() as ncbi:
                annotated_docs = await fetch_and_annotate(docs, docdb, ncbi)

                tqdm.write(
                    f"{len(annotated_docs)} abstracts processed in current batch..."
                )

            for doc in docs:
                docdb.table("documents").update(doc, doc_ids=[doc.doc_id])

        for batch_group in itertools.batched(batches, 3):
            async with asyncio.TaskGroup() as tg:
                for batch in batch_group:
                    tg.create_task(process(tuple(batch)))


def main():
    asyncio.run(run())
