"""Script to pre-annotate Documents with the entities found in them.

Annotations are marked by their offsets in the text. The following, for example,  marks
the string starting at position 17 in the text up to (but not including) position 36 as
an enzyme in the D3O ontology.

    ``EntityMarkup(start=17, end=36, label="d3o:Enzyme")``
"""

import asyncio
import itertools
import string
from pprint import pp
from typing import NamedTuple

import nltk
from aiotinydb import AIOTinyDB
from rapidfuzz import fuzz
from tinydb import where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb.table import Document as TDBDocument
from tqdm.asyncio import tqdm_asyncio

from brenda_references.brenda_types import Document, EntityMarkup, Strain
from brenda_references.config import config
from brenda_references.straininfo import StrainInfoAdapter
from ncbi import NCBIAdapter


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


async def mark_entities(doc: Document, db: AIOTinyDB) -> Document:
    """Annotate entities found in the abstract field of `doc`.

    The function adds the `annotation` field to `doc`. The value of this field is
    a set of EntityMarkup objects marking where the entities found in doc.bacteria,
    doc.strains, and doc.enzymes are found in doc.abstract.
    """
    # Enzymes: Get full enzyme metadata including synonyms

    new_spans = frozenset()

    for ec_id in getattr(doc, "enzymes", []):
        enzyme = db.table("enzymes").get(doc_id=ec_id)
        names = {enzyme["recommended_name"]} | set(enzyme["synonyms"])
        for name in names:
            new_spans = new_spans | frozenset(
                EntityMarkup(start=start, end=end, entity_id=ec_id, label="d3o:Enzyme")
                for start, end in fuzzy_find_all(doc.abstract, name)
            )

    # Bacteria: Check organism name and synonyms
    for bacteria_id, name in getattr(doc, "bacteria", {}).items():
        bacteria = db.table("bacteria").get(doc_id=int(bacteria_id))
        names = {bacteria["organism"]} | set(bacteria["synonyms"])
        for name in names:
            new_spans = new_spans | frozenset(
                EntityMarkup(
                    start=start, end=end, entity_id=bacteria_id, label="d3o:Bacteria"
                )
                for start, end in fuzzy_find_all(doc.abstract, name, try_abbrev=True)
            )

    # Strains: Check designations and culture numbers
    for strain_id in getattr(doc, "strains", []):
        strain = db.table("strains").get(doc_id=strain_id)
        names = set(strain["designations"]) | {
            c["strain_number"] for c in strain["cultures"]
        }
        for name in names:
            new_spans = new_spans | frozenset(
                EntityMarkup(
                    start=start, end=end, entity_id=strain_id, label="d3o:Strain"
                )
                for start, end in fuzzy_find_all(doc.abstract, name)
            )

    return doc.model_copy(update={"entity_spans": doc.entity_spans | new_spans})


async def add_abstracts(
    docs: dict[str, Document], adapter: NCBIAdapter
) -> dict[str, Document]:
    """Add abstracts to the documents in `docs` when they are available."""
    ids = tuple(
        doc.pubmed_id for doc in docs.values() if doc.abstract is None and doc.pubmed_id
    )
    abstracts = await adapter.fetch_ncbi_abstracts(ids)

    tqdm.write(f"Processing {len(ids)} documents in current batch...")

    for doc in docs.values():
        if doc.pubmed_id and not doc.abstract:
            doc.abstract = abstracts.get(doc.pubmed_id)

    return docs


async def fetch_and_annotate(docs: list[TDBDocument], db: AIOTinyDB) -> None:
    docs = {
        item.doc_id: Document.model_validate(item)
        for item in docs
        if "entity_spans" not in item or item["entity_spans"] == []
    }
    docs = await add_abstracts(docs, ncbi)

    for doc_id, doc in docs.items():
        doc = await mark_entities(doc, docdb)
        await db.table("documents").update(doc, doc_ids=[doc.doc_id])


async def run():
    async with (
        AIOTinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb,
        NCBIAdapter() as ncbi,
    ):
        documents = docdb.table("documents")
        batch_size = 250
        total = math.ceil(len(documents) / batch_size)

        batches = itertools.batched(documents, batch_size)

        await tqdm_asyncio.gather(
            *(fetch_and_annotate(list(batch)) for batch in batches), total=total
        )


def main():
    asyncio.run(run())
