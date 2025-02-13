"""Script to pre-annotate Documents with the entities found in them.

Annotations are marked by their offsets in the text. The following, for example,  marks
the string starting at position 17 in the text up to (but not including) position 36 as
an enzyme in the D3O ontology.

    ``EntityMarkup(start=17, end=36, label="d3o:Enzyme")``
"""

import asyncio
import datetime
import itertools
import math
import string
from collections.abc import Collection, Sequence

import nltk
from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage
from rapidfuzz import fuzz
from tinydb import Query
from tinydb.table import Document as TDBDocument
from tqdm import tqdm

import log
from brenda_references import add_abstracts
from brenda_references.brenda_types import Document, EntityMarkup, RDFClass
from brenda_references.config import config
from ncbi import NCBIAdapter
from utils import CachingMiddleware


def ratio(a: str, b: str) -> float:
    return (fuzz.ratio(a, b, processor=lambda s: s.lower()) + fuzz.ratio(a, b)) / 2


def fuzzy_find_all(
    text: str,
    pattern: str,
    threshold: int = 83,
    try_abbrev: bool = False,
) -> list[tuple[int, int]]:
    """Find all fuzzy matches of pattern in text with given threshold."""
    matches = []

    if not pattern.strip():
        return []

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
    logger = log.stderr_logger()

    if not getattr(doc, "abstract", None):
        return doc

    def get_names(
        ent_dict: dict[str, str | Collection[str]],
        ent_type: RDFClass,
    ) -> frozenset[str]:
        match ent_type:
            case RDFClass.D3OEnzyme:
                return {ent_dict["recommended_name"]} | frozenset(ent_dict["synonyms"])
            case RDFClass.D3OBacteria:
                return {ent_dict["organism"]} | frozenset(ent_dict["synonyms"])
            case RDFClass.D3OStrain:
                return frozenset(ent_dict["designations"]) | frozenset(
                    culture["strain_number"] for culture in ent_dict["cultures"]
                )
            case _:
                logger().error(f"Unknown entity type: {ent_type}")
                return frozenset()

    async def process_entity_type(  # noqa: RUF029
        doc: Document,
        db: AIOTinyDB,
        ent_type: RDFClass,
    ) -> frozenset[str]:
        markups = frozenset()

        keys = {
            "d3o:Enzyme": "enzymes",
            "d3o:Bacteria": "bacteria",
            "d3o:Strain": "strains",
        }

        for entity_id in getattr(doc, keys[ent_type], []):
            entity = db.table(keys[ent_type]).get(doc_id=entity_id)
            if entity:
                markups = markups.union(
                    *(
                        frozenset(
                            EntityMarkup(
                                start=start,
                                end=end,
                                entity_id=entity_id,
                                label=ent_type,
                            )
                            for start, end in fuzzy_find_all(
                                doc.abstract,
                                name,
                                try_abbrev=ent_type is RDFClass.D3OBacteria,
                            )
                        )
                        for name in get_names(entity, ent_type)
                    ),
                )

        return markups

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(process_entity_type(doc, db, ent_type))
            for ent_type in RDFClass
        ]

    new_spans = frozenset.union(*(task.result() for task in tasks))

    return doc.model_copy(
        update={
            "entity_spans": doc.entity_spans | new_spans,
            "modified": datetime.datetime.now(datetime.UTC),
        },
    )


async def fetch_and_annotate(
    docs: Sequence[TDBDocument],
    docdb: AIOTinyDB,
    ncbi: NCBIAdapter,
) -> tuple[TDBDocument]:
    """Add abstract and/or entity spans to the elements of `docs`.

    Return a tuple with the updated documents.
    """
    target_docs: list[TDBDocument] = list(
        filter(lambda d: not d.get("entity_spans"), docs),
    )

    processed_docs: list[Document] = await add_abstracts(
        list(map(Document.model_validate, target_docs)),
        ncbi,
    )

    marked_docs: list[Document] = await asyncio.gather(
        *[mark_entities(doc, docdb) for doc in processed_docs],
    )

    for doc, marked in zip(target_docs, marked_docs, strict=True):
        doc.update(reviewed=datetime.datetime.now(datetime.UTC).isoformat())

        spans = [span.model_dump() for span in marked.entity_spans]

        doc.update(
            abstract=marked.abstract,
            entity_spans=spans,
        )

    return target_docs


async def run() -> None:
    async with AIOTinyDB(
        config["documents"],
        storage=CachingMiddleware(AIOJSONStorage),
    ) as docdb:
        documents = docdb.table("documents").search(
            (~Query().entity_spans.exists() | (Query().entity_spans == []))
            & (Query().reviewed == Query().created),
        )

        if not documents or not len(documents):
            print("No documents to annotate")
            return

        batch_size = 250
        n_tasks = math.ceil(len(documents) / batch_size)

        batches = itertools.batched(documents, batch_size)

        progress_bar = tqdm(total=n_tasks)

        async def process(docs: Sequence[TDBDocument]) -> None:
            async with NCBIAdapter() as ncbi:
                annotated_docs = await fetch_and_annotate(docs, docdb, ncbi)

                for doc in annotated_docs:
                    docdb.table("documents").update(doc, doc_ids=[doc.doc_id])

                progress_bar.update(1)

        for batch_group in itertools.batched(batches, 3):
            async with asyncio.TaskGroup() as tg:
                for batch in batch_group:
                    tg.create_task(process(tuple(batch)))

        progress_bar.close()


def main() -> None:
    asyncio.run(run())
