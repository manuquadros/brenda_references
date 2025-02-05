"""Retrieve PubMed abstracts and add them to the database, when available.

Feed the document database with abstracts retrieved from PubMed. Many of these are not
open access, so they should only be used for model training, not for redistribution
with the  published version of the dataset. In that case, only metadata allowing
retrieval of the abstract texts should be shared.
"""

import asyncio
import itertools
import math
from operator import attrgetter
from pprint import pp
from typing import Iterable

from aiotinydb import AIOTinyDB
from tinydb import Query, where
from utils import CachingMiddleware
from aiotinydb.storage import AIOJSONStorage
from tqdm import tqdm

from brenda_references.brenda_types import Document
from brenda_references.config import config
from ncbi import NCBIAdapter
from utils import APIAdapter


async def add_abstracts(
    docs: dict[str, Document], adapter: APIAdapter
) -> dict[str, Document]:
    """Add abstracts to the documents in `docs` when they are available."""
    docs = {
        doc_id: doc
        for doc_id, doc in docs.items()
        if doc.abstract is None and doc.pubmed_id
    }
    abstracts = await adapter.fetch_ncbi_abstracts(
        (doc.pubmed_id for doc in docs.values())
    )

    tqdm.write(f"Processing {len(docs)} documents in current batch...")
    for doc in docs.values():
        if doc.pubmed_id:
            doc.abstract = abstracts.get(doc.pubmed_id)

    return docs


async def run() -> None:
    async with (
        AIOTinyDB(
            config["documents"], storage=CachingMiddleware(AIOJSONStorage)
        ) as docdb,
        NCBIAdapter() as ncbi,
    ):
        documents = docdb.table("documents")
        batch_size = 250
        total = math.ceil(len(documents) / batch_size)
        for batch in tqdm(
            itertools.batched(documents, batch_size),
            total=total,
            position=0,
            desc="Batches",
        ):
            docs = {item.doc_id: Document.model_validate(item) for item in batch}
            docs = await add_abstracts(docs, ncbi)

            for key, doc in docs.items():
                await documents.update(doc.model_dump(), doc_ids=[key])


def main():
    asyncio.run(run())
