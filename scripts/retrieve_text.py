"""Retrieve PubMed abstracts and full text content when available.

Feed the document database with abstracts  and full text retrieved from PubMed.
"""

import asyncio
import time
from collections.abc import Iterable, Iterator, MutableMapping
from types import TracebackType
from typing import Self

from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage
from brenda_references.brenda_types import Document
from brenda_references.config import config
from ncbi import NCBIAdapter
from tqdm import tqdm
from utils import APIAdapter, CachingMiddleware


class Missing(MutableMapping):
    """Context manager set of article ids

    Instances of the class will periodically retrieve data about its member ids
    and update the database with the results.
    """

    semaphore = asyncio.Semaphore(3)

    def __init__(
        self,
        docdb: AIOTinyDB,
        api: APIAdapter,
        elems: Iterable[tuple[str, Document]] = (),
        batch_size: int = 250,
    ) -> None:
        """Initialize the underlying set, but also specify a `batch_size`.

        :param elems: The elements to initialize the set
        :param batch_size: Size to which the set is allowed to grow before the
            data is flushed.
        """
        self._dict = dict(elems)
        self.batch_size = batch_size
        self._docdb = docdb
        self._api = api

    def __repr__(self) -> str:  # noqa: D105
        return self._dict.__repr__()

    async def __aenter__(self) -> Self:  # noqa: D105
        return self

    async def __aexit__(  # noqa: D105
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if hasattr(self, "flush"):
            await self.flush()

    def __len__(self) -> int:  # noqa: D105
        return len(self._dict)

    def __iter__(self) -> Iterator:  # noqa: D105
        return iter(self._dict)

    def __getitem__(self, key: str) -> Document:  # noqa: D105
        return self._dict[key]

    def __setitem__(self, key: str, value: Document) -> None:
        """__setitem__ method"""
        self._dict[key] = value

    async def set(self, key: str, value: Document) -> None:
        """Asynchronous wrapper over __setitem__, tracking `self.batch_size`"""
        if len(self._dict) >= self.batch_size:
            await self.flush()
            await asyncio.sleep(0.5)

        self[key] = value

    def __delitem__(self, key: str) -> None:  # noqa: D105
        self._dict.__delitem__(key)

    async def _store_in_db(self) -> None:
        """Store the updated documents into the instance's document database."""
        for key, doc in self.items():
            await self._docdb.table("documents").update(
                doc.model_dump(),
                doc_ids=[key],
            )

    async def flush(self) -> None:
        """Retrieve the relevant data from NCBI and store it in the database.

        The field to be retrieved is controlled by `self._field`, the retrieval
        function is self._fetch, and the appropriate NCBI id for retrieval is
        self._ncbi_id,
        """
        docs = self._dict.copy()
        self._dict.clear()

        async with Missing.semaphore:
            ids_to_retrieve = (
                getattr(doc, self._ncbi_id)
                for doc in docs.values()
                if hasattr(doc, self._ncbi_id)
            )

            retrieved = await self._fetch(ids_to_retrieve)

            for doc_id, doc in docs.items():
                if hasattr(doc, self._ncbi_id):
                    docs[doc_id] = doc.model_copy(
                        update={
                            self._field: retrieved.get(
                                getattr(doc, self._ncbi_id)
                            ),
                        },
                    )

            if len(retrieved):
                print(f"Retrieved {len(retrieved)} {self._field}records.")

        await self._store_in_db()


class MissingAbstract(Missing):
    """Context manager Set for documents without an abstract in record."""

    def __init__(  # noqa: D107
        self,
        docdb: AIOTinyDB,
        api: APIAdapter,
        elems: Iterable[tuple[str, Document]] = (),
    ) -> None:
        super().__init__(docdb, api, elems)
        self._ncbi_id = "pubmed_id"
        self._field = "abstract"
        self._fetch = self._api.fetch_ncbi_abstracts


class MissingFullText(Missing):
    """Context manager Set for documents without a full text in record."""

    def __init__(  # noqa: D107
        self,
        docdb: AIOTinyDB,
        api: APIAdapter,
        elems: Iterable[tuple[str, Document]] = (),
    ) -> None:
        super().__init__(docdb, api, elems)
        self._ncbi_id = "pmc_id"
        self._field = "fulltext"
        self._fetch = self._api.fetch_fulltext_articles


async def run() -> None:  # noqa: D103
    async with (
        AIOTinyDB(
            config["documents"],
            storage=CachingMiddleware(AIOJSONStorage),
        ) as docdb,
        NCBIAdapter() as ncbi,
        MissingAbstract(docdb=docdb, api=ncbi) as missing_abstract,
        MissingFullText(docdb=docdb, api=ncbi) as missing_fulltext,
        asyncio.TaskGroup() as tg,
    ):
        missing_abstract = 
        for counter, doc in enumerate(docdb.table("documents")):
            if doc.get("pubmed_id") and not doc.get("abstract", []):
                tg.create_task(
                    missing_abstract.set(doc.doc_id, Document.validate(doc)),
                )
            if (
                doc.get("pmc_id")
                and doc.get("pmc_open")
                and not doc.get("fulltext")
            ):
                tg.create_task(
                    missing_fulltext.set(doc.doc_id, Document.validate(doc)),
                )


def main() -> None:  # noqa: D103
    asyncio.run(run())
