"""Retrieve PubMed abstracts and full text content when available.

Feed the document database with abstracts  and full text retrieved from PubMed.
"""

import asyncio
from collections.abc import Iterable, Iterator, MutableSet
from types import TracebackType
from typing import Self

from brenda_references.brenda_types import Document
from brenda_references.config import config
from ncbi import NCBIAdapter
from tinydb import TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tqdm import tqdm
from utils import APIAdapter


class Missing(MutableSet):
    """Context manager set of article ids

    Instances of the class will periodically retrieve data about its member ids
    and update the database with the results.
    """

    def __init__(
        self,
        docdb: TinyDB,
        elems: Iterable[tuple[str, Document]] = (),
        batch_size: int = 250,
    ) -> None:
        """Initialize the underlying set, but also specify a `batch_size`.

        :param elems: The elements to initialize the set
        :param batch_size: Size to which the set is allowed to grow before the
            data is flushed.
        """
        self._set = set(elems)
        self.batch_size = batch_size
        self._docdb = docdb

    def __repr__(self):
        return self._set.__repr__()

    def __contains__(self, elem: tuple[str, Document]) -> bool:
        return elem in self._set

    def __len__(self) -> int:
        return len(self._set)

    def __iter__(self) -> Iterator:
        return iter(self._set)

    def __enter__(self) -> Self:  # noqa: D105
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if hasattr(self, "flush"):
            self.flush()

    def add(self, elem: tuple[str, Document]) -> None:
        """Add `elem` to set and flush if `batch_size` is reached."""
        print(elem)
        self._set.add(elem)

        if len(self) == self.batch_size:
            if hasattr(self, "flush"):
                self.flush()

            # In any case, we remove the documents already processed
            # from the buffer.
            self.clear()

    def discard(self, elem: tuple[str, Document]) -> None:  # noqa: D102
        self._set.discard(elem)

    def _store_in_db(self) -> None:
        """Store the updated documents into the instance's document database."""
        for key, doc in self:
            self._docdb.table("documents").update(
                doc.model_dump(),
                doc_ids=[key],
            )


class MissingAbstract(Missing):
    """Context manager Set for documents without an abstract in record."""

    def __init__(
        self,
        docdb: TinyDB,
        api: APIAdapter,
        elems: Iterable[tuple[str, Document]] = (),
    ):
        super().__init__(docdb, elems)
        self.api = api

    def flush(self):
        abstracts = asyncio.run(
            self.api.fetch_ncbi_abstracts(
                doc.pubmed_id for _, doc in self if doc.pubmed_id
            ),
        )

        for _, doc in self:
            if doc.pubmed_id:
                doc.abstract = abstracts.get(doc.pubmed_id)

        self._store_in_db()


class MissingFullText(Missing):
    """Context manager Set for documents without a full text in record."""

    def __init__(
        self,
        docdb: TinyDB,
        api: APIAdapter,
        elems: Iterable[tuple[str, Document]] = (),
    ):
        super().__init__(docdb, elems)
        self.api = api

    def flush(self):
        fulltext = asyncio.run(
            self.api.fetch_fulltext(
                doc.pmc_id
                for _, doc in self
                if doc.pmc_id and doc.pmc_open is True
            ),
        )

        for _, doc in self.validate_docs.values():
            if doc.pmc_id:
                doc = doc.model_copy(
                    update={"fulltext": fulltext.get(doc.pmc_id)}
                )

        self._store_in_db()


async def run() -> None:  # noqa: D103
    with TinyDB(
        config["documents"],
        storage=CachingMiddleware(JSONStorage),
    ) as docdb:
        async with NCBIAdapter() as ncbi:
            missing_abstract = MissingAbstract(docdb=docdb, api=ncbi)
            missing_fulltext = MissingFullText(docdb=docdb, api=ncbi)

            for doc in tqdm(docdb.table("documents")):
                if doc.get("pubmed_id") and not doc.get("abstract", []):
                    missing_abstract.add((doc.doc_id, Document.validate(doc)))
                if doc.get("pmc_id") and not doc.get("fulltext", []):
                    missing_fulltext.add((doc.doc_id, Document.validate(doc)))


def main() -> None:
    asyncio.run(run())
