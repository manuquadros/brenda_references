"""Module providing queries into the document database."""

from tinydb import TinyDB, where
from tinydb.table import Document as TDocument
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

from brenda_references.config import config
from brenda_references.brenda_types import Document
from ncbi.parser import is_scanned


def fulltext_articles() -> tuple[TDocument]:
    """Retrieve documents from the database with full text available."""
    with TinyDB(
        config["documents"], storage=CachingMiddleware(JSONStorage)
    ) as docdb:
        fulltext = docdb.table("documents").search(
            where("fulltext").exists() & (where("fulltext") != "")
        )
        return tuple(
            filter(lambda doc: not is_scanned(doc["fulltext"]), fulltext)
        )
