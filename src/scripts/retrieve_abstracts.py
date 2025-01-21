"""Retrieve PubMed abstracts and add them to the database, when available.

Feed the document database with abstracts retrieved from PubMed. Many of these are not
open access, so they should only be used for model training, not for redistribution
with the  published version of the dataset. In that case, only metadata allowing
retrieval of the abstract texts should be shared.
"""

from brenda_references.brenda_types import Document
from brenda_references.config import config
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb import TinyDB, where, Query
from tqdm import tqdm


def get_fulltext(doc: Document) -> Document:
    return doc


def main() -> None:
    with (
        TinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb,
    ):
        for item in tqdm(
            brendadb.table("documents").search(
                (where("pmc_open") == True) & (where("bacteria") != {})
            )
        ):
            if True or not open_subset.documents.contains(doc_id=item.doc_id):
                doc = get_fulltext(Document.model_validate(item))

                print(doc)
                break
