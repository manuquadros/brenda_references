#!/usr/bin/env python

from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
from brenda_references.config import config
from ncbi import NCBIAdapter
from tqdm import tqdm


def main():
    with (
        TinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb,
        NCBIAdapter() as ncbi,
    ):

        def update_pmc_open():
            def transform(doc):
                doc["pmc_open"] = ncbi.is_pmc_open(doc["pmc_id"])

            return transform

        for doc in tqdm(docdb.table("documents")):
            docdb.table("documents").update(update_pmc_open(), doc_ids=[doc.doc_id])
