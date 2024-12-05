#!/usr/bin/env python

from brenda_references.config import config
from ncbi import NCBIAdapter
from tinydb import TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
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
            if doc["pmc_id"] and not doc["pmc_open"]:
                docdb.table("documents").update(update_pmc_open(), doc_ids=[doc.doc_id])
