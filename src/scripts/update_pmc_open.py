#!/usr/bin/env python

"""
Script for making sure that articles marked as not being pmc_open actually are
not available through PubMed Central.
"""

from brenda_references.config import config
from ncbi import NCBIAdapter
from aiotinydb import AIOTinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tqdm import tqdm
import asyncio


async def run():
    """Update document records in the JSON database in case they are actually
    available through PubMed Central."""
    async with (
        AIOTinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb,
        NCBIAdapter() as ncbi,
    ):
        for doc in tqdm(docdb.table("documents")):
            if doc["pmc_id"] and not doc["pmc_open"]:
                is_open = await ncbi.is_pmc_open(doc["pmc_id"])
                docdb.table("documents").update(
                    {"pmc_open": is_open}, doc_ids=[doc.doc_id]
                )


def main():
    asyncio.run(run())
