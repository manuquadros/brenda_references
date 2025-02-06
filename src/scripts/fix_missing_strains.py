import asyncio
import itertools
import math

from aiotinydb import AIOTinyDB
from utils import CachingMiddleware
from aiotinydb.storage import AIOJSONStorage
from tinydb import where
from tqdm import tqdm

from brenda_references.brenda_types import Strain
from brenda_references.config import config
from brenda_references.straininfo import StrainInfoAdapter


async def run():
    async with (
        AIOTinyDB(
            config["documents"], storage=CachingMiddleware(AIOJSONStorage)
        ) as docdb,
        StrainInfoAdapter() as straininfo,
    ):
        straininfo.storage = docdb

        batch_size = 100
        total = math.ceil(
            docdb.table("strains").count(where("id") == None) / batch_size
        )
        for batch in tqdm(
            itertools.batched(
                docdb.table("strains").search(where("id") == None), batch_size
            ),
            total=total,
        ):
            strains = await straininfo.retrieve_strain_models(
                {doc.doc_id: Strain.model_validate(doc) for doc in batch}
            )

            await asyncio.gather(
                *(
                    docdb.table("strains").update(strain.model_dump(), doc_ids=[key])
                    for key, strain in strains.items()
                )
            )


def main():
    asyncio.run(run())
