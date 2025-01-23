from tinydb import Query, TinyDB, where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from brenda_references.brenda_types import Strain
from brenda_references.straininfo import StrainInfoAdapter
from brenda_references.config import config
from tqdm import tqdm
from pprint import pp
import itertools
import math


def main():
    with (
        TinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb,
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
            strains = straininfo.retrieve_strain_models(
                {doc.doc_id: Strain.model_validate(doc) for doc in batch}
            )

            for key, strain in strains.items():
                docdb.table("strains").update(strain.model_dump(), doc_ids=[key])
