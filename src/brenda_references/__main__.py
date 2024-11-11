import json
import tomllib
import os
from tqdm import tqdm

from brenda_references import db, config


def sync_doc_db():
    try:
        user = os.environ["BRENDA_USER"]
        password = os.environ["BRENDA_PASSWORD"]
    except KeyError as err:
        err.add_note(
            "Please set the BRENDA_USER and BRENDA_PASSWORD environment variables"
        )
        raise

    for record in tqdm(db.protein_connect_records(user, password)):
        print(record)
