import json
import tomllib
import os
from tqdm import tqdm

from brenda_references import db
from brenda_references.config import config
from brenda_references.brenda_types import Document, Organism, EC


def complete_doc(doc: Document) -> Document:
    return doc


def sync_doc_db():
    try:
        user = os.environ["BRENDA_USER"]
        password = os.environ["BRENDA_PASSWORD"]
    except KeyError as err:
        err.add_note(
            "Please set the BRENDA_USER and BRENDA_PASSWORD environment variables"
        )
        raise

    try:
        with open(config["documents"], "r") as docs:
            store = json.load(docs)
    except (FileNotFoundError, json.JSONDecodeError):
        store = {}

    entities = store.setdefault("entity", {})
    documents = store.setdefault("document", {})
    ecs = entities.setdefault("EC", {})
    bacteria = entities.setdefault("bacteria", {})
    strains = entities.setdefault("strains", [])

    for record in tqdm(db.protein_connect_records(user, password)):
        doc_id = record._Reference.reference_id
        ec_id = record._EC.ec_class_id
        organism_id = record._Organism.organism_id
        strain_id = record.Protein_Connect.protein_organism_strain_id

        if doc_id not in documents:
            doc = Document.parse_obj(record._Reference.model_dump())
            doc = complete_doc(doc)
            documents[doc_id] = doc.model_dump()

        if ec_id not in entities:
            ecs[ec_id] = EC.parse_obj(record._EC.model_dump()).model_dump(
                exclude={"ec_class_id"}
            )

        if organism_id not in bacteria:
            bacteria[organism_id] = Organism.parse_obj(
                record._Organism.model_dump()
            ).model_dump(exclude={"organism_id"})

        if strain_id and strain_id not in strains:
            strains.append(strain_id)

    with open(config["documents"], "w") as docs:
        print(store)
        json.dump(store, docs, indent=4)
