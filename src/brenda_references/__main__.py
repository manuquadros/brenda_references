import json
import tomllib
import os
from tqdm import tqdm

from brenda_references import db
from brenda_references.config import config
from brenda_references.brenda_types import Document, Organism, EC

from ncbi import get_article_ids, is_pmc_open

try:
    api_key = os.environ["NCBI_API_KEY"]
except KeyError:
    print(
        "Continuing without API key. If you want to go faster, set the ",
        "NCBI_API_KEY environment variable.",
    )


def expand_doc(doc: Document) -> Document:
    """Check if we can find a PMCID and a DOI for the article."""
    if not doc.pubmed_id:
        return doc

    article_ids = get_article_ids(doc.pubmed_id)
    pmc_id = article_ids.get("pmc")

    if isinstance(pmc_id, str):
        pmc_id = pmc_id.replace("PMC", "")

    return doc.model_copy(
        update={
            "doi": article_ids.get("doi"),
            "pmc_id": pmc_id,
            "pmc_open": is_pmc_open(article_ids.get("pmc")),
        }
    )


def expand_organism(organism: Organism) -> Organism:
    """Retrieve available synonyms for `organism`"""
    pass


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
            doc = Document.model_validate(record._Reference.model_dump())
            doc = expand_doc(doc)
            documents[doc_id] = doc.model_dump()

        if ec_id not in entities:
            synonyms = get_ec_synonyms(record._EC.ec_class_id)
            ecs[ec_id] = EC.parse_obj(record._EC.model_dump()).model_dump(
                exclude={"ec_class_id"}
            )

        if organism_id not in bacteria:
            bacteria[organism_id] = Organism.parse_obj(
                record._Organism.model_dump()
            ).model_dump(exclude={"organism_id"})

        if strain_id and strain_id not in strains:
            strains.append(strain_id)

    print(store)

    # with open(config["documents"], "w") as docs:
    #     print(store)
    #     json.dump(store, docs, indent=4)
