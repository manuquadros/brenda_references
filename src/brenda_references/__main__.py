import json
import os

from ncbi import get_article_ids, is_pmc_open
from tqdm import tqdm

from brenda_references import db

from .brenda_types import EC, Bacteria, Document
from .config import config
from debug import print
from .lpsn_interface import lpsn_synonyms
from .straininfo import get_strain_data, get_strain_ids

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

    try:
        article_ids = get_article_ids(doc.pubmed_id)
    except KeyError:
        print(doc)
        pmc_id = doi = None
        pmc_open = False
    else:
        pmc_id = article_ids.get("pmc")
        doi = article_ids.get("doi")
        pmc_open = is_pmc_open(article_ids.get("pmc"))

    if isinstance(pmc_id, str):
        pmc_id = pmc_id.replace("PMC", "")

    return doc.model_copy(
        update={
            "doi": doi,
            "pmc_id": pmc_id,
            "pmc_open": pmc_open,
        }
    )


def sync_doc_db():
    try:
        with open(config["documents"], "r") as docs:
            store = json.load(docs)
    except (FileNotFoundError, json.JSONDecodeError):
        store = {}

    db_engine = db.get_engine()

    entities = store.setdefault("entity", {})
    documents = store.setdefault("document", {})
    ecs = entities.setdefault("EC", dict())
    bacteria = entities.setdefault("bacteria", dict())
    strains = entities.setdefault("strains", dict())

    for record in tqdm(db.protein_connect_records(db_engine)):
        doc_id = record._Reference.reference_id
        ec_id = record._EC.ec_class_id
        organism_id = record._Organism.organism_id
        strain_mention = record._Strain.name

        if doc_id not in documents:
            doc = Document.model_validate(record._Reference.model_dump())
            doc = expand_doc(doc)

        if ec_id not in entities:
            synonyms = db.ec_synonyms(db_engine, record._EC.ec_class_id)
            ec = EC.model_validate(record._EC, from_attributes=True).copy(
                update={"synonyms": synonyms}
            )
            ecs[ec_id] = ec.model_dump(exclude={"ec_class_id"})

        # Add to doc those EC synonyms that are attested in the article
        doc.enzymes.setdefault(ec_id, set()).update(
            db.ec_synonyms(db_engine, ec_id, doc_id)
        )

        if organism_id not in bacteria:
            organism = Bacteria.model_validate(record._Organism, from_attributes=True)

            synonyms = lpsn_synonyms(organism.lpsn_id)
            organism = organism.model_copy(
                update={
                    "synonyms": synonyms,
                },
            )

            bacteria[organism_id] = organism.model_dump(exclude={"organism_id"})
            doc.bacteria.setdefault(organism_id, set()).add(organism.organism)

        if strain_mention:
            # straininfo_id key error
            for item in get_strain_data(get_strain_ids(strain_mention)):
                doc.strains.setdefault(item.id, set()).add(strain_mention)
                strains[item.id] = item

        documents[doc_id] = doc.model_dump()

    with open(config["documents"], "w") as docs:
        json.dump(store, docs, indent=4)
