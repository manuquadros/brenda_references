"""
Brenda References

This module provides functions to build a database of article references from
the BRENDA database. Each article reference is linked to the enzymes it is
associated with on BRENDA as well as with the organisms that are referenced
by the article as expressing each particular enzyme.

The main function is sync_doc_db, which will fetch references from BRENDA and
update the JSON database it founds references that are not already stored in
the latter.
"""

from functools import lru_cache

import tinydb
from ncbi import NCBIAdapter
from tinydb import Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tqdm import tqdm
from typing import Iterable

from brenda_references import db

from .brenda_types import EC, Bacteria, Document
from .config import config
from .lpsn_interface import lpsn_synonyms, name_parts
from .straininfo import StrainInfoAdapter


def expand_doc(ncbi: NCBIAdapter, doc: Document) -> Document:
    """Check if we can find a PMCID and a DOI for the article."""
    if not doc.pubmed_id:
        return doc

    try:
        article_ids = ncbi.article_ids(doc.pubmed_id)
    except KeyError:
        print(doc)
        pmc_id = doi = None
        pmc_open = False
    else:
        pmc_id = article_ids.get("pmc")
        doi = article_ids.get("doi")

        if isinstance(pmc_id, str):
            pmc_id = pmc_id.replace("PMC", "")

        pmc_open = ncbi.is_pmc_open(pmc_id)

    return doc.model_copy(
        update={
            "doi": doi,
            "pmc_id": pmc_id,
            "pmc_open": pmc_open,
        }
    )


def get_document(docdb: tinydb.TinyDB, reference: db._Reference) -> Document:
    """Retrieve document from the JSON database by reference_id."""
    doc = docdb.table("documents").get(doc_id=reference.reference_id)

    if doc is None:
        raise KeyError("{reference.reference_id} does not exist in the documents table")

    return Document.model_validate(doc)


def add_document(
    docdb: tinydb.TinyDB, ncbi: NCBIAdapter, reference: db._Reference
) -> None:
    """Add document metadata to the JSON database after retrieving additional
    data from NCBI.

    :param docdb: The JSON database
    :param ncbi: The API adapter connecting to NCBI
    :param reference: SQLModel containing the initial metadata retrieved from BRENDA.

    :return: Document model containing all the metadata retrieved.
    """
    doc = expand_doc(ncbi, Document.model_validate(reference.model_dump()))
    docdb.table("documents").insert(
        tinydb.table.Document(doc.model_dump(), doc_id=reference.reference_id)
    )


def store_enzyme_synonyms(
    docdb: tinydb.TinyDB,
    enzyme: EC,
    synonyms: Iterable[str],
) -> None:
    """Store enzyme data in the JSON database.

    :param docdb: The JSON database
    :param enzyme: EC model linked describing an enzyme
    :param synonyms: set of synonyms for that EC Class retrieved from BRENDA
    """
    enzyme = enzyme.model_copy(update={"synonyms": frozenset(synonyms)})
    docdb.table("enzymes").upsert(
        tinydb.table.Document(enzyme.model_dump(exclude="id"), doc_id=enzyme.id)
    )


def store_bacteria(docdb: tinydb.TinyDB, bacteria: Iterable[Bacteria]) -> None:
    """Retrieve bacterial synonyms from LPSN and store the resulting `Bacteria`
    models in the JSON database.

    :param docdb: The JSON database
    :param bacteria: Set of Bacteria models to be completed with synonyms
    """

    for bac in bacteria:
        bac = bac.model_copy(update={"synonyms": lpsn_synonyms(bac.lpsn_id)})
        docdb.table("bacteria").upsert(
            tinydb.table.Document(bac.model_dump(exclude="id"), doc_id=bac.id)
        )


def sync_doc_db() -> None:
    """Make sure that the references present in BRENDA are processed and
    reflected in the JSON database.

    For each reference, store into the JSON database the entities that are
    linked to it in BRENDA, as well as the relations between these entities
    that are annotated in the database.

    At this point, we are not performing any checks as to whether information
    on BRENDA has changed since the last time we visited it, except as to
    whether new references were added to it.
    """
    with (
        tinydb.TinyDB(
            config["documents"], storage=CachingMiddleware(JSONStorage)
        ) as docdb,
        NCBIAdapter() as ncbi,
        StrainInfoAdapter() as straininfo,
        db.BRENDA() as brenda,
    ):
        straininfo.storage = docdb

        print("Retrieving literature references.")
        with tqdm(total=brenda.count_references()) as progress_bar:
            for reference in brenda.references():
                if not docdb.table("documents").contains(doc_id=reference.reference_id):
                    doc = add_document(docdb, ncbi, reference)
                progress_bar.update(1)

        print("Retrieving enzyme-organism relations from BRENDA.")

        # Collect all organism/enzyme relations annotated in BRENDA for each document
        for doc in tqdm(docdb.table("documents")):
            relations = brenda.enzyme_relations(doc.doc_id)

            for enzyme in relations["enzymes"]:
                synonyms = brenda.ec_synonyms(enzyme.id)
                store_enzyme_synonyms(docdb, enzyme, synonyms)

            straininfo.store_strains(
                [
                    strain
                    for strain in relations["strains"]
                    if not docdb.table("strains").contains(doc_id=strain.id)
                ]
            )
            store_bacteria(docdb, relations["bacteria"])
            document = Document.model_validate(doc).copy(
                update={
                    "relations": relations["triples"],
                    "enzymes": frozenset(enzyme.id for enzyme in relations["enzymes"]),
                    "bacteria": {bac.id: bac.organism for bac in relations["bacteria"]},
                    "strains": [strain.id for strain in relations["strains"]],
                    "other_organisms": {
                        org.id: org.organism for org in relations["other_organisms"]
                    },
                }
            )

            docdb.table("documents").update(
                tinydb.table.Document(document.model_dump(), doc_id=doc.doc_id)
            )
