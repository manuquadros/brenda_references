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

import os
from functools import lru_cache
from typing import Iterable

import tinydb
from ncbi import NCBIAdapter
from tinydb import Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tqdm import tqdm

from brenda_references import db

from .brenda_types import EC, Bacteria, Document, Strain
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
) -> Document:
    """Add document metadata to the JSON database after retrieving additional
    data from NCBI.

    :param docdb: The JSON database
    :param ncbi: The API adapter connecting to NCBI
    :param reference: SQLModel containing the initial metadata retrieved from BRENDA.

    :return: Document model containing all the metadata retrieved.
    """
    doc = expand_doc(ncbi, Document.model_validate(reference.model_dump()))
    docdb.table("documents").insert(
        tinydb.table.Document(
            doc.model_dump(mode="json"), doc_id=reference.reference_id
        )
    )

    return Document.model_validate(doc)


def enzyme_synonyms(
    docdb: tinydb.TinyDB,
    synonym_refs_dict: dict[int, list[tuple[str, int]]],
    enzymes: set[EC],
    reference_id: int,
) -> dict[int, set[str]]:
    """Store enzyme data in the JSON database and return the subset of enzyme
    synonyms that appear in the document referenced by `reference_id`.

    :param docdb: The JSON database
    :param synonym_refs_dict: dict relating BRENDA EC class ids to a list of
                              (synonym, reference_id) pairs linking each synonym
                              to the BRENDA reference in which it was attested
    :param enzymes: EC classes linked to `reference_id`
    :param reference_id: ID of the document in the BRENDA database

    :return: dict keyed by EC Class ID, with values corresponding to their
             respective synonyms attested in `reference_id`.
    """
    enzymes_in_doc = {}

    for enzyme in enzymes:
        synonym_refs = synonym_refs_dict[enzyme.id]
        enzymes_in_doc[enzyme.id] = {
            syn[0] for syn in synonym_refs if syn[1] == reference_id
        }

        enzyme = enzyme.model_copy(
            update={"synonyms": {syn[0] for syn in synonym_refs}}
        )
        docdb.table("enzymes").upsert(
            tinydb.table.Document(
                enzyme.model_dump(exclude="id", mode="json"), doc_id=enzyme.id
            )
        )

    return enzymes_in_doc


def bacteria_synonyms(
    docdb: tinydb.TinyDB, bacteria: set[Bacteria]
) -> dict[int, set[str]]:
    """Retrieve bacterial synonyms from LPSN and store the resulting `Bacteria`
    models in the JSON database.

    :param docdb: The JSON database
    :param bacteria: Set of Bacteria models to be completed with synonyms

    :return: dict keyed by bacteria ID, containing each of the organism's
             synonyms that were attested in the document.
    """
    syn_in_doc: dict[int, set[str]] = {}

    for bac in bacteria:
        syn_in_doc.setdefault(bac.id, set()).add(bac.organism)
        bac = bac.model_copy(update={"synonyms": lpsn_synonyms(bac.lpsn_id)})
        docdb.table("bacteria").upsert(
            tinydb.table.Document(
                bac.model_dump(exclude="id", mode="json"), doc_id=bac.id
            )
        )

    return syn_in_doc


@lru_cache(maxsize=1024)
def known_designation(docdb: TinyDB, designation: str) -> bool:
    """Check whether `designation` has been attested in any of the documents
    in `docdb`."""
    return docdb.table("documents").contains(
        Query().strains.test(lambda attested: designation in attested)
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
    db_engine = db.get_engine()

    with (
        tinydb.TinyDB(
            config["documents"], storage=CachingMiddleware(JSONStorage)
        ) as docdb,
        NCBIAdapter() as ncbi,
        StrainInfoAdapter() as straininfo,
    ):
        straininfo.storage = docdb
        for reference in tqdm(db.brenda_references(db_engine)):
            try:
                doc = get_document(docdb, reference)
            except KeyError:
                doc = add_document(docdb, ncbi, reference)

                # Collect all organism/enzyme relations annotated for the document
                relations = db.brenda_enzyme_relations(
                    db_engine, reference.reference_id
                )
                ec_syn_refs = {
                    enzyme.id: db.ec_synonyms(db_engine, enzyme.id)
                    for enzyme in relations["enzymes"]
                }
                strain_names = {strain.name for strain in relations["strains"]}

                # Check if any of the bacteria identified by the entry contains
                # a strain designation that isn't already in strain_names
                for bacteria in relations["bacteria"]:
                    str_des = name_parts(bacteria.organism)["strain"]
                    if str_des:
                        strain_names.add(str_des)

                straininfo.store_strains(
                    name for name in strain_names if not known_designation(docdb, name)
                )

                doc = doc.model_copy(
                    update={
                        "triples": relations["triples"],
                        "enzymes": enzyme_synonyms(
                            docdb,
                            ec_syn_refs,
                            relations["enzymes"],
                            reference.reference_id,
                        ),
                        "bacteria": bacteria_synonyms(docdb, relations["bacteria"]),
                        "strains": strain_names,
                        "other_organisms": {
                            org.id: org.organism for org in relations["other_organisms"]
                        },
                    }
                )

                docdb.table("documents").upsert(
                    tinydb.table.Document(
                        doc.model_dump(mode="json"), doc_id=reference.reference_id
                    )
                )
