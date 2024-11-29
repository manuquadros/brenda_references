import json
import os

from ncbi import get_article_ids, is_pmc_open
from tqdm import tqdm

from brenda_references import db
from collections.abc import Set

from .brenda_types import EC, Bacteria, Document, Store, BaseModel
from .config import config
from debug import print
from .lpsn_interface import lpsn_synonyms
from .straininfo import get_strain_data, get_strain_ids
from sqlalchemy.engine import TupleResult
import tinydb
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage

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


def get_document(docdb: tinydb.TinyDB, reference: db._Reference) -> Document:
    doc = docdb.table("documents").get(doc_id=reference.reference_id)

    if not doc:
        doc = expand_doc(Document.model_validate(reference.model_dump()))
        docdb.table("documents").insert(
            tinydb.table.Document(
                doc.model_dump(mode="json"), doc_id=reference.reference_id
            )
        )

    return Document.model_validate(doc)


def enzyme_synonyms(
    docdb: tinydb.TinyDB,
    synonym_refs_dict: dict[int, tuple[str, int]],
    enzymes: set[EC],
    reference_id: int,
) -> dict[int, set[str]]:
    enzymes_in_doc = dict()

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
    syn_in_doc = dict()

    for bac in bacteria:
        syn_in_doc.setdefault(bac.id, set()).add(bac.organism)
        bac = bac.model_copy(update={"synonyms": lpsn_synonyms(bac.lpsn_id)})
        docdb.table("bacteria").upsert(
            tinydb.table.Document(
                bac.model_dump(exclude="id", mode="json"), doc_id=bac.id
            )
        )

    return syn_in_doc


def strain_synonyms(
    docdb: tinydb.TinyDB, strains: set[db._Strain]
) -> dict[int, set[str]]:
    syn_in_doc = dict()

    for strain in strains:
        syn_in_doc.setdefault(strain.id, set()).add(strain.name)
        straindata = [
            data.model_dump(mode="json")
            for data in get_strain_data(frozenset(get_strain_ids(strain.name)))
        ]
        docdb.table("strains").upsert(
            tinydb.table.Document(straindata, doc_id=strain.id)
        )

    return syn_in_doc


def sync_doc_db():
    db_engine = db.get_engine()

    with tinydb.TinyDB(
        config["documents"], storage=CachingMiddleware(JSONStorage)
    ) as docdb:
        for reference in tqdm(db.brenda_references(db_engine)):
            # Collect all organism/enzyme relations annotated for the document
            relations = db.brenda_enzyme_relations(db_engine, reference.reference_id)
            ec_syn_refs = {
                enzyme.id: db.ec_synonyms(db_engine, enzyme.id)
                for enzyme in relations["enzymes"]
            }

            doc = get_document(docdb, reference).model_copy(
                update={
                    "triples": relations["triples"],
                    "enzymes": enzyme_synonyms(
                        docdb, ec_syn_refs, relations["enzymes"], reference.reference_id
                    ),
                    "bacteria": bacteria_synonyms(docdb, relations["bacteria"]),
                    "strains": strain_synonyms(docdb, relations["strains"]),
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
