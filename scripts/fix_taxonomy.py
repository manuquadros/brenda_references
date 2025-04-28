"""Reclassify organisms in the `other_organisms` fields of the doc database.

When one of those organisms is a bacteria in the NCBI taxonomy, move it to the
bacteria field of the document and add an entry for it in the `bacteria` table
of the database.

Similarly, if one of those organisms is a bacterial strain, move it to the
strain field of the document and add an entry for it in the `strains` table.
Furthermore, extract the species part of the strain name, if there is one, and
make sure it is reflected both in the bacteria field of the document and on the
bacteria table of the database.
"""

from apiadapters.straininfo import StrainInfoAdapter
from taxonomy import ncbitax
from tqdm import tqdm
from brenda_references.docdb import BrendaDocDB
from brenda_types import Strain
from tinydb.table import Document as TinyDBDoc


def update_doc_bacteria(
    docdb: BrendaDocDB, doc: TinyDBDoc, bacname: str
) -> None:
    """Update `doc` in `docdb` with `bacname`.

    `bacname` is added as a new record if it is not one of the designations of
    an existing record in docdb.bacteria.
    """
    if docdb.bacteria_by_name(bacname) is None:
        bacid = docdb.insert_bacteria_record(bacname)
        doc["bacteria"].update({bacid: bacname})
        docdb.update_record(table="documents", fields=doc, doc_id=doc.doc_id)


def update_doc_strain(
    docdb: BrendaDocDB, doc: TinyDBDoc, strainname: str
) -> None:
    """Update `doc` in `docdb` with `strainname`.

    `strainname` is added as a new record if it is not one of the designations
    of an existing record in docdb.strains.
    """
    match = docdb.strain_by_designation(strainname)

    if match is not None:
        docdb.add_strain_synonyms(doc_id=match.doc_id, synonyms={strainname})
    else:
        with StrainInfoAdapter() as si:
            model = si.retrieve_strain_models(
                {0: Strain(designations=frozenset({strainname}))}  # type: ignore[call-arg]
            )

        strainid = docdb.insert(table="strains", record=model[0].model_dump())
        if strainid is not None:
            doc["strains"].append(strainid)
            docdb.update_record(
                table="documents", fields=doc, doc_id=doc.doc_id
            )
        else:
            msg = f"Insertion of {strainname} in the strains table failed."
            raise ValueError(msg)


def fix_taxonomy(docdb: BrendaDocDB) -> None:
    """Make sure there are no bacteria in the other_organisms field."""
    for doc in tqdm(docdb.references):
        doc_id = doc.doc_id
        delete_from_other: set[int] = set()
        bacteria: set[str] = set()
        strains: set[str] = set()

        for _id, orgname in doc.get("other_organisms", {}).items():
            if ncbitax.is_bacterial_strain(orgname):
                delete_from_other.add(_id)

                nameparts = ncbitax.decompose_strain_name(orgname)

                if nameparts:
                    species, strain = nameparts.species, nameparts.strain
                    if species:
                        bacteria.add(species)
                    if strain:
                        strains.add(strain)
                else:
                    strains.add(orgname)

            elif ncbitax.is_bacteria(orgname):
                delete_from_other.add(_id)
                bacteria.add(orgname)

        for orgname in bacteria:
            update_doc_bacteria(docdb, doc, orgname)

        for orgname in strains:
            update_doc_strain(docdb, doc, orgname)

        docdb.update_record(
            table="documents",
            fields={
                "other_organisms": {
                    k: v
                    for k, v in doc["other_organisms"].items()
                    if k not in delete_from_other
                }
            },
            doc_id=doc_id,
        )


if __name__ == "__main__":
    with BrendaDocDB() as docdb:
        fix_taxonomy(docdb)
