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
                else:
                    species, strain = None, orgname

                if species:
                    bacteria.add(species)
                if strain:
                    strains.add(strain)
            elif ncbitax.is_bacteria(orgname):
                delete_from_other.add(_id)
                bacteria.add(orgname)

        for orgname in bacteria:
            match = docdb.bacteria_by_name(orgname)

            if match is not None:
                docdb.add_bac_synonyms(doc_id=match.doc_id, synonyms={orgname})
            else:
                bacid = docdb.insert_bacteria_record(orgname)
                doc["bacteria"].update({bacid: orgname})
                docdb.update_record(
                    table="documents", fields=doc, doc_id=doc.doc_id
                )

        for orgname in strains:
            match = docdb.strain_by_designation(orgname)

            if match is not None:
                docdb.add_strain_synonyms(
                    doc_id=match.doc_id, synonyms={orgname}
                )
            else:
                with StrainInfoAdapter() as si:
                    model = si.retrieve_strain_models(
                        {0: Strain(designations=frozenset({orgname}))}  # type: ignore[call-arg]
                    )

                strainid = docdb.insert(
                    table="strains", record=model[0].model_dump()
                )
                if strainid is not None:
                    doc["strains"].append(strainid)
                    docdb.update_record(
                        table="documents", fields=doc, doc_id=doc.doc_id
                    )
                else:
                    msg = f"Insertion of {orgname} in the strains table failed."
                    raise ValueError(msg)

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
