import pytest
from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage

from brenda_references.brenda_types import Document, EntityMarkup
from brenda_references.config import config
from ncbi import NCBIAdapter
from scripts.preannotate import fetch_and_annotate
from utils import CachingMiddleware


def tup_to_markup(*args) -> EntityMarkup:
    return EntityMarkup.model_validate(
        dict(zip(EntityMarkup.model_fields.keys(), args, strict=True)),
    )


@pytest.mark.asyncio
async def test_annotate_nureki() -> None:
    doc = {
        "authors": "Nureki, O.; Suzuki, K.; Hara-Yokoyama, M.; Kohno, T.; Matsuzawa, H.; Ohta, T.; Shimizu, T.; Morikawa, K.; Miyazawa, T.; Yokoyama, S.",
        "title": "Glutamyl-tRNA synthetase from Thermus thermophilus HB8. Molecular cloning of the gltX gene and crystallization of the overproducing protein",
        "journal": "Eur. J. Biochem.",
        "volume": "204",
        "pages": "465-472",
        "year": 1992,
        "pubmed_id": "1541262",
        "path": "/home/data/brenda/literatur/6/6_1_1_17/Eur_J_Biochem_204_465.pdf",
        "pmc_id": None,
        "pmc_open": False,
        "doi": "10.1111/j.1432-1033.1992.tb16656.x",
        "created": "2025-01-15T18:47:14.383720+00:00",
        "enzymes": [3502],
        "bacteria": {"2305": "Thermus thermophilus"},
        "strains": [6880],
        "other_organisms": {},
        "relations": {
            "HasEnzyme": [
                {"subject": 2305, "object": 3502},
                {"subject": 6880, "object": 3502},
            ],
            "HasSpecies": [{"subject": 6880, "object": 2305}],
        },
        "reviewed": "2025-01-15T18:47:14.383720+00:00",
    }

    async with (
        AIOTinyDB(
            config["documents"],
            storage=CachingMiddleware(AIOJSONStorage),
        ) as docdb,
        NCBIAdapter() as ncbi,
    ):

        doc = await fetch_and_annotate([doc], docdb, ncbi)
        doc = Document.validate(doc[0])

    markup = doc.entity_spans

    assert tup_to_markup(17, 36, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(194, 213, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(380, 399, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(642, 661, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(700, 720, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(846, 865, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(988, 1007, 3502, "d3o:Enzyme") in markup
    assert tup_to_markup(1158, 1177, 3502, "d3o:Enzyme") in markup

    assert tup_to_markup(66, 86, 2305, "d3o:Bacteria") in markup
    assert tup_to_markup(364, 379, 2305, "d3o:Bacteria") in markup
    assert tup_to_markup(626, 641, 2305, "d3o:Bacteria") in markup
    assert tup_to_markup(972, 987, 2305, "d3o:Bacteria") in markup
    assert tup_to_markup(1142, 1157, 2305, "d3o:Bacteria") in markup

    assert tup_to_markup(87, 90, 6880, "d3o:Strain") in markup
