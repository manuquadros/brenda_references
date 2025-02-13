import pytest
from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage

from brenda_references.brenda_types import Document, EntityMarkup
from brenda_references.config import config
from ncbi import NCBIAdapter
from scripts.preannotate import mark_entities
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

        annotated_doc = await mark_entities(doc, docdb)
        annotated_doc = Document.validate(annotated_doc)

    markup = annotated_doc.entity_spans

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


@pytest.mark.asyncio
async def test_deak() -> None:
    doc = {
        "authors": "Deak, F.; Denes, G.",
        "title": "Purification and some properties of rat liver tyrosyl-tRNA synthetase",
        "journal": "Biochim. Biophys. Acta",
        "volume": "526",
        "pages": "626-634",
        "year": 1978,
        "pubmed_id": "31184",
        "path": "/home/data/brenda/literatur/6/6_1_1_1/Biochim_Biophys_Acta_526_626.pdf",
        "pmc_id": None,
        "pmc_open": False,
        "doi": "10.1016/0005-2744(78)90153-5",
        "created": "2025-01-15T18:46:35.151975+00:00",
        "enzymes": [3494],
        "bacteria": {},
        "strains": [],
        "other_organisms": {"5301": "Rattus norvegicus"},
        "relations": {"HasEnzyme": [{"subject": 5301, "object": 3494}]},
        "abstract": (
            "Rat liver cytoplasmic tyrosine:tRNA ligase "
            "(tyrosine:tRNA ligase, EC 6.1.1.1) was purifie"
            "d by ultracentrifugation, DEAE-cellulose chromatography "
            "and repeated phosphocellulose chromatography by mo"
            "re than 1500-fold. The molecular weight of the enzyme was "
            "approx. 150 000 as determined by Sephadex G-200 "
            "gel filtration. On the basis of sodium dodecyl sulfate-polyacrylamide "
            "gel electrophoresis, the enzyme consisted of two subunits, "
            "each of 68 000 daltons. We found the following Km values for "
            "the enzyme: 13 micrometer for tyrosine and 1.7 mM for ATP in "
            "the ATP:PPi exchange reaction and 13 micrometer for tyrosine, 210 "
            "micrometer for ATP and 0.14 micrometer for tRNATyr in the aminoacylation "
            "reaction. The rate of tyrosyl-tRNA synthesis was 50-fold lower than that "
            "of ATP:PPi exchange. Addition of a saturating amount of tRNA did not "
            "affect the rate of ATP:PPi exchange."
        ),
        "entity_spans": [
            {"start": 713, "end": 735, "entity_id": 3494, "label": "d3o:Enzyme"},
            {"start": 43, "end": 63, "entity_id": 3494, "label": "d3o:Enzyme"},
            {"start": 22, "end": 42, "entity_id": 3494, "label": "d3o:Enzyme"},
        ],
        "reviewed": "2025-01-15T18:46:35.151975+00:00",
    }

    async with (
        AIOTinyDB(
            config["documents"],
            storage=CachingMiddleware(AIOJSONStorage),
        ) as docdb,
        NCBIAdapter() as ncbi,
    ):
        annotated_doc = await mark_entities(doc, docdb)
        annotated_doc = Document.validate(annotated_doc)

    markup = annotated_doc.entity_spans

    assert tup_to_markup(22, 42, 3494, "d3o:Enzyme") in markup
    assert tup_to_markup(44, 64, 3494, "d3o:Enzyme") in markup
    assert tup_to_markup(713, 725, 3494, "d3o:Enzyme") in markup
