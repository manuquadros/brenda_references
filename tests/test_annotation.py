from pprint import pp

import pytest
from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage

from brenda_references import add_abstracts
from brenda_references.brenda_types import Document, EntityMarkup
from brenda_references.config import config
from ncbi import NCBIAdapter
from scripts.preannotate import fetch_and_annotate
from utils import CachingMiddleware


def tup_to_markup(*args) -> EntityMarkup:
    return EntityMarkup(
        **{k: v for k, v in zip(EntityMarkup.model_fields.keys(), args)}
    )


@pytest.mark.asyncio
async def test_annotate_nureki():
    async with (
        AIOTinyDB(
            config["documents"], storage=CachingMiddleware(AIOJSONStorage)
        ) as docdb,
        NCBIAdapter() as ncbi,
    ):
        doc = docdb.table("documents").get(doc_id=204)
        doc = await fetch_and_annotate([doc], docdb, ncbi)
        doc = Document.validate(doc[0])

    markup = doc.entity_spans

    pp(markup)

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
