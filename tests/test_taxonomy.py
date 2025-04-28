from scripts import fix_taxonomy
from brenda_references.docdb import BrendaDocDB
from tinydb.storages import MemoryStorage

import pathlib

TESTDB_PATH = pathlib.Path(__file__).parent / "test_files/testdb.json"


def test_fix_bacteria():
    with BrendaDocDB(path=str(TESTDB_PATH)) as testdb_disk:
        data = testdb_disk.as_dict()

    with BrendaDocDB(storage="memory") as testdb:
        testdb._db.storage.write(data)
        other_bacids = (978, 4346, 1665, 4358, 456)
        testdoc = testdb.documents.get(doc_id=287675)
        other_bac_names = set(
            name
            for _id, name in testdoc["other_organisms"].items()
            if _id in other_bacids
        )

        for name in other_bac_names:
            assert name not in testdoc["bacteria"].values()
            assert testdb.bacteria_by_name(name) is None

        fix_taxonomy.fix_taxonomy(testdb)
        testdoc = testdb.documents.get(doc_id=287675)
        testdoc_names = set()

        for bacid in testdoc["bacteria"].keys():
            record = testdb.get_bacteria(bacid)
            testdoc_names.update([record["organism"]], record["synonyms"])

        for name in other_bac_names:
            assert name in testdoc_names
            assert testdb.bacteria_by_name(name) is not None


def test_fix_strains():
    with BrendaDocDB(path=str(TESTDB_PATH)) as testdb_disk:
        data = testdb_disk.as_dict()

    with BrendaDocDB(storage="memory") as testdb:
        testdb._db.storage.write(data)
        testdoc = testdb.documents.get(doc_id=766653)

        assert (
            "Crocosphaera subtropica ATCC 51142"
            in testdoc["other_organisms"].values()
        )
        assert "Crocosphaera subtropica" not in testdoc["bacteria"].values()
        assert not testdoc["strains"]
        assert testdb.strain_by_designation("ATCC 51142") is None

        fix_taxonomy.fix_taxonomy(testdb)
        testdoc = testdb.documents.get(doc_id=766653)

        assert "Crocosphaera subtropica" in testdoc["bacteria"].values()
        assert testdoc["strains"]

        strain_id = testdoc["strains"][0]
        assert testdb.strains.get(doc_id=strain_id) is not None
        assert testdb.strain_by_designation("ATCC 51142") is not None
