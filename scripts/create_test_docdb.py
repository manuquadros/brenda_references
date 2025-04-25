from brenda_references.docdb import BrendaDocDB
import pathlib

TEST_DIR = pathlib.Path(__file__).parent.parent / "tests"

if __name__ == "__main__":
    with (
        BrendaDocDB() as maindb,
        BrendaDocDB(path=str(TEST_DIR / "test_files/testdb.json")) as testdb,
    ):
        sample = maindb.get_reference(287675)
        testdb.insert(table="documents", record=sample)

        for tblname in ("enzymes", "bacteria", "strains", "other_organisms"):
            for organism in sample.get(tblname, []):
                record = maindb.get_record(table=tblname, doc_id=int(organism))
                if record is not None:
                    testdb.insert(table=tblname, record=record)
