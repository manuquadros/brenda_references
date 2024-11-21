from brenda_references.lpsn_interface import lpsn_synonyms, lpsn_id, get_lpsn
from brenda_references.brenda_types import Organism, Bacteria
from brenda_references.straininfo import get_strain_ids


get_lpsn()
caldanaerobacter = Organism(organism_id=1, organism="Caldanaerobacter subterraneus")
thermoanaerobacter = Organism(organism_id=2, organism="Thermoanaerobacter subterraneus")


def test_bacteria_post_init_lpsn_id():
    bac = Bacteria.model_validate(caldanaerobacter, from_attributes=True)
    assert bac.lpsn_id == 774333


def test_subterraneus_synonyms():
    assert thermoanaerobacter.organism in lpsn_synonyms(caldanaerobacter.organism)
    assert caldanaerobacter.organism in lpsn_synonyms(thermoanaerobacter.organism)


def test_lpsn_id_works():
    assert lpsn_id("Clostridium difficile") == 774867
    assert lpsn_id("Agrobacterium") == 515059


def test_strain_id_retrieval():
    assert get_strain_ids("K-12") == [11469, 35283, 38539, 39812, 66369, 309797, 341518]
