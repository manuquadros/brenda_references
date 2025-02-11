from brenda_references.brenda_types import EntityMarkup, Triple


def test_triple_comparisons() -> None:
    A = Triple(subject=24, object=25)
    B = Triple(subject=45, object=23)
    C = Triple(subject=45, object=25)

    assert A < B
    assert B == B
    assert B != C
    assert B < C
    assert C != A
    assert not C < A
    assert C > A
    assert not B < B
    assert B >= B
    assert B >= A
    assert sorted([B, A, C]) == [A, B, C]


def test_entity_markup_comparisons() -> None:
    A = EntityMarkup(start=194, end=213, entity_id=3506, label="d3o:Strain")
    B = EntityMarkup(start=294, end=303, entity_id=3502, label="d3o:Enzyme")
    C = EntityMarkup(start=294, end=313, entity_id=3502, label="d3o:Enzyme")

    assert A < B
    assert B == B
    assert B != C
    assert B < C
    assert C != A
    assert not C < A
    assert C > A
    assert not B < B
    assert B >= B
    assert B >= A
    assert sorted([B, A, C]) == [A, B, C]
