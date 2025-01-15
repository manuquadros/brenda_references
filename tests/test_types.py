import pytest
from brenda_references.brenda_types import Triple


def test_triple_comparisons():
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
