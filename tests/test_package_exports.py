import mongoengine
from mongoengine import connection, document, errors, fields, queryset, signals

SUBMODULES = (document, fields, connection, queryset, signals, errors)


def test_dunder_all_is_the_union_of_submodule_exports():
    """``mongoengine.__all__`` is a static tuple; keep it in sync with the submodules."""
    expected: set[str] = set()
    for module in SUBMODULES:
        expected.update(module.__all__)

    assert set(mongoengine.__all__) == expected


def test_dunder_all_has_no_duplicates_and_resolves():
    assert len(mongoengine.__all__) == len(set(mongoengine.__all__))
    for name in mongoengine.__all__:
        assert hasattr(mongoengine, name), name
