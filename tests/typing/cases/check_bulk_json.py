"""Consumer type regression cases: ``in_bulk()`` and ``from_json()``.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``:
``in_bulk(ids)`` takes any iterable of the model's primary-key type and
returns ``dict[PK, R]`` (missing ids are absent; the values follow the
projection mode), and ``from_json()`` always builds model instances
(``list[T]``).
"""

from collections.abc import Iterable
from typing import Any, assert_type

from bson import ObjectId

from mongoengine import Document, QuerySet, StringField


class Item(Document):
    name = StringField()

    meta = {"allow_inheritance": True}

    def label(self) -> str:
        return f"item:{self.name}"


class Sub(Item):
    extra = StringField()


class Coded(Document[str]):
    code = StringField(primary_key=True)
    name = StringField()


async def in_bulk_documents(query: QuerySet[Item], ids: list[ObjectId], some_ids: Iterable[ObjectId]) -> None:
    docs = await query.in_bulk(ids)
    assert_type(docs, dict[ObjectId, Item])
    for key, doc in docs.items():
        assert_type(key, ObjectId)
        assert_type(doc, Item)
        assert_type(doc.label(), str)
    # Missing ids are simply absent, so lookups must handle None.
    assert_type(docs.get(ObjectId()), Item | None)
    # Any iterable of primary keys is accepted.
    assert_type(await query.in_bulk((ObjectId(),)), dict[ObjectId, Item])
    assert_type(await query.in_bulk({ObjectId()}), dict[ObjectId, Item])
    assert_type(await query.in_bulk(some_ids), dict[ObjectId, Item])
    assert_type(await query.in_bulk(oid for oid in ids), dict[ObjectId, Item])
    assert_type(await query.in_bulk([]), dict[ObjectId, Item])
    # Through the manager and on subclasses.
    assert_type(await Item.objects.in_bulk(ids), dict[ObjectId, Item])
    assert_type(await Sub.objects.in_bulk(ids), dict[ObjectId, Sub])
    assert_type(await query.filter(name="x").select_related().in_bulk(ids), dict[ObjectId, Item])


async def in_bulk_projections(query: QuerySet[Item], ids: list[ObjectId]) -> None:
    raw = await query.as_pymongo().in_bulk(ids)
    assert_type(raw, dict[ObjectId, dict[str, Any]])
    for row in raw.values():
        assert_type(row["name"], Any)
    assert_type(await query.scalar("name").in_bulk(ids), dict[ObjectId, Any])
    assert_type(await query.scalar("name", "id").in_bulk(ids), dict[ObjectId, tuple[Any, ...]])
    assert_type(await query.values_list("name", "id").in_bulk(ids), dict[ObjectId, tuple[Any, ...]])
    assert_type(await query.scalar("name").scalar().in_bulk(ids), dict[ObjectId, Item])
    assert_type(await query.no_cache().as_pymongo().in_bulk(ids), dict[ObjectId, dict[str, Any]])


async def in_bulk_custom_primary_key(codes: list[str]) -> None:
    assert_type(await Coded.objects.in_bulk(codes), dict[str, Coded])
    assert_type(await Coded.objects.in_bulk(["a", "b"]), dict[str, Coded])
    assert_type(await Coded.objects.as_pymongo().in_bulk(codes), dict[str, dict[str, Any]])
    assert_type(await Coded.objects.scalar("name").in_bulk(codes), dict[str, Any])
    coded = (await Coded.objects.in_bulk(codes)).get("a")
    if coded is not None:
        assert_type(coded.id, str | None)


async def from_json(query: QuerySet[Item]) -> None:
    assert_type(query.from_json("[]"), list[Item])
    assert_type(Item.objects.from_json("[]"), list[Item])
    assert_type(Sub.objects.from_json("[]"), list[Sub])
    assert_type(Coded.objects.from_json("[]"), list[Coded])
    # from_json() always builds documents, whatever the projection mode.
    assert_type(query.as_pymongo().from_json("[]"), list[Item])
    assert_type(query.scalar("name").from_json("[]"), list[Item])
    for doc in query.from_json("[]"):
        assert_type(doc.label(), str)
        assert_type(doc.id, ObjectId | None)


async def wrong_key_types(query: QuerySet[Item], ids: list[ObjectId]) -> None:
    await query.in_bulk(["a"])  # expect-error: reportArgumentType
    await Coded.objects.in_bulk(ids)  # expect-error: reportArgumentType
    await query.in_bulk(ObjectId())  # expect-error: reportArgumentType


async def results_keep_their_types(query: QuerySet[Item], ids: list[ObjectId]) -> None:
    docs = await query.in_bulk(ids)
    docs[ObjectId()].missing  # expect-error: reportAttributeAccessIssue "missing"
    docs.get(ObjectId()).label()  # expect-error: reportOptionalMemberAccess "label"
    raw = await query.as_pymongo().in_bulk(ids)
    raw[ObjectId()].label()  # expect-error: reportAttributeAccessIssue "label"
    wrong: dict[str, Item] = await query.in_bulk(ids)  # expect-error: reportAssignmentType
    _ = wrong
    restored = query.from_json("[]")
    restored[0].missing  # expect-error: reportAttributeAccessIssue "missing"
    wrong_list: list[Sub] = query.from_json("[]")  # expect-error: reportAssignmentType
    _ = wrong_list
    wrong_raw: list[dict[str, Any]] = query.as_pymongo().from_json("[]")  # expect-error: reportAssignmentType
    _ = wrong_raw
