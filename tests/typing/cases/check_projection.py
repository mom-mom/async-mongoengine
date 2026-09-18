"""Consumer type regression cases: projection modes (``as_pymongo()``, ``scalar()``).

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``: ``QuerySet[T, R, PK]``
tracks the result type ``R`` separately from the model ``T``.  ``as_pymongo()``
makes ``R`` ``dict[str, Any]``, ``scalar(field)`` makes it ``Any``,
``scalar(f1, f2, ...)`` makes it ``tuple[Any, ...]`` and ``scalar()`` restores
``T``.  The mode is preserved through chaining and cache switching.
"""

from typing import Any, assert_type

from bson import ObjectId

from mongoengine import Document, IntField, QuerySet, QuerySetNoCache, StringField


class Item(Document):
    name = StringField(required=True)
    count = IntField()

    meta = {"allow_inheritance": True}

    def label(self) -> str:
        return f"item:{self.name}"


class Sub(Item):
    extra = StringField()


class Coded(Document[str]):
    code = StringField(primary_key=True)
    name = StringField()


async def document_mode(query: QuerySet[Item]) -> None:
    assert_type(query, QuerySet[Item, Item, ObjectId])
    assert_type(await query.first(), Item | None)
    assert_type(await query.get(name="x"), Item)
    assert_type(await query.get_item(0), Item)
    assert_type(await query.to_list(), list[Item])
    assert_type(await query.with_id("x"), Item | None)
    async for item in query:
        assert_type(item, Item)


async def raw_mode(query: QuerySet[Item]) -> None:
    raw = query.as_pymongo()
    assert_type(raw, QuerySet[Item, dict[str, Any], ObjectId])
    assert_type(await raw.first(), dict[str, Any] | None)
    assert_type(await raw.get(name="x"), dict[str, Any])
    assert_type(await raw.get_item(0), dict[str, Any])
    assert_type(await raw.to_list(), list[dict[str, Any]])
    assert_type(await raw.with_id("x"), dict[str, Any] | None)
    async for row in raw:
        assert_type(row, dict[str, Any])
        assert_type(row["name"], Any)
    first = await raw.first()
    if first is not None:
        assert_type(first.get("name"), Any | None)

    # The mode is preserved through chaining and slicing ...
    assert_type(raw.filter(name="x").order_by("-count")[0:5], QuerySet[Item, dict[str, Any], ObjectId])
    assert_type(query.filter(name="x").as_pymongo().limit(1), QuerySet[Item, dict[str, Any], ObjectId])
    assert_type(await raw.only("name").skip(1).first(), dict[str, Any] | None)
    assert_type(await raw.select_related().first(), dict[str, Any] | None)
    assert_type(await Item.objects.as_pymongo().first(), dict[str, Any] | None)
    assert_type(await Sub.objects.as_pymongo().to_list(), list[dict[str, Any]])
    assert_type(await Coded.objects.as_pymongo().first(), dict[str, Any] | None)

    # ... while queries are still built and written against the model.
    assert_type(await raw.create(name="x"), Item)
    assert_type(await raw.modify(set__name="x"), Item | None)
    assert_type(await raw.count(), int)
    assert_type(await raw.update(set__name="x"), int)


async def scalar_mode(query: QuerySet[Item]) -> None:
    one = query.scalar("name")
    assert_type(one, QuerySet[Item, Any, ObjectId])
    assert_type(await one.first(), Any | None)
    assert_type(await one.get(name="x"), Any)
    assert_type(await one.to_list(), list[Any])
    async for value in one:
        assert_type(value, Any)

    many = query.scalar("name", "count")
    assert_type(many, QuerySet[Item, tuple[Any, ...], ObjectId])
    assert_type(await many.first(), tuple[Any, ...] | None)
    assert_type(await many.get_item(0), tuple[Any, ...])
    assert_type(await many.to_list(), list[tuple[Any, ...]])
    assert_type(query.scalar("name", "count", "id"), QuerySet[Item, tuple[Any, ...], ObjectId])
    async for row in many:
        assert_type(row, tuple[Any, ...])
    pair = await many.first()
    if pair is not None:
        name, count = pair
        assert_type(name, Any)
        assert_type(count, Any)

    # values_list() is an alias of scalar().
    assert_type(query.values_list("name"), QuerySet[Item, Any, ObjectId])
    assert_type(query.values_list("name", "count"), QuerySet[Item, tuple[Any, ...], ObjectId])
    assert_type(query.values_list(), QuerySet[Item, Item, ObjectId])

    # scalar() without fields restores document mode.
    assert_type(query.scalar(), QuerySet[Item, Item, ObjectId])
    assert_type(one.scalar(), QuerySet[Item, Item, ObjectId])
    assert_type(await many.scalar().first(), Item | None)

    # The mode is preserved through chaining.
    assert_type(one.filter(count__gt=1).order_by("name").limit(2), QuerySet[Item, Any, ObjectId])
    assert_type(many.filter(count__gt=1)[1:3], QuerySet[Item, tuple[Any, ...], ObjectId])
    assert_type(await Sub.objects.scalar("extra").to_list(), list[Any])
    assert_type(await Coded.objects.scalar("code", "name").to_list(), list[tuple[Any, ...]])

    # Writes and creation still use the model.
    assert_type(await one.create(name="x"), Item)
    assert_type(await many.modify(set__name="x"), Item | None)


async def cache_switching(query: QuerySet[Item]) -> None:
    assert_type(query.no_cache(), QuerySetNoCache[Item, Item, ObjectId])
    assert_type(query.as_pymongo().no_cache(), QuerySetNoCache[Item, dict[str, Any], ObjectId])
    assert_type(await query.as_pymongo().no_cache().first(), dict[str, Any] | None)
    assert_type(query.no_cache().as_pymongo(), QuerySetNoCache[Item, dict[str, Any], ObjectId])
    assert_type(query.no_cache().scalar("name"), QuerySetNoCache[Item, Any, ObjectId])
    assert_type(query.no_cache().scalar("name", "count"), QuerySetNoCache[Item, tuple[Any, ...], ObjectId])
    assert_type(query.no_cache().values_list("name"), QuerySetNoCache[Item, Any, ObjectId])
    assert_type(query.no_cache().scalar("name").scalar(), QuerySetNoCache[Item, Item, ObjectId])
    assert_type(query.no_cache().scalar("name", "count").cache(), QuerySet[Item, tuple[Any, ...], ObjectId])
    assert_type(query.scalar("name").no_cache().cache(), QuerySet[Item, Any, ObjectId])
    async for row in query.no_cache().as_pymongo():
        assert_type(row, dict[str, Any])
    async for value in query.no_cache().scalar("name"):
        assert_type(value, Any)
    assert_type(await query.no_cache().as_pymongo().get_item(0), dict[str, Any])


async def precedence_follows_the_last_call(query: QuerySet[Item]) -> None:
    # At runtime as_pymongo() takes precedence over scalar() whatever the
    # order; statically the type follows the last call. Do not combine them.
    assert_type(await query.as_pymongo().scalar("name").first(), Any | None)
    assert_type(await query.scalar("name").as_pymongo().first(), dict[str, Any] | None)


async def raw_results_are_not_documents(query: QuerySet[Item]) -> None:
    first = await query.as_pymongo().first()
    if first is not None:
        first.label()  # expect-error: reportAttributeAccessIssue "label"
        first.name  # expect-error: reportAttributeAccessIssue "name"
    got = await query.as_pymongo().get(name="x")
    got.label()  # expect-error: reportAttributeAccessIssue "label"
    async for row in query.as_pymongo():
        row.label()  # expect-error: reportAttributeAccessIssue "label"
    wrong: Item = await query.as_pymongo().get(name="x")  # expect-error: reportAssignmentType
    _ = wrong
    wrong_list: list[Item] = await query.as_pymongo().to_list()  # expect-error: reportAssignmentType
    _ = wrong_list


async def scalar_results_are_not_documents(query: QuerySet[Item]) -> None:
    pair = await query.scalar("name", "count").first()
    if pair is not None:
        pair.label()  # expect-error: reportAttributeAccessIssue "label"
    pairs: list[Item] = await query.scalar("name", "count").to_list()  # expect-error: reportAssignmentType
    _ = pairs
    # A single-field scalar queryset is ``QuerySet[Item, Any, ObjectId]``; ``Any``
    # is assignable to ``QuerySet[Item]``, so only the tuple form is rejected.
    lenient: QuerySet[Item] = query.scalar("name")
    _ = lenient
    wrong: QuerySet[Item] = query.scalar("name", "count")  # expect-error: reportAssignmentType
    _ = wrong
    wrong_raw: QuerySet[Item, Item, ObjectId] = query.as_pymongo()  # expect-error: reportAssignmentType
    _ = wrong_raw
