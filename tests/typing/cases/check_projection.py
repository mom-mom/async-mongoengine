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

from typing import TYPE_CHECKING, Any, ClassVar, assert_type, overload

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


class PublishedQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
    """Custom queryset whose methods stay visible after a projection switch.

    The three projection methods are re-declared under ``TYPE_CHECKING`` so
    that they return this class; the runtime implementation is inherited.
    This is the pattern documented in ``docs/typing.md``.
    """

    def published(self) -> "PublishedQuerySet[T, R, PK]":
        return self.filter(published=True)

    if TYPE_CHECKING:

        def as_pymongo(self) -> "PublishedQuerySet[T, dict[str, Any], PK]": ...

        @overload
        def scalar(self) -> "PublishedQuerySet[T, T, PK]": ...
        @overload
        def scalar(self, field: str, /) -> "PublishedQuerySet[T, Any, PK]": ...
        @overload
        def scalar(self, field1: str, field2: str, /, *fields: str) -> "PublishedQuerySet[T, tuple[Any, ...], PK]": ...
        def scalar(self, *fields: str) -> "PublishedQuerySet[T, Any, PK]": ...

        @overload
        def values_list(self) -> "PublishedQuerySet[T, T, PK]": ...
        @overload
        def values_list(self, field: str, /) -> "PublishedQuerySet[T, Any, PK]": ...
        @overload
        def values_list(
            self, field1: str, field2: str, /, *fields: str
        ) -> "PublishedQuerySet[T, tuple[Any, ...], PK]": ...
        def values_list(self, *fields: str) -> "PublishedQuerySet[T, Any, PK]": ...


class PlainQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
    """Custom queryset without the re-declaration: a switch falls back to ``QuerySet``."""

    def published(self) -> "PlainQuerySet[T, R, PK]":
        return self.filter(published=True)


class Post(Document):
    title = StringField()

    if TYPE_CHECKING:
        objects: ClassVar[PublishedQuerySet["Post"]]

    meta = {"queryset_class": PublishedQuerySet}


class PlainPost(Document):
    title = StringField()

    if TYPE_CHECKING:
        objects: ClassVar[PlainQuerySet["PlainPost"]]

    meta = {"queryset_class": PlainQuerySet}


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


async def dynamic_field_lists(query: QuerySet[Item], names: list[str]) -> None:
    # Documented limitation (docs/typing.md): Pyright cannot tell an unpacked
    # list of unknown length from two or more literal fields, so a dynamic
    # field list is typed as the tuple form even though a 0- or 1-element
    # list yields documents or single values at runtime.
    assert_type(query.scalar(*names), QuerySet[Item, tuple[Any, ...], ObjectId])
    assert_type(await query.scalar(*names).to_list(), list[tuple[Any, ...]])
    assert_type(await query.values_list(*names).first(), tuple[Any, ...] | None)


async def custom_querysets_after_a_projection_switch() -> None:
    # With the re-declaration the subclass survives the switch ...
    assert_type(Post.objects, PublishedQuerySet[Post, Post, ObjectId])
    assert_type(Post.objects.as_pymongo(), PublishedQuerySet[Post, dict[str, Any], ObjectId])
    assert_type(Post.objects.as_pymongo().published(), PublishedQuerySet[Post, dict[str, Any], ObjectId])
    assert_type(await Post.objects.as_pymongo().published().first(), dict[str, Any] | None)
    assert_type(await Post.objects.published().as_pymongo().to_list(), list[dict[str, Any]])
    assert_type(Post.objects.scalar("title").published(), PublishedQuerySet[Post, Any, ObjectId])
    assert_type(await Post.objects.scalar("title", "id").published().first(), tuple[Any, ...] | None)
    assert_type(await Post.objects.values_list("title").published().to_list(), list[Any])
    assert_type(Post.objects.scalar("title").scalar().published(), PublishedQuerySet[Post, Post, ObjectId])
    assert_type(await Post.objects.as_pymongo().scalar().published().first(), Post | None)

    # ... without it the type falls back to the plain QuerySet.
    assert_type(PlainPost.objects.published(), PlainQuerySet[PlainPost, PlainPost, ObjectId])
    assert_type(PlainPost.objects.as_pymongo(), QuerySet[PlainPost, dict[str, Any], ObjectId])
    assert_type(PlainPost.objects.scalar("title"), QuerySet[PlainPost, Any, ObjectId])
    PlainPost.objects.as_pymongo().published()  # expect-error: reportAttributeAccessIssue "published"
    PlainPost.objects.scalar("title").published()  # expect-error: reportAttributeAccessIssue "published"
    PlainPost.objects.values_list("title").published()  # expect-error: reportAttributeAccessIssue "published"


async def document_producers_ignore_the_mode(query: QuerySet[Item], item: Item) -> None:
    # insert(), upsert_one() and from_json() produce model instances in any mode.
    assert_type(await query.as_pymongo().insert(item), Item)
    assert_type(await query.as_pymongo().insert([item, item]), list[Item])
    assert_type(await query.scalar("name", "count").insert([item]), list[Item])
    assert_type(await query.scalar("name").insert(item), Item)
    assert_type(await query.scalar("name").insert(item, load_bulk=False), ObjectId)
    assert_type(await query.as_pymongo().upsert_one(set__name="x"), Item)
    assert_type(await query.scalar("name").upsert_one(set__name="x"), Item)
    assert_type(await Coded.objects.as_pymongo().upsert_one(set__name="x"), Coded)
    assert_type(query.as_pymongo().from_json("[]"), list[Item])
    wrong: dict[str, Any] = await query.as_pymongo().upsert_one(set__name="x")  # expect-error: reportAssignmentType
    _ = wrong


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


async def the_last_mode_switch_wins(query: QuerySet[Item]) -> None:
    # The projection modes are mutually exclusive: the last switch wins at
    # runtime as well as statically (see test_queryset_9_bulk_json.py).
    assert_type(await query.as_pymongo().scalar("name").first(), Any | None)
    assert_type(await query.scalar("name").as_pymongo().first(), dict[str, Any] | None)
    assert_type(await query.as_pymongo().scalar().first(), Item | None)
    assert_type(await query.scalar("name", "count").as_pymongo().to_list(), list[dict[str, Any]])
    assert_type(await query.as_pymongo().values_list("name", "count").first(), tuple[Any, ...] | None)


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
