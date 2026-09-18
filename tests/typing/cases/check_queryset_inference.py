"""Consumer type regression cases: QuerySet model inference.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.
"""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, assert_type

from mongoengine import Document, IntField, QuerySet, QuerySetNoCache, StringField


class Item(Document):
    name = StringField(required=True)
    count = IntField()

    meta = {"allow_inheritance": True}

    def label(self) -> str:
        return f"item:{self.name}"


class Sub(Item):
    extra = StringField()


class CustomQuerySet[T: Document](QuerySet[T]):
    def published(self) -> "CustomQuerySet[T]":
        return self.filter(published=True)


class Post(Document):
    title = StringField()

    if TYPE_CHECKING:
        objects: ClassVar[CustomQuerySet["Post"]]

    meta = {"queryset_class": CustomQuerySet}


class CityCount(TypedDict):
    _id: str
    count: int


async def manager_inference() -> None:
    assert_type(Item.objects, QuerySet[Item])
    assert_type(Item.objects(name="x"), QuerySet[Item])
    assert_type(Item.objects.filter(name="x").order_by("-count").limit(5), QuerySet[Item])
    assert_type(Item.objects[1:3], QuerySet[Item])


async def result_types(query: QuerySet[Item]) -> None:
    assert_type(await query.first(), Item | None)
    assert_type(await query.get(name="x"), Item)
    assert_type(await query.to_list(), list[Item])
    assert_type(await query.get_item(0), Item)
    assert_type(await query.modify(set__name="renamed"), Item | None)
    assert_type(await query.create(name="created"), Item)
    assert_type(await query.with_id("some-id"), Item | None)

    async for item in query:
        assert_type(item, Item)

    first = await query.first()
    first.label()  # expect-error: reportOptionalMemberAccess "label"
    if first is not None:
        assert_type(first.label(), str)
        first.missing_method()  # expect-error: reportAttributeAccessIssue "missing_method"

    wrong: Sub = await query.get(name="x")  # expect-error: reportAssignmentType
    _ = wrong


async def subclass_preservation() -> None:
    assert_type(Sub.objects, QuerySet[Sub])
    assert_type(await Sub.objects.first(), Sub | None)
    assert_type(await Sub.objects.filter(extra="e").to_list(), list[Sub])
    sub = await Sub.objects.get(extra="e")
    assert_type(sub.label(), str)


async def custom_queryset_override() -> None:
    assert_type(Post.objects, CustomQuerySet[Post])
    assert_type(Post.objects.published(), CustomQuerySet[Post])
    assert_type(await Post.objects.published().first(), Post | None)
    assert_type(await Post.objects.filter(title="t").published().to_list(), list[Post])
    Post.objects.unpublished()  # expect-error: reportAttributeAccessIssue "unpublished"


async def cache_switching(query: QuerySet[Item]) -> None:
    assert_type(query.no_cache(), QuerySetNoCache[Item])
    assert_type(await query.no_cache().first(), Item | None)
    assert_type(query.no_cache().cache(), QuerySet[Item])


async def aggregation(query: QuerySet[Item]) -> None:
    assert_type(await query.aggregate([]), list[dict[str, Any]])
    assert_type(await query.aggregate([]).to_list(), list[dict[str, Any]])
    assert_type(await query.aggregate([]).typed(CityCount), list[CityCount])
    assert_type(await query.aggregate([]).typed(CityCount).to_list(), list[CityCount])
    async for row in query.aggregate([]).typed(CityCount):
        assert_type(row, CityCount)
        assert_type(row["count"], int)
