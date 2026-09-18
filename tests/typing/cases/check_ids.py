"""Consumer type regression cases: the ``Document[PK]`` id / pk contract.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``.
"""

import datetime
import uuid
from typing import TYPE_CHECKING, Any, ClassVar, assert_type

from bson import ObjectId

from mongoengine import (
    Document,
    DynamicDocument,
    EmbeddedDocument,
    IntField,
    QuerySet,
    StringField,
    UUIDField,
)
from mongoengine.base import BaseField


class Item(Document):
    name = StringField()

    meta = {"allow_inheritance": True}


class Sub(Item):
    extra = StringField()


class Dyn(DynamicDocument):
    label = StringField()


class Coded(Document[str]):
    code = StringField(primary_key=True)


class IdNamed(Document[str]):
    id = StringField(primary_key=True)


class Numbered(Document[int]):
    id = IntField(primary_key=True)


class Tagged(DynamicDocument[uuid.UUID]):
    id = UUIDField(primary_key=True)


class NotOptedIn(Document):
    # Documented limitation: a custom primary key on a model that does not
    # subclass ``Document[str]`` keeps the default ``ObjectId | None`` typing
    # for ``id`` / ``pk`` (the field itself is typed normally).
    code = StringField(primary_key=True)


class NotOptedInId(Document):
    # Naming the custom primary key ``id`` without opting in conflicts with
    # the inherited ``id: BaseField[ObjectId, None]`` declaration.
    id = StringField(primary_key=True)  # expect-error: reportAssignmentType


class Address(EmbeddedDocument):
    city = StringField()


class AnyPkQuerySet[T: Document[Any]](QuerySet[T]):
    """A custom QuerySet usable with models of any primary-key type."""

    def active(self) -> "AnyPkQuerySet[T]":
        return self.filter(active=True)


class Post(Document[str]):
    slug = StringField(primary_key=True)

    if TYPE_CHECKING:
        objects: ClassVar[AnyPkQuerySet["Post"]]

    meta = {"queryset_class": AnyPkQuerySet}


async def default_ids(item: Item, sub: Sub, dyn: Dyn) -> None:
    assert_type(item.id, ObjectId | None)
    assert_type(item.pk, ObjectId | None)
    assert_type(sub.id, ObjectId | None)
    assert_type(sub.pk, ObjectId | None)
    assert_type(dyn.id, ObjectId | None)
    assert_type(dyn.pk, ObjectId | None)
    if item.id is not None:
        assert_type(item.id, ObjectId)
        assert_type(item.id.generation_time, datetime.datetime)
    # Class-level access yields the (metaclass-injected) field.
    assert_type(Item.id, BaseField[ObjectId, None])
    assert_type(Item.id.db_field, str | None)


async def query_results() -> None:
    created = await Item.objects.create(name="x")
    assert_type(created.id, ObjectId | None)
    assert_type(created.pk, ObjectId | None)
    first = await Item.objects.first()
    if first is not None:
        assert_type(first.id, ObjectId | None)
    got = await Item.objects.get(name="x")
    assert_type(got.id, ObjectId | None)
    sub = await Sub.objects.get(extra="e")
    assert_type(sub.id, ObjectId | None)
    async for dyn in Dyn.objects:
        assert_type(dyn.id, ObjectId | None)


async def default_id_assignment(item: Item) -> None:
    item.id = ObjectId()
    item.id = None
    item.pk = ObjectId()
    item.pk = None
    item.id = "x"  # expect-error: reportAttributeAccessIssue "id"
    item.pk = "x"  # expect-error: reportAttributeAccessIssue "pk"


async def custom_primary_keys(coded: Coded, id_named: IdNamed, numbered: Numbered, tagged: Tagged) -> None:
    assert_type(coded.id, str | None)
    assert_type(coded.pk, str | None)
    assert_type(coded.code, str | None)
    assert_type(id_named.id, str | None)
    assert_type(id_named.pk, str | None)
    # Class-level access uses the inherited ``id`` declaration, with PK applied.
    assert_type(IdNamed.id, BaseField[str, None])
    assert_type(numbered.id, int | None)
    assert_type(numbered.pk, int | None)
    assert_type(tagged.id, uuid.UUID | None)
    assert_type(tagged.pk, uuid.UUID | None)
    coded.pk = "abc"
    coded.id = "abc"
    id_named.id = "abc"
    coded.id = ObjectId()  # expect-error: reportAttributeAccessIssue "id"
    numbered.pk = "1"  # expect-error: reportAttributeAccessIssue "pk"
    created = await Coded.objects.create(code="k")
    assert_type(created.id, str | None)
    loaded = await IdNamed.objects.get(id="k")
    assert_type(loaded.pk, str | None)


async def not_opted_in(doc: NotOptedIn) -> None:
    assert_type(doc.code, str | None)
    assert_type(doc.id, ObjectId | None)
    assert_type(doc.pk, ObjectId | None)


async def embedded_documents_have_no_id(address: Address) -> None:
    assert_type(address.city, str | None)
    address.id  # expect-error: reportAttributeAccessIssue "id"
    address.pk  # expect-error: reportAttributeAccessIssue "pk"


def accepts_any_pk(doc: Document[Any]) -> None:
    _ = doc.pk


def accepts_object_id_pk(doc: Document) -> None:
    assert_type(doc.id, ObjectId | None)


async def pk_agnostic_parameters(item: Item, sub: Sub, dyn: Dyn, coded: Coded) -> None:
    accepts_any_pk(item)
    accepts_any_pk(sub)
    accepts_any_pk(dyn)
    accepts_any_pk(coded)
    accepts_object_id_pk(item)
    accepts_object_id_pk(sub)
    accepts_object_id_pk(dyn)
    # Bare ``Document`` means ``Document[ObjectId]``; custom-PK models need ``Document[Any]``.
    accepts_object_id_pk(coded)  # expect-error: reportArgumentType


async def custom_queryset_with_custom_pk() -> None:
    assert_type(Post.objects, AnyPkQuerySet[Post])
    assert_type(await Post.objects.active().first(), Post | None)
    post = await Post.objects.get(slug="x")
    assert_type(post.id, str | None)
    assert_type(post.pk, str | None)
