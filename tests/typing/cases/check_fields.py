"""Consumer type regression cases: field descriptor inference.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``.
"""

import datetime
import decimal
import uuid
from enum import Enum
from typing import Any, Never, assert_type

from bson import DBRef, ObjectId

from mongoengine import (
    BinaryField,
    BooleanField,
    ComplexDateTimeField,
    DateField,
    DateTimeField,
    Decimal128Field,
    DecimalField,
    DictField,
    Document,
    DynamicDocument,
    DynamicField,
    EmailField,
    EmbeddedDocument,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    EnumField,
    FloatField,
    GenericEmbeddedDocumentField,
    GenericLazyReferenceField,
    GenericReferenceField,
    GeoPointField,
    IntField,
    LazyReferenceField,
    ListField,
    MapField,
    ObjectIdField,
    PointField,
    ReferenceField,
    SequenceField,
    SortedListField,
    StringField,
    URLField,
    UUIDField,
)
from mongoengine.base import EmbeddedDocumentList, LazyReference


class Status(Enum):
    NEW = "new"
    DONE = "done"


class Address(EmbeddedDocument):
    city = StringField(required=True)
    zip_code = StringField()
    lines = ListField(StringField())


class Owner(Document):
    name = StringField()


def maybe_str() -> str | None:
    return None


def maybe_tag_list() -> list[str] | None:
    return None


def tag_list() -> list[str]:
    return []


def maybe_score_map() -> dict[str, int] | None:
    return None


def score_map() -> dict[str, int]:
    return {}


def maybe_address_list() -> list[Address] | None:
    return None


def address_list() -> list[Address]:
    return []


def get_flag() -> bool:
    return True


# A ``bool`` of unknown value.  ``null=`` / ``required=`` given as such a
# flag cannot be resolved statically, so the field is typed optional.
FLAG = get_flag()


class Item(Document):
    # Scalars
    name = StringField(required=True)
    nick = StringField()
    url = URLField()
    email = EmailField(required=True)
    count = IntField()
    count_default = IntField(default=0)
    count_factory = IntField(default=lambda: 3)
    count_explicit = IntField(required=False, default=None)
    ratio = FloatField(default=1.0)
    price = DecimalField()
    price128 = Decimal128Field(required=True)
    active = BooleanField(default=False)
    created = DateTimeField(default=datetime.datetime.now)
    updated = DateTimeField()
    birthday = DateField()
    exact = ComplexDateTimeField(required=True)
    payload = BinaryField()
    token = UUIDField(default=uuid.uuid4)
    ref_id = ObjectIdField()
    seq = SequenceField()
    status = EnumField(Status, default=Status.NEW)
    maybe_status = EnumField(Status)
    location = GeoPointField()
    point = PointField()
    anything = DynamicField()
    # Containers (never None: the default is an empty container)
    tags = ListField(StringField())
    nested = ListField(ListField(IntField()))
    untyped = ListField()
    sorted_tags = SortedListField(StringField())
    scores = MapField(IntField())
    extra = DictField()
    typed_extra = DictField(ListField(StringField()))
    # Embedded documents
    address = EmbeddedDocumentField(Address)
    address_req = EmbeddedDocumentField(Address, required=True)
    lazy_address = EmbeddedDocumentField("Address")
    lazy_address_req = EmbeddedDocumentField("Address", required=True)
    addresses = EmbeddedDocumentListField(Address)
    lazy_addresses = EmbeddedDocumentListField("Address")
    generic_embedded = GenericEmbeddedDocumentField()
    # References
    owner = ReferenceField(Owner)
    owner_req = ReferenceField("Owner", required=True)
    generic_ref = GenericReferenceField()
    lazy_owner = LazyReferenceField(Owner)
    lazy_owner_req = LazyReferenceField(Owner, required=True)
    lazy_by_name = LazyReferenceField("Owner")
    generic_lazy = GenericLazyReferenceField()
    # Optionality: null=True always wins, an explicit None default and a
    # factory that may return None stay optional
    nick_null = StringField(null=True)
    name_null = StringField(required=True, null=True)
    count_null_default = IntField(default=0, null=True)
    maybe_nick = StringField(default=maybe_str)
    created_null = DateTimeField(default=datetime.datetime.now, null=True)
    status_null = EnumField(Status, default=Status.NEW, null=True)
    address_null = EmbeddedDocumentField(Address, required=True, null=True)
    point_null = PointField(null=True)
    lazy_owner_default = LazyReferenceField(Owner, default=DBRef("owner", 1))
    lazy_owner_none = LazyReferenceField(Owner, default=None)
    generic_lazy_none = GenericLazyReferenceField(default=None)
    # Documented limitation: the fallback accepts any default (so nullable
    # factories type-check), so a default of the wrong type is not rejected.
    bad_default = IntField(default="x")
    # A non-literal null= is not narrowed (a true null keeps None at runtime);
    # the literal null=False changes nothing.
    nick_flag = StringField(default="x", null=FLAG)
    count_flag = IntField(required=True, null=FLAG)
    created_flag = DateTimeField(default=datetime.datetime.now, null=FLAG)
    seq_flag = SequenceField(required=True, null=FLAG)
    seq_str_flag = SequenceField(value_decorator=str, default="0", null=FLAG)
    tags_flag = ListField(StringField(), null=FLAG)
    extra_flag = DictField(null=FLAG)
    nick_not_null = StringField(default="x", null=False)
    count_not_null = IntField(required=True, null=False)
    tags_not_null = ListField(StringField(), null=False)
    # Containers with an explicit None default or null=True are optional
    tags_none = ListField(StringField(), default=None)
    tags_null = ListField(StringField(), null=True)
    untyped_none = ListField(default=None)
    sorted_none = SortedListField(IntField(), default=None)
    addresses_none = EmbeddedDocumentListField(Address, default=None)
    lazy_addresses_null = EmbeddedDocumentListField("Address", null=True)
    extra_none = DictField(default=None)
    typed_extra_null = DictField(IntField(), null=True)
    scores_none = MapField(IntField(), default=None)
    # Container defaults: a container of the right type, or a factory
    # returning one, keeps the value non-optional; a factory that may return
    # None (or a default of the wrong type) makes it optional.
    tags_factory = ListField(StringField(), default=tag_list)
    tags_list = ListField(StringField(), default=list)
    tags_literal = ListField(StringField(), default=["a"])
    tags_maybe = ListField(StringField(), default=maybe_tag_list)
    tags_bad = ListField(StringField(), default=[1])
    untyped_list = ListField(default=list)
    untyped_maybe = ListField(default=maybe_tag_list)
    sorted_factory = SortedListField(StringField(), default=tag_list)
    sorted_list = SortedListField(StringField(), default=list)
    sorted_maybe = SortedListField(StringField(), default=maybe_tag_list)
    addresses_factory = EmbeddedDocumentListField(Address, default=address_list)
    addresses_list = EmbeddedDocumentListField(Address, default=list)
    addresses_maybe = EmbeddedDocumentListField(Address, default=maybe_address_list)
    lazy_addresses_maybe = EmbeddedDocumentListField("Address", default=maybe_address_list)
    extra_dict = DictField(default=dict)
    extra_maybe = DictField(default=maybe_score_map)
    typed_extra_factory = DictField(IntField(), default=score_map)
    typed_extra_maybe = DictField(IntField(), default=maybe_score_map)
    scores_factory = MapField(IntField(), default=score_map)
    scores_dict = MapField(IntField(), default=dict)
    scores_maybe = MapField(IntField(), default=maybe_score_map)
    # The element type is the exposed Python type of the inner field
    dates = ListField(DateField())
    exacts = ListField(ComplexDateTimeField())
    sorted_dates = SortedListField(DateField())
    date_map = MapField(DateField())
    # SequenceField: the value type follows value_decorator
    seq_req = SequenceField(required=True)
    seq_default = SequenceField(default=0)
    seq_str = SequenceField(value_decorator=str)
    seq_str_req = SequenceField(value_decorator=str, required=True)
    seq_hex_null = SequenceField(value_decorator=hex, null=True)
    seq_named = SequenceField("counters", sequence_name="items", value_decorator=str)

    meta = {"allow_inheritance": True}


class SubItem(Item):
    label = StringField(default="x")


class Dyn(DynamicDocument):
    label = StringField()
    payload = DynamicField(required=True)


async def scalar_fields(item: Item) -> None:
    assert_type(item.name, str)
    assert_type(item.nick, str | None)
    assert_type(item.url, str | None)
    assert_type(item.email, str)
    assert_type(item.count, int | None)
    assert_type(item.count_default, int)
    assert_type(item.count_factory, int)
    assert_type(item.count_explicit, int | None)
    assert_type(item.ratio, float)
    assert_type(item.price, decimal.Decimal | None)
    assert_type(item.price128, decimal.Decimal)
    assert_type(item.active, bool)
    assert_type(item.created, datetime.datetime)
    assert_type(item.updated, datetime.datetime | None)
    assert_type(item.birthday, datetime.date | None)
    assert_type(item.exact, datetime.datetime)
    assert_type(item.payload, bytes | None)
    assert_type(item.token, uuid.UUID)
    assert_type(item.ref_id, ObjectId | None)
    assert_type(item.seq, int | None)
    assert_type(item.status, Status)
    assert_type(item.maybe_status, Status | None)
    assert_type(item.location, list[float] | None)
    assert_type(item.point, Any | None)
    assert_type(item.anything, Any | None)
    assert_type(item.name.upper(), str)


async def optionality(item: Item) -> None:
    # null=True makes the value optional whatever else is passed.
    assert_type(item.nick_null, str | None)
    assert_type(item.name_null, str | None)
    assert_type(item.count_null_default, int | None)
    assert_type(item.created_null, datetime.datetime | None)
    assert_type(item.status_null, Status | None)
    assert_type(item.address_null, Address | None)
    assert_type(item.point_null, Any | None)
    # A factory that may return None falls through to the optional overload.
    assert_type(item.maybe_nick, str | None)
    # An explicit None default stays optional; a non-None default narrows.
    assert_type(item.lazy_owner_none, LazyReference[Owner] | None)
    assert_type(item.lazy_owner_default, LazyReference[Owner])
    assert_type(item.generic_lazy_none, LazyReference[Any] | None)
    assert_type(item.bad_default, int | None)
    # A non-literal null= falls through to the optional overload.
    assert_type(item.nick_flag, str | None)
    assert_type(item.count_flag, int | None)
    assert_type(item.created_flag, datetime.datetime | None)
    assert_type(item.seq_flag, int | None)
    assert_type(item.seq_str_flag, str | None)
    assert_type(item.tags_flag, list[str] | None)
    assert_type(item.extra_flag, dict[str, Any] | None)
    # The literal null=False does not widen.
    assert_type(item.nick_not_null, str)
    assert_type(item.count_not_null, int)
    assert_type(item.tags_not_null, list[str])


async def sequence_fields(item: Item) -> None:
    assert_type(item.seq, int | None)
    assert_type(item.seq_req, int)
    assert_type(item.seq_default, int)
    assert_type(item.seq_str, str | None)
    assert_type(item.seq_str_req, str)
    assert_type(item.seq_hex_null, str | None)
    assert_type(item.seq_named, str | None)
    assert_type(Item.seq, SequenceField[int, None])
    assert_type(Item.seq_str, SequenceField[str, None])
    assert_type(Item.seq_str_req, SequenceField[str, Never])
    assert_type(await Item.seq.generate(), int)
    assert_type(await Item.seq_str.generate(), str)
    assert_type(await Item.seq_str.get_next_value(), str)
    assert_type(await Item.seq_str.set_next_value(3), str)


async def container_fields(item: Item) -> None:
    assert_type(item.tags, list[str])
    assert_type(item.nested, list[list[int]])
    assert_type(item.untyped, list[Any])
    assert_type(item.sorted_tags, list[str])
    assert_type(item.scores, dict[str, int])
    assert_type(item.extra, dict[str, Any])
    assert_type(item.typed_extra, dict[str, list[str]])
    for tag in item.tags:
        assert_type(tag, str)
    for row in item.nested:
        assert_type(row, list[int])
    assert_type(item.scores["a"], int)
    # Explicit default=None / null=True make a container optional.
    assert_type(item.tags_none, list[str] | None)
    assert_type(item.tags_null, list[str] | None)
    assert_type(item.untyped_none, list[Any] | None)
    assert_type(item.sorted_none, list[int] | None)
    assert_type(item.addresses_none, EmbeddedDocumentList[Address] | None)
    assert_type(item.lazy_addresses_null, EmbeddedDocumentList[Any] | None)
    assert_type(item.extra_none, dict[str, Any] | None)
    assert_type(item.typed_extra_null, dict[str, int] | None)
    assert_type(item.scores_none, dict[str, int] | None)
    # A container default of the right type (or a factory returning one) is
    # non-optional; a factory that may return None is optional.
    assert_type(item.tags_factory, list[str])
    assert_type(item.tags_list, list[str])
    assert_type(item.tags_literal, list[str])
    assert_type(item.tags_maybe, list[str] | None)
    assert_type(item.tags_bad, list[str] | None)
    assert_type(item.untyped_list, list[Any])
    assert_type(item.untyped_maybe, list[Any] | None)
    assert_type(item.sorted_factory, list[str])
    assert_type(item.sorted_list, list[str])
    assert_type(item.sorted_maybe, list[str] | None)
    assert_type(item.addresses_factory, EmbeddedDocumentList[Address])
    assert_type(item.addresses_list, EmbeddedDocumentList[Address])
    assert_type(item.addresses_maybe, EmbeddedDocumentList[Address] | None)
    assert_type(item.lazy_addresses_maybe, EmbeddedDocumentList[Any] | None)
    assert_type(item.extra_dict, dict[str, Any])
    assert_type(item.extra_maybe, dict[str, Any] | None)
    assert_type(item.typed_extra_factory, dict[str, int])
    assert_type(item.typed_extra_maybe, dict[str, int] | None)
    assert_type(item.scores_factory, dict[str, int])
    assert_type(item.scores_dict, dict[str, int])
    assert_type(item.scores_maybe, dict[str, int] | None)
    # The element type is the Python type the inner field exposes.
    assert_type(item.dates, list[datetime.date])
    assert_type(item.exacts, list[datetime.datetime])
    assert_type(item.sorted_dates, list[datetime.date])
    assert_type(item.date_map, dict[str, datetime.date])
    for exact in item.exacts:
        assert_type(exact.microsecond, int)


async def embedded_fields(item: Item) -> None:
    assert_type(item.address, Address | None)
    assert_type(item.address_req, Address)
    assert_type(item.address_req.city, str)
    assert_type(item.address_req.zip_code, str | None)
    assert_type(item.address_req.lines, list[str])
    assert_type(item.lazy_address, Any | None)
    assert_type(item.lazy_address_req, Any)
    assert_type(item.addresses, EmbeddedDocumentList[Address])
    assert_type(item.addresses.first(), Address | None)
    assert_type(item.addresses.get(city="x"), Address)
    assert_type(item.addresses.filter(city="x"), EmbeddedDocumentList[Address])
    assert_type(item.addresses.create(city="x"), Address)
    assert_type(item.lazy_addresses, EmbeddedDocumentList[Any])
    assert_type(item.generic_embedded, EmbeddedDocument | None)
    for address in item.addresses:
        assert_type(address, Address)
        assert_type(address.city, str)
    if item.address is not None:
        assert_type(item.address.city, str)


async def embedded_document_fields(address: Address) -> None:
    assert_type(address.city, str)
    assert_type(address.zip_code, str | None)
    assert_type(address.lines, list[str])
    assert_type(Address.city, StringField[Never])


async def reference_fields(item: Item) -> None:
    # Reference fields are ``Any``: the descriptor returns whatever was stored
    # (a document, a DBRef or an ObjectId) without dereferencing.
    assert_type(item.owner, Any | None)
    assert_type(item.owner_req, Any)
    assert_type(item.generic_ref, Any | None)
    assert_type(item.lazy_owner, LazyReference[Owner] | None)
    assert_type(item.lazy_owner_req, LazyReference[Owner])
    assert_type(await item.lazy_owner_req.fetch(), Owner)
    assert_type(item.lazy_by_name, LazyReference[Any] | None)
    assert_type(item.generic_lazy, LazyReference[Any] | None)
    if item.lazy_owner is not None:
        owner = await item.lazy_owner.fetch()
        assert_type(owner.name, str | None)


async def dynamic_documents(dyn: Dyn) -> None:
    assert_type(dyn.label, str | None)
    assert_type(dyn.payload, Any)


async def class_level_access() -> None:
    # Reading a field through the class yields the field instance itself.
    assert_type(Item.name, StringField[Never])
    assert_type(Item.name, StringField[Never, str])  # the hidden value-type parameter defaults to str
    assert_type(Item.nick, StringField[None])
    assert_type(Item.nick_null, StringField[None])
    assert_type(Item.count_default, IntField[Never])
    assert_type(Item.created, DateTimeField[datetime.datetime, Never])
    assert_type(Item.updated, DateTimeField[datetime.datetime, None])
    assert_type(Item.birthday, DateField[None])
    assert_type(Item.exact, ComplexDateTimeField[Never])
    assert_type(Item.tags, ListField[str, Never])
    assert_type(Item.tags_none, ListField[str, None])
    assert_type(Item.tags_maybe, ListField[str, None])
    assert_type(Item.tags_list, ListField[str, Never])
    assert_type(Item.tags_flag, ListField[str, None])
    assert_type(Item.nick_flag, StringField[None])
    assert_type(Item.nick_not_null, StringField[Never])
    assert_type(Item.dates, ListField[datetime.date, Never])
    assert_type(Item.scores_none, MapField[int, None])
    assert_type(Item.addresses_none, EmbeddedDocumentListField[Address, None])
    assert_type(Item.address, EmbeddedDocumentField[Address, None])
    assert_type(Item.addresses, EmbeddedDocumentListField[Address])
    assert_type(Item.status, EnumField[Status, Never])
    assert_type(Item.lazy_owner, LazyReferenceField[Owner, None])
    assert_type(Item.name.db_field, str | None)
    assert_type(Item.name.required, bool)
    assert_type(Item.tags.max_length, int | None)
    _ = Item.name.name


async def inherited_models(sub: SubItem) -> None:
    assert_type(sub.name, str)
    assert_type(sub.nick, str | None)
    assert_type(sub.tags, list[str])
    assert_type(sub.address_req, Address)
    assert_type(sub.label, str)
    assert_type(SubItem.name, StringField[Never])
    assert_type(SubItem.label, StringField[Never])


async def valid_assignments(item: Item) -> None:
    item.name = "renamed"
    item.nick = None
    item.count = 3
    item.count_default = 4
    item.tags = ["a", "b"]
    item.nested = [[1], [2, 3]]
    item.scores = {"a": 1}
    item.address = Address(city="x")
    item.address = None
    item.addresses = [Address(city="x")]
    item.status = Status.DONE
    item.birthday = datetime.date.today()
    item.payload = b"bytes"
    item.payload = bytearray(b"bytes")
    item.lazy_owner = Owner()
    item.owner = Owner()
    item.owner = ObjectId()
    # null=True / explicit None default / nullable factory accept None
    item.nick_null = None
    item.name_null = None
    item.count_null_default = None
    item.maybe_nick = None
    item.lazy_owner_none = None
    item.tags_none = None
    item.tags_none = ["a"]
    item.addresses_none = None
    item.scores_none = None
    item.scores_none = {"a": 1}
    item.tags_maybe = None
    item.tags_maybe = ["a"]
    item.scores_maybe = None
    item.nick_flag = None
    item.count_flag = None
    item.tags_flag = None
    # Values inside containers follow the exposed Python type
    item.exact = datetime.datetime.now()
    item.dates = [datetime.date.today()]
    item.exacts = [datetime.datetime.now()]
    item.sorted_dates = [datetime.date.today()]
    item.date_map = {"today": datetime.date.today()}
    item.seq = 7
    item.seq_str = "7"


async def invalid_declarations() -> None:
    # value_decorator must be passed by keyword for its return type to be inferred.
    SequenceField("counters", None, None, str)  # expect-error: reportArgumentType "value_decorator"
    # MapField requires an inner field (it raises at class definition otherwise).
    MapField()  # expect-error: reportCallIssue


async def optional_access(item: Item) -> None:
    item.nick.upper()  # expect-error: reportOptionalMemberAccess "upper"
    item.address.city  # expect-error: reportOptionalMemberAccess "city"
    item.tags_maybe.append("x")  # expect-error: reportOptionalMemberAccess "append"
    item.tags_flag.append("x")  # expect-error: reportOptionalMemberAccess "append"
    item.nick_flag.upper()  # expect-error: reportOptionalMemberAccess "upper"
    item.scores_maybe["a"]  # expect-error: reportOptionalSubscript
    _ = item.name.missing  # expect-error: reportAttributeAccessIssue "missing"


async def invalid_assignments(item: Item) -> None:
    item.name = 5  # expect-error: reportAttributeAccessIssue "name"
    item.name = None  # expect-error: reportAttributeAccessIssue "name"
    item.email = None  # expect-error: reportAttributeAccessIssue "email"
    item.count = "3"  # expect-error: reportAttributeAccessIssue "count"
    item.count_default = None  # expect-error: reportAttributeAccessIssue "count_default"
    item.tags = [1]  # expect-error: reportAttributeAccessIssue "tags"
    item.tags = None  # expect-error: reportAttributeAccessIssue "tags"
    item.scores = {"a": "1"}  # expect-error: reportAttributeAccessIssue "scores"
    item.address = Owner()  # expect-error: reportAttributeAccessIssue "address"
    item.address_req = None  # expect-error: reportAttributeAccessIssue "address_req"
    item.status = "done"  # expect-error: reportAttributeAccessIssue "status"
    item.nick_null = 5  # expect-error: reportAttributeAccessIssue "nick_null"
    item.tags_none = [1]  # expect-error: reportAttributeAccessIssue "tags_none"
    item.tags_maybe = [1]  # expect-error: reportAttributeAccessIssue "tags_maybe"
    item.tags_list = None  # expect-error: reportAttributeAccessIssue "tags_list"
    item.tags_not_null = None  # expect-error: reportAttributeAccessIssue "tags_not_null"
    item.nick_not_null = None  # expect-error: reportAttributeAccessIssue "nick_not_null"
    item.count_not_null = None  # expect-error: reportAttributeAccessIssue "count_not_null"
    item.scores_none = {"a": "1"}  # expect-error: reportAttributeAccessIssue "scores_none"
    item.exact = "2024,01,01,00,00,00,000000"  # expect-error: reportAttributeAccessIssue "exact"
    item.dates = ["2024-01-01"]  # expect-error: reportAttributeAccessIssue "dates"
    item.exacts = [datetime.date.today()]  # expect-error: reportAttributeAccessIssue "exacts"
    item.date_map = {"today": "2024-01-01"}  # expect-error: reportAttributeAccessIssue "date_map"
    item.seq_str = 7  # expect-error: reportAttributeAccessIssue "seq_str"
    item.seq_req = None  # expect-error: reportAttributeAccessIssue "seq_req"
    item.seq_str_req = None  # expect-error: reportAttributeAccessIssue "seq_str_req"
