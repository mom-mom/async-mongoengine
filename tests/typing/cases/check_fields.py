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

from bson import ObjectId

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
    assert_type(Item.nick, StringField[None])
    assert_type(Item.count_default, IntField[Never])
    assert_type(Item.tags, ListField[str, Never])
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


async def optional_access(item: Item) -> None:
    item.nick.upper()  # expect-error: reportOptionalMemberAccess "upper"
    item.address.city  # expect-error: reportOptionalMemberAccess "city"
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
