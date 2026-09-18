# Static typing

async-mongoengine ships with `py.typed`. This document is the contract for
what a type checker sees: how field values are inferred from model
declarations, and how document primary keys are typed. The reference checker
is Pyright in `basic` mode; the contract is verified by the consumer type
regression cases under `tests/typing/cases/` (`uv run pytest tests/typing`).

All of this is static typing only. No runtime behaviour (validation,
conversion, defaults, the metaclass) changes because of it.

## Model declaration style

Fields are generic descriptors. Declare them **without** a value annotation and
let the checker infer the value type from the field class and its
`required=` / `default=` arguments:

```python
from enum import Enum

from mongoengine import *


class Status(Enum):
    NEW = "new"
    DONE = "done"


class Address(EmbeddedDocument):
    city = StringField(required=True)
    zip_code = StringField()


class User(Document):
    name = StringField(required=True)                # user.name: str
    age = IntField()                                 # user.age: int | None
    tags = ListField(StringField())                  # user.tags: list[str]
    address = EmbeddedDocumentField(Address)         # user.address: Address | None
    status = EnumField(Status, default=Status.NEW)   # user.status: Status
```

`name: str = StringField()` is **not supported**: Pyright rejects it
(`reportAssignmentType`, a `StringField` is not a `str`) and it would also hide
the descriptor typing described below. Do not annotate fields.

## Inferred field types

Reading a field through a document instance yields the value type below, plus
`| None` unless the field is non-optional (next section).

| Field | `doc.field` |
|---|---|
| `StringField`, `URLField`, `EmailField` | `str` |
| `IntField`, `SequenceField` | `int` |
| `FloatField` | `float` |
| `DecimalField`, `Decimal128Field` | `decimal.Decimal` |
| `BooleanField` | `bool` |
| `DateTimeField`, `ComplexDateTimeField` | `datetime.datetime` |
| `DateField` | `datetime.date` |
| `BinaryField` | `bytes` |
| `UUIDField` | `uuid.UUID` |
| `ObjectIdField` | `bson.ObjectId` |
| `EnumField(Status)` | `Status` |
| `GeoPointField` | `list[float]` |
| `PointField`, `LineStringField`, `PolygonField`, `MultiPointField`, `MultiLineStringField`, `MultiPolygonField` | `Any` (a GeoJSON `dict` or the raw coordinate list, as stored) |
| `DynamicField` | `Any` |
| `ListField(StringField())` / `ListField()` | `list[str]` / `list[Any]` |
| `SortedListField(IntField())` | `list[int]` |
| `EmbeddedDocumentListField(Address)` / `EmbeddedDocumentListField("Address")` | `EmbeddedDocumentList[Address]` / `EmbeddedDocumentList[Any]` |
| `DictField()` / `DictField(IntField())` | `dict[str, Any]` / `dict[str, int]` |
| `MapField(IntField())` | `dict[str, int]` |
| `EmbeddedDocumentField(Address)` / `EmbeddedDocumentField("Address")` | `Address` / `Any` |
| `GenericEmbeddedDocumentField` | `EmbeddedDocument` |
| `ReferenceField`, `CachedReferenceField`, `GenericReferenceField` | `Any` (see below) |
| `LazyReferenceField(Owner)` / `LazyReferenceField("Owner")` | `LazyReference[Owner]` / `LazyReference[Any]` |
| `GenericLazyReferenceField` | `LazyReference[Any]` |

Notes:

- **Container fields are never `None`.** `ListField`, `SortedListField`,
  `EmbeddedDocumentListField`, `DictField` and `MapField` default to an empty
  container, so `user.tags` is `list[str]`, not `list[str] | None`. The inner
  type comes from the inner field (`ListField(ListField(IntField()))` is
  `list[list[int]]`; `DictField(ListField(StringField()))` is
  `dict[str, list[str]]`).
- **Embedded documents named by string** (`EmbeddedDocumentField("Address")`,
  used for forward and recursive references) cannot be resolved statically and
  are `Any`.
- **`EmbeddedDocumentList[T]`** is a `list[T]` with the query helpers typed:
  `first() -> T | None`, `get(**kw) -> T`, `filter(**kw) -> EmbeddedDocumentList[T]`,
  `create(**values) -> T`.
- **Reference fields are `Any` on purpose.** In async mode the descriptor
  cannot dereference on access, so `doc.owner` is whatever was stored or
  assigned last: a document instance, a `DBRef` or an `ObjectId`. A document
  type would be a lie. Use `LazyReferenceField` when you want a typed handle:
  `await doc.owner.fetch()` is typed as the referenced document class.
- **`EnumField`** exposes the enum class. Raw values (`"done"`) are still
  converted at runtime, but the static contract for assignment is the enum
  member (`user.status = Status.DONE`).
- **Dynamic documents.** Declared fields on a `DynamicDocument` are typed like
  any other field. Undeclared attributes are created at runtime: assigning one
  is accepted by the checker (documents define `__setattr__`), but reading it
  back is an attribute error. Declare a `DynamicField()` for an attribute you
  want to read statically (it is `Any`).

## Optional vs. required values

Every field is `V | None` unless the declaration guarantees a value:

```python
class User(Document):
    name = StringField()                       # str | None
    email = StringField(required=True)         # str
    nick = StringField(default="")             # str
    joined = DateTimeField(default=datetime.datetime.now)  # datetime (callable default)
    score = IntField(default=None)             # int | None (a None default does not narrow)
    active = BooleanField(required=flag)       # bool | None (non-literal `required`)
```

The rule, applied per field class:

1. `required=True` (the literal) makes the value non-optional.
2. A non-`None` `default=` (a value or a zero-argument callable returning one)
   makes the value non-optional.
3. Anything else, including `primary_key=True`, leaves the value optional.

**Read this honestly.** `required=True` and `default=` change only the *static*
type. `required` is enforced when the document is validated or saved, so
`User().email` is still `None` at runtime until you assign it. A default is
applied whenever the field is left unset or assigned `None`, so a defaulted
field does hold a value in practice.

`null=True` is **not modelled**. A field declared with `default=` and
`null=True` may hold `None` at runtime after an explicit `None` assignment; its
static type stays non-optional.

Assignments are checked against the same type: `user.email = None` and
`user.tags = [1]` are errors, `user.name = None` is fine. `BinaryField` also
accepts `bytearray`; `LazyReferenceField` accepts a document, a `DBRef`, a
`LazyReference` or a primary key (it is normalised on the next read).

## Class-level access

Reading a field through the **class** yields the field instance, so field
metadata stays available:

```python
User.name             # StringField[Never]  (Never: non-optional)
User.age              # IntField[None]
User.tags             # ListField[str, Never]
User.address          # EmbeddedDocumentField[Address, None]
User.name.db_field    # str | None
User.name.required    # bool
```

The second type parameter records optionality: `None` for optional fields,
`Never` for non-optional ones. It is normally inferred; you only spell it out
when subclassing a field (below).

## Inherited models and mixins

Fields declared on a base document (including abstract bases and plain mixin
classes) keep their types on subclasses; `Sub.objects.first()` yields `Sub`,
and `sub.name` is typed exactly as on the base.

## Custom field classes

- A class that subclasses `BaseField` (or `ComplexBaseField`) directly, without
  declaring type parameters, is typed `Any` on read and accepts anything on
  write, exactly as before.
- A class that subclasses a concrete field inherits its value type but not the
  `required=` / `default=` narrowing: `class Slug(StringField)` is always
  `str | None`. Subclass `StringField[Never]` for a field that is always
  present, or declare the class generic and repeat the constructor overloads of
  the parent if you need per-instance narrowing (see `StringField` in
  `mongoengine/fields.py` for the shape).

## Document IDs: `Document[PK]`

`Document` and `DynamicDocument` are generic over the primary-key type, with
`ObjectId` as the default:

```python
class Item(Document):            # same as Document[ObjectId]
    name = StringField()

item = Item()
item.id                          # ObjectId | None  (None until saved)
item.pk                          # ObjectId | None
(await Item.objects.get(name="x")).id   # ObjectId | None
Item.id                          # BaseField[ObjectId, None] (the injected field)
```

`id` and `pk` are `PK | None` because an unsaved document has no id yet; narrow
with `if item.id is not None:` where you need the `ObjectId`.

**Custom primary keys** opt in by parametrising the base class. The primary-key
field itself is typed like any other field (`str | None` here, since
`primary_key=True` does not narrow):

```python
class Product(Document[str]):
    sku = StringField(primary_key=True)     # product.sku: str | None

product.id                                  # str | None
product.pk                                  # str | None


class Counter(Document[int]):
    id = IntField(primary_key=True)         # naming the key "id" also works


class Session(DynamicDocument[uuid.UUID]):
    id = UUIDField(primary_key=True)
```

Limitations of the ID contract:

- A model with a custom primary key that does **not** opt in
  (`class Product(Document): sku = StringField(primary_key=True)`) keeps the
  default typing: `product.id` and `product.pk` are `ObjectId | None` even
  though the runtime value is a `str`. Opt in with `Document[str]`.
- Naming the custom key `id` **without** opting in
  (`class Product(Document): id = StringField(primary_key=True)`) is reported
  as `reportAssignmentType`, because it conflicts with the inherited
  `id: BaseField[ObjectId, None]` declaration. Opt in with `Document[str]`.
- Class-level `Product.id` uses the inherited declaration
  (`BaseField[str, None]`) even when the model declares the field itself.
- Do not combine `primary_key=True` with `required=True` on a field named `id`;
  `required=True` narrows the field to `StringField[Never]`, which no longer
  matches the `BaseField[str, None]` declaration (`primary_key=True` already
  implies required at runtime).
- `EmbeddedDocument` has no `id` or `pk`; accessing them is an attribute error.

**Bare `Document` means `Document[ObjectId]`.** A parameter annotated
`doc: Document` does not accept a `Document[str]` model. Use `Document[Any]`
for code that handles documents regardless of their primary-key type:

```python
def describe(doc: Document[Any]) -> str:
    return f"{type(doc).__name__}:{doc.pk}"
```

The same applies to **custom `QuerySet` subclasses**: bound the model type
parameter with `Document[Any]` so the queryset can be used by models with any
primary key.

```python
class PublishedQuerySet[T: Document[Any]](QuerySet[T]):
    def published(self) -> "PublishedQuerySet[T]":
        return self.filter(published=True)


class Post(Document[str]):
    slug = StringField(primary_key=True)

    if TYPE_CHECKING:
        objects: ClassVar[PublishedQuerySet["Post"]]

    meta = {"queryset_class": PublishedQuerySet}
```

## QuerySet results

`Model.objects` is a `QuerySet[Model]`; `first()` yields `Model | None`,
`get()` / `create()` yield `Model`, `to_list()` yields `list[Model]` and
`async for` iterates `Model` instances (see
`tests/typing/cases/check_queryset_inference.py`). Result typing for
projections (`as_pymongo()`, `scalar()`), `update()` / `insert()` return values
and `in_bulk()` keys is documented in a follow-up section.

## Limitations summary

- `null=True` is not modelled.
- `required=` / `default=` narrow the static type only; validation happens at
  save time.
- Reference fields (`ReferenceField`, `CachedReferenceField`,
  `GenericReferenceField`) are `Any`.
- String document names (`EmbeddedDocumentField("Address")`,
  `LazyReferenceField("Owner")`) are `Any` / `LazyReference[Any]`.
- A custom-primary-key model that does not subclass `Document[PK]` is typed
  `ObjectId | None`.
- Subclasses of concrete fields do not narrow on `required=` / `default=`
  unless they repeat the constructor overloads.
- The contract is verified with Pyright; mypy is not part of the regression
  suite.
