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
| `IntField` | `int` |
| `SequenceField()` / `SequenceField(value_decorator=str)` | `int` / `str` (the return type of `value_decorator`) |
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
| `ListField(DateField())` / `ListField(ComplexDateTimeField())` | `list[datetime.date]` / `list[datetime.datetime]` |
| `SortedListField(IntField())` | `list[int]` |
| `EmbeddedDocumentListField(Address)` / `EmbeddedDocumentListField("Address")` | `EmbeddedDocumentList[Address]` / `EmbeddedDocumentList[Any]` |
| `DictField()` / `DictField(IntField())` | `dict[str, Any]` / `dict[str, int]` |
| `MapField(IntField())` / `MapField(DateField())` | `dict[str, int]` / `dict[str, datetime.date]` |
| `EmbeddedDocumentField(Address)` / `EmbeddedDocumentField("Address")` | `Address` / `Any` |
| `GenericEmbeddedDocumentField` | `EmbeddedDocument` |
| `ReferenceField`, `CachedReferenceField`, `GenericReferenceField` | `Any` (see below) |
| `LazyReferenceField(Owner)` / `LazyReferenceField("Owner")` | `LazyReference[Owner]` / `LazyReference[Any]` |
| `GenericLazyReferenceField` | `LazyReference[Any]` |

Notes:

- **Container fields default to an empty container.** `ListField`,
  `SortedListField`, `EmbeddedDocumentListField`, `DictField` and `MapField`
  default to `[]` / `{}`, so `user.tags` is `list[str]`, not
  `list[str] | None`. They become optional exactly like scalar fields (see
  "Optional vs. required values"): with `null=True` or a non-literal `null=`,
  with an explicit `default=None` (`ListField(StringField(), default=None)`
  keeps `None` at runtime), or with a `default=` factory that may return
  `None`. A default that is a container of the right type, or a factory
  returning one (`default=list`, `default=["a"]`), keeps them non-optional.
  The inner type is the Python type the
  inner field exposes, whatever it stores: `ListField(ListField(IntField()))`
  is `list[list[int]]`, `DictField(ListField(StringField()))` is
  `dict[str, list[str]]`, `ListField(DateField())` is `list[datetime.date]`
  and `ListField(ComplexDateTimeField())` is `list[datetime.datetime]`
  (although it is stored as a string).
- **`SequenceField`** exposes the return type of `value_decorator` (`int`
  without one), so `SequenceField(value_decorator=str)` is `str`, and
  `await Model.counter.generate()` returns the same type. Pass
  `value_decorator` as a keyword argument for the type to be inferred.
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
def maybe_nick() -> str | None: ...
def maybe_tags() -> list[str] | None: ...
def get_flag() -> bool: ...


class User(Document):
    name = StringField()                       # str | None
    email = StringField(required=True)         # str
    nick = StringField(default="")             # str
    slug = StringField(default="", null=False) # str
    joined = DateTimeField(default=datetime.datetime.now)  # datetime (callable default)
    score = IntField(default=None)             # int | None (a None default does not narrow)
    alias = StringField(default=maybe_nick)    # str | None (the factory may return None)
    label = StringField(default="", null=True) # str | None (null=True always wins)
    title = StringField(default="", null=get_flag())  # str | None (non-literal `null`)
    active = BooleanField(required=get_flag()) # bool | None (non-literal `required`)
    tags = ListField(StringField())            # list[str] (empty-container default)
    names = ListField(StringField(), default=list)         # list[str] (list factory)
    aliases = ListField(StringField(), default=None)       # list[str] | None
    labels = ListField(StringField(), default=maybe_tags)  # list[str] | None
```

The rule, applied per field class, in this order:

1. `null=True` (the literal) makes the value optional whatever else is passed:
   such a field may hold `None` after an explicit `None` assignment even when
   it has a default.
2. `required=True` (the literal), with `null` absent or `False`, makes the
   value non-optional.
3. A `default=` whose value, or whose zero-argument factory's return type, is
   the field's value type, with `null` absent or `False`, makes the value
   non-optional.
4. Anything else leaves the value optional: `null=` given as a non-literal
   `bool` (at runtime a true `null` keeps `None`, so the checker cannot
   promise a value), an explicit `default=None`, a factory that may return
   `None` (`default=maybe_nick`), a default of the wrong type, a non-literal
   `required=` without a `default=`, or `primary_key=True` alone. A
   non-literal `required=` is ignored by the checker: with a `default=` that
   satisfies rule 3 the field is still non-optional.

Container fields (`ListField`, `SortedListField`, `EmbeddedDocumentListField`,
`DictField`, `MapField`) follow the same rule, with their implicit
empty-container default counting as rule 3 when `default=` is absent. So
`ListField(StringField())`, `ListField(StringField(), default=list)` and
`ListField(StringField(), default=["a"])` are `list[str]`, while an explicit
`default=None` (kept as is at runtime, unlike a scalar field's `None` default),
a factory that may return `None` or a non-literal `null=` make the field
`list[str] | None`.

**Read this honestly.** `required=True` and `default=` change only the *static*
type. `required` is enforced when the document is validated or saved, so
`User().email` is still `None` at runtime until you assign it. A default is
applied whenever the field is left unset or assigned `None`, so a defaulted
field does hold a value in practice.

Because the fallback (rule 4) accepts any `default=`, a default of the wrong
type is **not rejected**: `IntField(default="x")` and
`ListField(StringField(), default=[1])` type-check and the fields are simply
`int | None` and `list[str] | None`; validation still catches the value at
save time.

Assignments are checked against the same type: `user.email = None` and
`user.tags = [1]` are errors, `user.name = None` and `user.aliases = None` are
fine. `BinaryField` also accepts `bytearray`; `LazyReferenceField` accepts a
document, a `DBRef`, a `LazyReference` or a primary key (it is normalised on
the next read), so assigning `None` to a lazy reference is not reported either.

## Class-level access

Reading a field through the **class** yields the field instance, so field
metadata stays available:

```python
User.name             # StringField[Never]  (Never: non-optional)
User.age              # IntField[None]
User.tags             # ListField[str, Never]
User.aliases          # ListField[str, None]
User.joined           # DateTimeField[datetime.datetime, Never]
User.address          # EmbeddedDocumentField[Address, None]
User.counter          # SequenceField[int, None]
User.name.db_field    # str | None
User.name.required    # bool
```

The last type parameter records optionality: `None` for optional fields,
`Never` for non-optional ones. It is normally inferred; you only spell it out
when subclassing a field (below). Two classes carry an extra, defaulted
value-type parameter so that a subclass can expose another Python type:
`StringField[N, V = str]` (`ComplexDateTimeField` is `StringField[N, datetime.datetime]`)
and `DateTimeField[V = datetime.datetime, N]` (`DateField` is
`DateTimeField[datetime.date, N]`). `StringField[Never]` is the same type as
`StringField[Never, str]`; a plain field never needs it spelled out.

## Inherited models and mixins

Fields declared on a base document (including abstract bases and plain mixin
classes) keep their types on subclasses; `Sub.objects.first()` yields `Sub`,
and `sub.name` is typed exactly as on the base.

## Custom field classes

- A class that subclasses `BaseField` (or `ComplexBaseField`) directly, without
  declaring type parameters, is typed `Any` on read and accepts anything on
  write, exactly as before.
- A class that subclasses a concrete field inherits its value type but not the
  `required=` / `default=` / `null=` narrowing: `class Slug(StringField)` is
  always `str | None`. Subclass `StringField[Never]` for a field that is always
  present, or declare the class generic and repeat the constructor overloads of
  the parent if you need per-instance narrowing (see `StringField` in
  `mongoengine/fields.py` for the shape: `null: Literal[True]`, then
  `required: Literal[True]` with `null: Literal[False] = False`, then a typed
  `default=` with `null: Literal[False] = False`, then the fallback with
  `default: Any = None, null: bool = False`).
- A string-backed field that exposes another Python type subclasses
  `StringField[N, V]` with `V` set, as `ComplexDateTimeField` does
  (`class ComplexDateTimeField[N = None](StringField[N, datetime.datetime])`),
  and overrides `__get__` / `__set__` accordingly.

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


class Ticket(Document[str]):
    # SequenceField follows value_decorator, so the key type is str here.
    id = SequenceField(primary_key=True, value_decorator=str)


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
  implies required at runtime). `null=True` is fine (it keeps `None`).
- The primary-key field's value type must match `PK`:
  `class Counter(Document[int]): id = SequenceField(primary_key=True, value_decorator=str)`
  is reported as `reportAssignmentType`.
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
primary key. `QuerySet` has three type parameters (`QuerySet[T, R, PK]`, see
the next section); a custom queryset for models with an `ObjectId` primary key
only needs the model parameter, while the fully general form repeats all three
so that `insert(..., load_bulk=False)` and `in_bulk()` keys follow a custom
primary key:

```python
class PublishedQuerySet[T: Document[Any]](QuerySet[T]):       # ObjectId models
    def published(self) -> "PublishedQuerySet[T]":
        return self.filter(published=True)


class GeneralQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
    def published(self) -> "GeneralQuerySet[T, R, PK]":
        return self.filter(published=True)


class Post(Document[str]):
    slug = StringField(primary_key=True)

    if TYPE_CHECKING:
        objects: ClassVar[GeneralQuerySet["Post", "Post", str]]

    meta = {"queryset_class": GeneralQuerySet}
```

With `objects: ClassVar[PublishedQuerySet["Post"]]` on a `Document[str]` model
the custom methods are visible but `PK` falls back to `ObjectId`. Managers
declared with `@queryset_manager` are `QuerySetManager[Any]` (the decorator
cannot see the model), so `Model.manager` is `QuerySet[Model, Model, Any]`.

## QuerySet results: `QuerySet[T, R, PK]`

`QuerySet` and `QuerySetNoCache` are generic over three parameters:

| Parameter | Meaning | Default |
|---|---|---|
| `T` | the model the queries are built against; what `create()`, `modify()`, `upsert_one()`, `insert()` and `from_json()` produce | — |
| `R` | the value yielded by *executing* the query: `first()`, `get()`, `get_item()`, `to_list()`, `async for`, `with_id()` and the values of `in_bulk()` | `T` |
| `PK` | the primary-key type of `T`: `insert(..., load_bulk=False)` results and `in_bulk()` keys | `ObjectId` |

`QuerySet[Item]` is short for `QuerySet[Item, Item, ObjectId]`. `Model.objects`
is `QuerySet[Model, Model, PK]` with `PK` taken from `Document[PK]`, so
`Product.objects` is `QuerySet[Product, Product, str]` for
`class Product(Document[str])`.

```python
Item.objects                               # QuerySet[Item, Item, ObjectId]
await Item.objects.first()                 # Item | None
await Item.objects.get(name="x")           # Item
await Item.objects.get_item(0)             # Item
await Item.objects.to_list()               # list[Item]
async for item in Item.objects: ...        # Item
await Item.objects.with_id(some_id)        # Item | None
await Item.objects.create(name="x")        # Item
await Item.objects.modify(set__name="y")   # Item | None
await Item.objects(name="x").upsert_one(set__name="y")  # Item
```

Chainable methods (`filter()`, `order_by()`, slicing, `only()`, `limit()`,
`select_related()`, `no_cache()` / `cache()`, ...) keep all three parameters.

### Projection modes: `as_pymongo()` and `scalar()`

The projection switches change `R` and nothing else:

```python
raw = Item.objects.as_pymongo()               # QuerySet[Item, dict[str, Any], ObjectId]
await raw.first()                             # dict[str, Any] | None
await raw.to_list()                           # list[dict[str, Any]]

one = Item.objects.scalar("name")             # QuerySet[Item, Any, ObjectId]
await one.to_list()                           # list[Any]
many = Item.objects.scalar("name", "count")   # QuerySet[Item, tuple[Any, ...], ObjectId]
await many.first()                            # tuple[Any, ...] | None
Item.objects.scalar("name").scalar()          # QuerySet[Item, Item, ObjectId] (document mode again)
```

`values_list()` is an alias of `scalar()`. The mode survives chaining and
`no_cache()` / `cache()` (`raw.no_cache()` is
`QuerySetNoCache[Item, dict[str, Any], ObjectId]`). Writes and creation keep
using the model: `raw.create(...)` is `Item`, `raw.update(...)` is `int`.

Limits:

- **Field values are not typed by name.** A single scalar is `Any` and several
  are `tuple[Any, ...]`; narrow them yourself.
- **A dynamic field list is typed as the tuple form.** Pyright cannot tell an
  unpacked `list[str]` of unknown length from two or more literal fields, so
  `qs.scalar(*names)` is `QuerySet[Item, tuple[Any, ...], ObjectId]` even
  though a 0- or 1-element list yields documents or single values at
  runtime. Pass the fields literally, or narrow the result yourself.
- **The last mode switch wins**, at runtime as well as statically:
  `qs.as_pymongo().scalar("x")` yields field values,
  `qs.scalar("x").as_pymongo()` yields dicts and `qs.as_pymongo().scalar()`
  is back in document mode. `as_pymongo()` keeps the field selection in
  force, including the one `scalar("x")` made through `only("x")`, so the
  dicts of `qs.scalar("x").as_pymongo()` hold only `_id` and `x`;
  `scalar()` with fields selects its own fields and `scalar()` without
  fields resets the selection. This is a behaviour change: the combination
  used to be unspecified and inconsistent (iteration let `as_pymongo()` win
  whatever the order, `in_bulk()` applied `scalar()` first).
- **Custom queryset classes lose their methods after a switch** unless they
  re-declare the projection methods. `Self` cannot re-parametrise `R`, so
  `as_pymongo()` / `scalar()` / `values_list()` on a custom queryset class
  return the plain `QuerySet[...]` and `Post.objects.as_pymongo().published()`
  is an attribute error. The supported pattern is to re-declare the three
  methods under `if TYPE_CHECKING:` so that they return the subclass (the
  runtime implementation stays inherited; copy the shape from `QuerySet` in
  `mongoengine/queryset/queryset.py`):

  ```python
  from typing import TYPE_CHECKING, Any, ClassVar, overload


  class PublishedQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
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

          # ... and the same three overloads for values_list().


  class Post(Document):
      title = StringField()

      if TYPE_CHECKING:
          objects: ClassVar[PublishedQuerySet["Post"]]

      meta = {"queryset_class": PublishedQuerySet}


  Post.objects.as_pymongo().published()      # PublishedQuerySet[Post, dict[str, Any], ObjectId]
  Post.objects.scalar("title").published()   # PublishedQuerySet[Post, Any, ObjectId]
  ```

### Update results

`update()`, `update_one()` and `Document.update()` are overloaded on
`full_result`:

| Call | Result |
|---|---|
| `update(**changes)` or `update(full_result=False, **changes)` | `int`: the number of **matched** documents (`UpdateResult.matched_count`, not the number modified) |
| `update(full_result=True, **changes)` | `pymongo.results.UpdateResult` (`matched_count`, `modified_count`, `upserted_id`, `did_upsert`) |
| `update(full_result=flag, **changes)` with a runtime `bool` | `int \| UpdateResult`; narrow with `isinstance` |

```python
count = await Item.objects(name="x").update(set__count=1)                # int
result = await Item.objects(name="x").update(full_result=True, inc__count=1)  # UpdateResult
result.matched_count, result.modified_count, result.upserted_id
await item.update(full_result=True, set__count=2)                        # UpdateResult
```

- Querysets that cannot match anything (`none()`, an empty slice such as
  `qs[5:5]`) do not touch the database: they return `0`, or with
  `full_result=True` a zero-count acknowledged `UpdateResult`
  (`matched_count == modified_count == 0`, `upserted_id is None`).
- With an unacknowledged write concern (`w=0`) the server reports nothing:
  the count form returns `None` at runtime and the `UpdateResult` is
  unacknowledged. The static type describes acknowledged writes.
- `Document.update()` forwards to `update_one()`. An unsaved document raises
  `OperationError` unless `upsert=True` is passed, in which case its current
  field values are the upsert query.

### Insert results

`insert()` is overloaded on the input shape and `load_bulk`:

| Call | Result |
|---|---|
| `insert(doc)` or `insert(doc, load_bulk=True)` | `T` (the reloaded document) |
| `insert([doc, ...])` (any `Sequence[T]`) | `list[T]` |
| `insert(doc, load_bulk=False)` | `PK` |
| `insert([doc, ...], load_bulk=False)` | `list[PK]` |
| a runtime `bool` flag | `T \| PK` or `list[T] \| list[PK]` |

```python
item = await Item.objects.insert(Item(name="x"))                  # Item
items = await Item.objects.insert([Item(name="a"), Item(name="b")])  # list[Item]
oid = await Item.objects.insert(Item(name="x"), load_bulk=False)  # ObjectId
sku = await Product.objects.insert(Product(sku="p1"), load_bulk=False)  # str
```

With `load_bulk=True` the documents are reloaded in one `in_bulk()` query. If a
document cannot be reloaded (for example when the queryset reads from a
secondary that has not caught up yet), the in-memory document that was
inserted is returned in its place; its primary key is already set, so the
result never contains `None`. `pre_bulk_insert` / `post_bulk_insert` fire
exactly as before (`loaded=True` / `loaded=False`).

### `in_bulk()` and `from_json()`

- `in_bulk(ids: Iterable[PK]) -> dict[PK, R]`: accepts any iterable of primary
  keys (it is materialised into a list for the `$in` query). Ids that do not
  exist are absent from the result, so use `.get()` when an id may be missing.
  The values follow the projection mode: `dict[ObjectId, Item]` by default,
  `dict[ObjectId, dict[str, Any]]` after `as_pymongo()`, `dict[ObjectId, Any]`
  or `dict[ObjectId, tuple[Any, ...]]` after `scalar()`; keys follow the
  model's primary key (`dict[str, Product]`).
- `from_json(json) -> list[T]`: always builds model instances, whatever the
  projection mode. With inheritance enabled the `_cls` entry reconstructs the
  matching subclass.

These contracts are verified by `tests/typing/cases/check_update.py`,
`check_insert.py`, `check_projection.py` and `check_bulk_json.py`, and at
runtime by `tests/queryset/test_queryset_7_update_result.py`,
`test_queryset_8_insert_result.py` and `test_queryset_9_bulk_json.py`.

## Limitations summary

- `null=True` / `required=` / `default=` narrow the static type only;
  validation happens at save time.
- A `default=` of the wrong type (`IntField(default="x")`) is not rejected; the
  field is then optional. The fallback accepts any default so that factories
  returning `V | None` type-check as optional.
- `null=` given as a non-literal `bool` cannot be resolved statically; such a
  field is optional even when it has a default. A non-literal `required=` is
  ignored: the field is optional unless a `default=` makes it non-optional.
- `SequenceField`'s value type is inferred from `value_decorator` only when it
  is passed as a keyword argument; the checker rejects it passed positionally
  (the runtime still accepts it).
- `MapField()` without an inner field is rejected by the checker (it also
  raises at class definition).
- Assigning `None` to a `LazyReferenceField` / `GenericLazyReferenceField` is
  never reported (their setter accepts any reference form).
- Reference fields (`ReferenceField`, `CachedReferenceField`,
  `GenericReferenceField`) are `Any`.
- String document names (`EmbeddedDocumentField("Address")`,
  `LazyReferenceField("Owner")`) are `Any` / `LazyReference[Any]`.
- A custom-primary-key model that does not subclass `Document[PK]` is typed
  `ObjectId | None`.
- Subclasses of concrete fields do not narrow on `required=` / `default=`
  unless they repeat the constructor overloads.
- `scalar()` values are `Any` (one field) or `tuple[Any, ...]` (several); a
  dynamic field list (`scalar(*names)`) is typed as the tuple form whatever
  its length. The last of `as_pymongo()` / `scalar()` wins, at runtime and
  statically.
- After `as_pymongo()` / `scalar()` a custom queryset class is typed as the
  plain `QuerySet[...]` unless it re-declares the projection methods under
  `TYPE_CHECKING` (see "Projection modes"); `@queryset_manager` managers are
  `QuerySetManager[Any]`.
- The count form of `update()` is `None` at runtime for unacknowledged
  writes (`w=0`); the static type describes acknowledged writes.
- **Primary keys whose stored form differs from their Python type.**
  `insert(..., load_bulk=False)` returns the stored `_id` values as PyMongo
  reports them (and sets the in-memory document's `pk` to the same value),
  and `in_bulk()` matches the ids it is given against the stored values
  without the primary-key field's query conversion. For
  `UUIDField(binary=False)` (stores `str`) or `EnumField` (stores the enum
  value) the runtime values are therefore the stored form while the static
  type is `PK`: with `class Session(Document[uuid.UUID])`,
  `await Session.objects.insert(session, load_bulk=False)` is a `str`,
  `in_bulk([str(session_id)])` matches and `in_bulk([session_id])` returns
  nothing (whereas `get(id=session_id)` converts and matches). Issue #33
  tracks applying the field's `to_python` / `prepare_query_value` conversions;
  until then the current behaviour is pinned by
  `test_insert_returns_the_stored_primary_key_form` and
  `test_in_bulk_matches_stored_primary_key_values`.
- The contract is verified with Pyright; mypy is not part of the regression
  suite.
