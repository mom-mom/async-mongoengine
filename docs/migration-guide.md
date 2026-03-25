# Migrating from mongoengine to async-mongoengine

This guide is intended for **AI agents** (or developers) converting an existing mongoengine-based project to async-mongoengine. Follow the steps in order. Every code transformation is mechanical and deterministic.

---

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| Python | 3.13+ |
| MongoDB | 7.0+ |
| PyMongo | 4.10+ |

async-mongoengine uses PyMongo's native `AsyncMongoClient` — **Motor is not needed**.

---

## Step 1: Replace the package

```diff
# pyproject.toml / requirements.txt
- mongoengine>=0.27
+ async-mongoengine>=0.1
```

The import name stays the same:

```python
import mongoengine          # unchanged
from mongoengine import *   # unchanged
```

---

## Step 2: Make functions async

Every function that calls a mongoengine DB operation must become `async def` and the call must be `await`ed. This is the core of the migration.

### 2.1 Document instance methods

```python
# Before
doc.save()
doc.delete()
doc.update(set__name="new")
doc.modify(query={}, set__name="new")
doc.reload()
doc.select_related()
doc.cascade_save()
doc.switch_db(db_alias)
doc.switch_collection(collection_name)

# After
await doc.save()
await doc.delete()
await doc.update(set__name="new")
await doc.modify(query={}, set__name="new")
await doc.reload()
await doc.select_related()
await doc.cascade_save()
await doc.switch_db(db_alias)
await doc.switch_collection(collection_name)
```

### 2.2 Document class methods

```python
# Before
MyDoc.drop_collection()
MyDoc.create_index(keys)
MyDoc.ensure_indexes()
MyDoc.list_indexes()
MyDoc.compare_indexes()

# After
await MyDoc.drop_collection()
await MyDoc.create_index(keys)
await MyDoc.ensure_indexes()
await MyDoc.list_indexes()
await MyDoc.compare_indexes()
```

### 2.3 QuerySet terminal methods (require `await`)

These methods hit the database and must be awaited:

```python
# Before → After
qs.get(**query)          → await qs.get(**query)
qs.first()               → await qs.first()
qs.count()               → await qs.count()
qs.create(**kwargs)      → await qs.create(**kwargs)
qs.insert(docs)          → await qs.insert(docs)
qs.delete()              → await qs.delete()
qs.update(**kwargs)      → await qs.update(**kwargs)
qs.update_one(**kwargs)  → await qs.update_one(**kwargs)
qs.upsert_one(**kwargs)  → await qs.upsert_one(**kwargs)
qs.modify(**kwargs)      → await qs.modify(**kwargs)
qs.with_id(id)           → await qs.with_id(id)
qs.in_bulk(ids)          → await qs.in_bulk(ids)
qs.distinct(field)       → await qs.distinct(field)
qs.sum(field)            → await qs.sum(field)
qs.average(field)        → await qs.average(field)
qs.item_frequencies(f)   → await qs.item_frequencies(f)
qs.explain()             → await qs.explain()
qs.to_json()             → await qs.to_json()
qs.aggregate(pipeline)   → qs.aggregate(pipeline)        # Returns AggregationResult (see §3.7)
qs.map_reduce(m, r, out) → await qs.map_reduce(m, r, out)
qs.using(alias)          → await qs.using(alias)
qs.to_list()             → await qs.to_list()          # New
qs.is_empty()            → await qs.is_empty()          # New (replaces `if qs:`)
qs.get_item(index)       → await qs.get_item(index)     # New (replaces `qs[0]`)
```

### 2.4 QuerySet chaining methods (remain sync, NO `await`)

These return a new QuerySet and do NOT touch the database:

```python
# All unchanged — no await needed
qs.filter(**kwargs)
qs.exclude(**kwargs)
qs.order_by(*fields)
qs.limit(n)
qs.skip(n)
qs.only(*fields)
qs.fields(**kwargs)
qs.hint(index)
qs.collation(collation)
qs.batch_size(n)
qs.search_text(text)
qs.scalar(*fields)
qs.values_list(*fields)
qs.as_pymongo()
qs.max_time_ms(ms)
qs.comment(text)
qs.no_sub_classes()
qs.clear_cls_query()
qs.none()
qs.timeout(t)
qs.allow_disk_use(b)
qs.read_preference(pref)
qs.read_concern(concern)
qs.where(js)
qs.clone()
qs.no_cache()
qs.cache()
qs.select_related()
```

A typical chained query combines sync chaining with an async terminal:

```python
# Before
docs = MyDoc.objects.filter(active=True).order_by("-created").limit(10)
first = docs[0]

# After
docs = MyDoc.objects.filter(active=True).order_by("-created").limit(10)
first = await docs.get_item(0)
```

### 2.5 Connection management

```python
# connect() remains sync — no change
connect("mydb", host="localhost", port=27017)

# disconnect is now async
await disconnect()
await disconnect_all()
```

---

## Step 3: Replace removed patterns

### 3.1 Iteration

```python
# Before
for doc in MyDoc.objects:
    process(doc)

docs = list(MyDoc.objects.filter(active=True))

# After
async for doc in MyDoc.objects:
    await process(doc)

docs = await MyDoc.objects.filter(active=True).to_list()
# or
docs = [doc async for doc in MyDoc.objects.filter(active=True)]
```

### 3.2 `len(qs)` — removed

```python
# Before
n = len(MyDoc.objects)

# After
n = await MyDoc.objects.count()
```

### 3.3 `if qs:` / `bool(qs)` — raises TypeError

```python
# Before
if MyDoc.objects.filter(name="Alice"):
    do_something()

# After
if not await MyDoc.objects.filter(name="Alice").is_empty():
    do_something()
```

### 3.4 `qs[index]` — raises TypeError for int index

```python
# Before
doc = MyDoc.objects[0]
doc = MyDoc.objects[2]

# After
doc = await MyDoc.objects.get_item(0)
doc = await MyDoc.objects.get_item(2)

# Slicing still works synchronously
subset = MyDoc.objects[1:3]  # returns QuerySet — unchanged
```

### 3.5 Context managers

```python
# Before
with switch_db(MyDoc, "other") as Cls:
    Cls.objects.first()

with switch_collection(MyDoc, "archive") as Cls:
    Cls.objects.count()

with query_counter() as q:
    doc.save()
    assert q == 1

with run_in_transaction():
    doc1.save()
    doc2.delete()

# After
async with switch_db(MyDoc, "other") as Cls:
    await Cls.objects.first()

async with switch_collection(MyDoc, "archive") as Cls:
    await Cls.objects.count()

async with query_counter() as q:
    await doc.save()
    assert await q.get_count() == 1

async with run_in_transaction():
    await doc1.save()
    await doc2.delete()
```

> `no_sub_classes()`, `set_write_concern()`, `set_read_write_concern()` remain sync `with` statements.

### 3.6 `map_reduce()` — returns list, not generator

```python
# Before
for doc in MyDoc.objects.map_reduce(map_f, reduce_f, "output"):
    process(doc)

# After
results = await MyDoc.objects.map_reduce(map_f, reduce_f, "output")
for doc in results:
    process(doc)
```

### 3.7 `aggregate()` — returns `AggregationResult`

`aggregate()` is no longer a coroutine. It returns an `AggregationResult` that
supports `await` (list), `async for` (streaming), `.to_list()`, `.get_cursor()`,
and `.typed(T)` for type narrowing.

```python
# Before (mongoengine — sync)
for doc in MyDoc.objects.aggregate(pipeline):
    process(doc)

# After — await returns a list
results = await MyDoc.objects.aggregate(pipeline)

# After — async for streams documents
async for doc in MyDoc.objects.aggregate(pipeline):
    await process(doc)

# After — explicit to_list() / get_cursor()
results = await MyDoc.objects.aggregate(pipeline).to_list()
cursor = await MyDoc.objects.aggregate(pipeline).get_cursor()

# After — type narrowing with typed()
class CityCount(TypedDict):
    _id: str
    count: int

results = await MyDoc.objects.aggregate(pipeline).typed(CityCount)
# type checker sees: list[CityCount]
```

### 3.8 `MapReduceDocument.object` property — removed

```python
# Before
obj = mr_doc.object

# After
obj = await mr_doc.get_object()
```

---

## Step 4: Handle reference field changes

**Auto-dereference is removed** for all reference fields. This is the most impactful behavioral change.

### 4.1 ReferenceField

```python
# Before — auto-dereferences on access
class Post(Document):
    author = ReferenceField(User)

post = Post.objects.first()
print(post.author.name)  # User instance, auto-fetched

# After — returns raw DBRef or ObjectId
post = await Post.objects.first()
print(post.author)       # ObjectId or DBRef — NOT a User instance

# Explicit fetch required:
author = await User.objects.get(pk=post.author)
# or if stored as DBRef:
author = await User.objects.get(pk=post.author.id)
print(author.name)
```

### 4.2 GenericReferenceField

```python
# Before
doc.ref  # → Document instance

# After
doc.ref  # → {"_cls": "ClassName", "_ref": DBRef(...)}

# Explicit fetch:
from mongoengine import Document
ref_data = doc.ref
from mongoengine.base import _DocumentRegistry
cls = _DocumentRegistry.get(ref_data["_cls"])
obj = await cls.objects.get(pk=ref_data["_ref"].id)
```

### 4.3 LazyReferenceField

```python
# Before — passthrough attribute access
doc.author_ref.name  # auto-fetches and returns attribute

# After — passthrough removed
lazy_ref = doc.author_ref   # LazyReference object
author = await lazy_ref.fetch()
print(author.name)
```

### 4.4 CachedReferenceField

- Auto-sync signal handler removed — no automatic cache updates
- `sync_all()` method removed
- Returns raw value instead of dereferenced document

### 4.5 Batch dereferencing with `select_related()`

If you need to dereference multiple documents at once:

```python
# On QuerySet (sync chaining, deref on consumption)
async for doc in MyDoc.objects.select_related():
    doc.author.name  # already resolved

doc = await MyDoc.objects.select_related().first()
docs = await MyDoc.objects.select_related().to_list()

# On a single document instance (async)
doc = await MyDoc.objects.first()
await doc.select_related()
doc.author.name  # now resolved
```

---

## Step 5: Handle SequenceField changes

Auto-generation no longer happens on field access. Values are generated at `save()` time.

```python
# Before
doc = MyDoc()
print(doc.seq)  # 1 (auto-generated immediately)

# After
doc = MyDoc()
print(doc.seq)  # None
await doc.save()
print(doc.seq)  # 1 (generated during save)

# Manual generation
doc.seq = await MyDoc.seq.generate()
```

---

## Step 6: Remove usages of dropped features

| Removed Feature | Replacement |
|---|---|
| `FileField` / `ImageField` | Use GridFS directly via PyMongo, or an external storage service |
| `exec_js()` | Use `aggregate()` pipeline |
| `snapshot()` | Remove — was a no-op |
| `no_dereference()` | Remove — auto-dereference is already disabled globally |
| `query_counter` comparison operators (`q == 1`, `q < 5`) | Use `await q.get_count()` and compare the int |

---

## Step 7: Update signals (if used)

Sync signal handlers continue to work. For async handlers, use the new `_async` signal variants:

```python
# Sync handler — unchanged
def on_pre_save(sender, document, **kwargs):
    document.updated_at = datetime.utcnow()

signals.pre_save.connect(on_pre_save, sender=MyDoc)

# Async handler — new
async def on_post_save_async(sender, document, **kwargs):
    await notify_service(document.id)

signals.post_save_async.connect(on_post_save_async, sender=MyDoc)
```

Available async signals: `pre_save_async`, `pre_save_post_validation_async`, `post_save_async`, `pre_delete_async`, `post_delete_async`, `pre_bulk_insert_async`, `post_bulk_insert_async`.

> `pre_init` and `post_init` are sync-only (constructors cannot be async).

---

## Step 8: Configure pytest for async tests

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

With `asyncio_mode = "auto"`, all `async def test_*` functions are automatically recognized. No `@pytest.mark.asyncio` decorator needed.

```python
# Before
class TestUser:
    def test_create_user(self):
        user = User(name="Alice").save()
        assert User.objects.count() == 1

# After
class TestUser:
    async def test_create_user(self):
        await User(name="Alice").save()
        assert await User.objects.count() == 1
```

---

## Quick-reference: Transformation Rules

Use this table to mechanically transform code. Search for the **Before** pattern and replace with the **After** pattern.

| # | Before | After |
|---|---|---|
| 1 | `doc.save()` | `await doc.save()` |
| 2 | `doc.delete()` | `await doc.delete()` |
| 3 | `doc.update(...)` | `await doc.update(...)` |
| 4 | `doc.modify(...)` | `await doc.modify(...)` |
| 5 | `doc.reload()` | `await doc.reload()` |
| 6 | `doc.select_related()` | `await doc.select_related()` |
| 7 | `doc.cascade_save()` | `await doc.cascade_save()` |
| 8 | `doc.switch_db(alias)` | `await doc.switch_db(alias)` |
| 9 | `doc.switch_collection(name)` | `await doc.switch_collection(name)` |
| 10 | `MyDoc.drop_collection()` | `await MyDoc.drop_collection()` |
| 11 | `MyDoc.create_index(keys)` | `await MyDoc.create_index(keys)` |
| 12 | `MyDoc.ensure_indexes()` | `await MyDoc.ensure_indexes()` |
| 13 | `MyDoc.list_indexes()` | `await MyDoc.list_indexes()` |
| 14 | `MyDoc.compare_indexes()` | `await MyDoc.compare_indexes()` |
| 15 | `.objects.get(...)` | `await .objects.get(...)` |
| 16 | `.objects.first()` | `await .objects.first()` |
| 17 | `.objects.count()` | `await .objects.count()` |
| 18 | `.objects.create(...)` | `await .objects.create(...)` |
| 19 | `.objects.insert(...)` | `await .objects.insert(...)` |
| 20 | `.objects.delete()` | `await .objects.delete()` |
| 21 | `.objects.update(...)` | `await .objects.update(...)` |
| 22 | `.objects.update_one(...)` | `await .objects.update_one(...)` |
| 23 | `.objects.upsert_one(...)` | `await .objects.upsert_one(...)` |
| 24 | `.objects.modify(...)` | `await .objects.modify(...)` |
| 25 | `.objects.with_id(...)` | `await .objects.with_id(...)` |
| 26 | `.objects.in_bulk(...)` | `await .objects.in_bulk(...)` |
| 27 | `.objects.distinct(...)` | `await .objects.distinct(...)` |
| 28 | `.objects.sum(...)` | `await .objects.sum(...)` |
| 29 | `.objects.average(...)` | `await .objects.average(...)` |
| 30 | `.objects.item_frequencies(...)` | `await .objects.item_frequencies(...)` |
| 31 | `.objects.aggregate(...)` | `.objects.aggregate(...)` → `AggregationResult` (await/async for) |
| 32 | `.objects.map_reduce(...)` | `await .objects.map_reduce(...)` |
| 33 | `.objects.explain()` | `await .objects.explain()` |
| 34 | `.objects.to_json()` | `await .objects.to_json()` |
| 35 | `.objects.to_list()` | `await .objects.to_list()` |
| 36 | `.objects.using(alias)` | `await .objects.using(alias)` |
| 37 | `for doc in qs:` | `async for doc in qs:` |
| 38 | `list(qs)` | `await qs.to_list()` |
| 39 | `len(qs)` | `await qs.count()` |
| 40 | `if qs:` | `if not await qs.is_empty():` |
| 41 | `qs[0]` | `await qs.get_item(0)` |
| 42 | `with switch_db(...)` | `async with switch_db(...)` |
| 43 | `with switch_collection(...)` | `async with switch_collection(...)` |
| 44 | `with query_counter() as q:` | `async with query_counter() as q:` |
| 45 | `with run_in_transaction():` | `async with run_in_transaction():` |
| 46 | `q == N` (query_counter) | `await q.get_count() == N` |
| 47 | `disconnect()` | `await disconnect()` |
| 48 | `disconnect_all()` | `await disconnect_all()` |
| 49 | `doc.ref_field.attr` (ReferenceField) | `ref = await RefDoc.objects.get(pk=doc.ref_field); ref.attr` |
| 50 | `lazy_ref.attr` (LazyReference) | `doc = await lazy_ref.fetch(); doc.attr` |
| 51 | `mr_doc.object` | `await mr_doc.get_object()` |

---

## Complete Migration Example

### Before (sync mongoengine)

```python
from mongoengine import connect, disconnect, Document, StringField, ReferenceField

class Author(Document):
    name = StringField(required=True)

class Post(Document):
    title = StringField(required=True)
    author = ReferenceField(Author)

def main():
    connect("blog")

    author = Author(name="Alice").save()
    Post(title="Hello World", author=author).save()

    for post in Post.objects.filter(title="Hello World"):
        print(f"{post.title} by {post.author.name}")

    count = Post.objects.count()
    print(f"Total posts: {count}")

    if Post.objects.filter(author=author):
        print("Author has posts")

    disconnect()

main()
```

### After (async-mongoengine)

```python
import asyncio
from mongoengine import connect, disconnect, Document, StringField, ReferenceField

class Author(Document):
    name = StringField(required=True)

class Post(Document):
    title = StringField(required=True)
    author = ReferenceField(Author)

async def main():
    connect("blog")

    author = await Author(name="Alice").save()
    await Post(title="Hello World", author=author).save()

    async for post in Post.objects.filter(title="Hello World"):
        # author is now ObjectId, not a Document — explicit fetch needed
        author_doc = await Author.objects.get(pk=post.author)
        print(f"{post.title} by {author_doc.name}")

    count = await Post.objects.count()
    print(f"Total posts: {count}")

    if not await Post.objects.filter(author=author).is_empty():
        print("Author has posts")

    await disconnect()

asyncio.run(main())
```

### Key changes in the example:

1. `def main()` → `async def main()` + `asyncio.run(main())`
2. All `.save()`, `.count()`, `.disconnect()` calls get `await`
3. `for` → `async for`
4. `post.author.name` (auto-deref) → explicit `await Author.objects.get(pk=post.author)`
5. `if qs:` → `if not await qs.is_empty():`
