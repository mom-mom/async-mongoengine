# async-mongoengine Migration Reference

This document covers **all API changes** introduced when converting mongoengine (sync) to async-mongoengine.
Use this as the authoritative reference when updating tests, documentation, or writing a user migration guide.

---

## 1. Infrastructure Changes

### 1.1 PyMongo Driver

| Item | Before | After |
|---|---|---|
| Client | `pymongo.MongoClient` | `pymongo.AsyncMongoClient` |
| Minimum pymongo | `>=3.12, <5.0` | `>=4.10` |
| Minimum Python | `>=3.7` | `>=3.13` |
| Minimum MongoDB | `4.4+` | `7.0+` |
| Package name | `mongoengine` | `async-mongoengine` |
| Motor dependency | None | None (uses PyMongo native async) |

### 1.2 Session Management

| Item | Before | After |
|---|---|---|
| Storage | `threading.local` (deque-based stack) | `contextvars.ContextVar` (tuple-based stack) |
| Functions | `_set_session()`, `_get_session()`, `_clear_session()` | Same (no signature change) |
| Nesting support | Yes (deque stack) | Yes (tuple stack) |

> `threading.local` cannot isolate coroutines in asyncio since they all run on the same thread.
> `ContextVar` provides independent values per async task.

### 1.3 Removed Features

| Feature | Reason |
|---|---|
| `FileField` / `ImageField` (GridFS) | Dropped entirely — unnecessary dependency |
| `exec_js()` | MongoDB `$eval` command removed in MongoDB 4.2 |
| `snapshot()` | No-op since PyMongo 3+, fully removed |
| `_item_frequencies_exec_js()` | Depended on `exec_js` |
| Pillow dependency | Only needed for `ImageField` |

---

## 2. Connection (`mongoengine/connection.py`)

### Async Methods

| Function | Before | After | Notes |
|---|---|---|---|
| `disconnect(alias)` | `def` | `async def` | Requires `await connection.close()` |
| `disconnect_all()` | `def` | `async def` | Calls `await disconnect()` internally |

### Unchanged (remain sync)

`connect()`, `register_connection()`, `get_connection()`, `get_db()`, `_get_connection_settings()`

> `get_connection()` and `get_db()` with `reconnect=True` emit a deprecation warning.
> In async code, call `await disconnect(alias)` then `get_connection(alias)`.

### Removed Code

- pymongo < 4.0 compat code (`PYMONGO_VERSION < (4,)` branches)
- `get_db()` internal `db.authenticate()` call (unnecessary in pymongo 4+)
- UUID representation deprecation warning
- MongoDB version constants `MONGODB_36` through `MONGODB_60`
- All version-gate decorators (`requires_mongodb_lt_42`, `requires_mongodb_gte_*`)

---

## 3. Document (`mongoengine/document.py`)

### `def` → `async def` Conversions

#### Document Instance Methods

```python
# All require await
await doc.save(...)
await doc.delete(...)
await doc.modify(query, **update)
await doc.update(**kwargs)
await doc.reload(*fields, **kwargs)
await doc.select_related(max_depth=1)
await doc.switch_db(db_alias)
await doc.switch_collection(collection_name)
await doc.cascade_save(**kwargs)
```

#### Document Class Methods

```python
await MyDoc._get_collection()
await MyDoc._get_capped_collection()
await MyDoc._get_timeseries_collection()
await MyDoc.drop_collection()
await MyDoc.create_index(keys, **kwargs)
await MyDoc.ensure_indexes()
await MyDoc.list_indexes()
await MyDoc.compare_indexes()
```

#### Internal Methods

```python
await doc._save_create(doc, force_insert, write_concern)
await doc._save_update(doc, save_condition, write_concern)
```

### Unchanged (remain sync)

`_get_db()`, `_disconnect()`, `to_mongo()`, `to_dbref()`, `_get_update_doc()`, `_integrate_shard_key()`, `_reload()`, `validate()`, `clean()`

### Behavioral Changes

#### `save()` Internal Ordering

```
Before: pre_save → validate → to_mongo(id) → to_mongo() → init_collection → DB write
After:  pre_save/pre_save_async → _generate_async_fields (recursive) → validate → pre_save_post_validation/pre_save_post_validation_async → to_mongo() → init_collection → DB write → post_save/post_save_async
```

- **`_generate_async_fields(doc)`**: Module-level async function. Recursively traverses the document and all embedded sub-documents to generate SequenceField values.
- **SequenceField**: `_auto_gen = True` is retained (exempts required-field validation). The sync `_auto_gen` path in `to_mongo()` is skipped via an `inspect.iscoroutinefunction(field.generate)` guard. Actual generation happens in `_generate_async_fields()`.
- This step runs **before** `validate()` so that `SequenceField(primary_key=True)` fields are populated before validation.
- SequenceFields inside embedded documents are processed recursively.

#### `_save_create()` / `_save_update()` Collection Resolution

```python
# Before
collection = self._get_collection()  # Always class-based

# After
collection = self._collection if self._collection is not None else await self.__class__._get_collection()
# Uses instance _collection if set (e.g. via switch_db), otherwise resolves lazily
```

#### `_qs` Property

```python
# Before
self.__objects = queryset_class(self.__class__, self._get_collection())

# After
self.__objects = queryset_class(self.__class__, self.__class__._collection)
# _collection may be None. QuerySet resolves lazily via _ensure_collection()
```

#### `reload()` Changes

```python
# Before
obj = self._qs.filter(...).limit(1).select_related(max_depth=max_depth)
if obj:
    obj = obj[0]

# After
obj = await self._qs.filter(...).limit(1).first()
# select_related() chaining removed (cannot chain async calls)
# Uses `if obj is None:` instead of `if obj:` (QuerySet.__bool__ not supported)
```

#### `switch_db()` / `switch_collection()` Changes

```python
# Before (sync context manager)
with switch_db(cls, alias) as cls: ...
self._get_collection = lambda: collection  # Sync override on instance

# After (async context manager)
async with switch_db(cls, alias) as cls: ...
self._collection = collection  # Lambda override removed
```

#### `MapReduceDocument.object` Property Removed

```python
# Before
doc.object  # sync property, calls with_id() internally

# After — removed
await doc.get_object()  # async method only
```

---

## 4. QuerySet (`mongoengine/queryset/base.py`, `queryset.py`)

### `def` → `async def` Conversions

```python
await qs.get(*q_objs, **query)
await qs.create(**kwargs)
await qs.first()
await qs.insert(doc_or_docs, ...)
await qs.count(with_limit_and_skip=False)
await qs.delete(write_concern=None, ...)
await qs.update(upsert=False, multi=True, ...)
await qs.upsert_one(...)
await qs.update_one(...)
await qs.modify(upsert=False, remove=False, new=False, ...)
await qs.with_id(object_id)
await qs.in_bulk(object_ids)
await qs.distinct(field)
await qs.select_related(max_depth=1)
await qs.using(alias)
await qs.explain()
await qs.to_json(...)
await qs.aggregate(pipeline, **kwargs)
await qs.map_reduce(map_f, reduce_f, output, ...)
await qs.sum(field)
await qs.average(field)
await qs.item_frequencies(field, normalize=False)
await qs.is_empty()          # New
await qs.get_item(index)     # New
```

### Unchanged (remain sync — chaining methods that return QuerySet)

`filter()`, `__call__()`, `all()`, `order_by()`, `limit()`, `skip()`, `hint()`, `collation()`, `batch_size()`, `only()`, `exclude()`, `fields()`, `all_fields()`, `search_text()`, `scalar()`, `values_list()`, `as_pymongo()`, `max_time_ms()`, `comment()`, `no_sub_classes()`, `clear_cls_query()`, `none()`, `timeout()`, `allow_disk_use()`, `read_preference()`, `read_concern()`, `where()`, `clone()`, `no_cache()`, `cache()`

### Magic Method Changes

| Before | After | Notes |
|---|---|---|
| `__iter__()` | `__aiter__()` | `for doc in qs` → `async for doc in qs` |
| `__next__()` | `__anext__()` | `next(qs)` → `await qs.__anext__()` |
| `__len__()` | **Removed** | `len(qs)` → `await qs.count()` |
| `__bool__()` | **Raises TypeError** | `if qs:` → `if not await qs.is_empty():` |
| `__getitem__(int)` | **Raises TypeError** | `qs[0]` → `await qs.get_item(0)` |
| `__getitem__(slice)` | Same (sync) | `qs[1:3]` works as before |
| `rewind()` | Remains sync | No-op when cursor_obj is None |

### New Methods

| Method | Description |
|---|---|
| `async _ensure_collection()` | Calls `await _get_collection()` if `_collection_obj` is None |
| `async is_empty()` | Replaces `__bool__`. Returns True if no results |
| `async get_item(index)` | Replaces `__getitem__(int)`. Retrieves document by index |

### QuerySet Class Changes

#### `QuerySet` (caching)

```python
# Before
def __iter__(self): ...
def __len__(self): ...
def _iter_results(self): ...     # sync generator
def _populate_cache(self): ...

# After
async def __aiter__(self): ...   # Includes _ensure_collection() call
async def _iter_results(self):   # async generator (uses yield)
async def _populate_cache(self):
# __len__ removed
```

#### `QuerySetNoCache` (non-caching)

```python
# Before
def __iter__(self): ...

# After
async def __aiter__(self): ...
async def _get_async_cursor(self): ...  # New helper
```

#### `__repr__()` Changes

```python
# Before: Accesses DB to fetch and display data
# After: Displays cached data only; returns placeholder string if cache is empty
```

### `map_reduce()` Return Type Change

```python
# Before: generator (yield MapReduceDocument)
# After: list (return [MapReduceDocument, ...])
```

### `aggregate()` Return Type

```python
# Before: pymongo CommandCursor (sync iterable)
# After: pymongo AsyncCommandCursor (async iterable)
# Usage: async for doc in await qs.aggregate(pipeline): ...
```

### QuerySetManager (`mongoengine/queryset/manager.py`)

```python
# Before
queryset = queryset_class(owner, owner._get_collection())

# After
queryset = queryset_class(owner, owner._collection)
# _collection may be None → QuerySet._ensure_collection() resolves lazily
```

### Removed Methods

| Method | Reason |
|---|---|
| `exec_js()` | MongoDB `$eval` removed in 4.2 |
| `snapshot()` | No-op since PyMongo 3+ |
| `_item_frequencies_exec_js()` | Depended on `exec_js` |
| `no_dereference()` | No-op without auto-dereference; removed |

`item_frequencies()` no longer accepts a `map_reduce` parameter — it always uses the map_reduce implementation.

`distinct()` on ReferenceField now returns raw PKs instead of dereferenced Document objects, consistent with the removal of auto-dereference.

---

## 5. Fields (`mongoengine/fields.py`)

### 5.1 ReferenceField — Auto-dereference Removed

```python
# Before: __get__ automatically dereferences DBRef to Document
ref_value = instance._data.get(self.name)
if auto_dereference and isinstance(ref_value, DBRef):
    instance._data[self.name] = self._lazy_load_ref(cls, ref_value)

# After: Returns raw value (DBRef or ObjectId) as-is
return super().__get__(instance, owner)
```

**Removed method**: `_lazy_load_ref()` (static method)

**Impact**:
- Accessing `doc.author` returns `DBRef` or `ObjectId` instead of a `Document`
- Explicit dereference required: `author = await Author.objects.get(pk=doc.author.id)`

### 5.2 GenericReferenceField — Auto-dereference Removed

Same as ReferenceField. `_lazy_load_ref()` removed, `__get__` returns raw dict.

```python
# Before: doc.ref → Document instance
# After:  doc.ref → {"_cls": "ClassName", "_ref": DBRef(...)}
```

### 5.3 CachedReferenceField

| Item | Before | After |
|---|---|---|
| `__get__` | auto-dereference | Returns raw value |
| `_lazy_load_ref()` | Exists | **Removed** |
| `to_python(value)` | dict → `db.dereference()` → Document | Returns raw value as-is |
| `start_listener()` | Registers signal (auto_sync) | **No-op** (pass) |
| `sync_all()` | Full cache synchronization | **Removed** |
| `on_document_pre_save()` | Signal handler | **Removed** |

### 5.4 LazyReferenceField / GenericLazyReferenceField

No signature changes. These already return `LazyReference` objects, making them suitable for async.

### 5.5 LazyReference (`mongoengine/base/datastructures.py`)

| Item | Before | After |
|---|---|---|
| `fetch()` | `def fetch()` | `async def fetch()` |
| `__getattr__` (passthrough) | `self.fetch().attr` auto-delegates | **Raises AttributeError** (use `await lazy_ref.fetch()`) |
| `__getitem__` (passthrough) | `self.fetch()[key]` auto-delegates | **Raises KeyError** (use `await lazy_ref.fetch()`) |

```python
# Before
lazy_ref = doc.author  # LazyReference
lazy_ref.name           # passthrough=True auto-fetches and accesses attribute

# After
lazy_ref = doc.author   # LazyReference
doc = await lazy_ref.fetch()
doc.name                # Explicit fetch before access
```

### 5.6 SequenceField

| Item | Before | After |
|---|---|---|
| `_auto_gen` | `True` | `True` retained — exempts required-field validation. `to_mongo()` sync path skipped via `iscoroutinefunction` guard |
| `generate()` | `def` (sync DB call) | `async def` |
| `set_next_value(value)` | `def` (sync DB call) | `async def` |
| `get_next_value()` | `def` (sync DB call) | `async def` |
| `__get__` | Auto-calls `self.generate()` when None | **No auto-generation**. Generated at `save()` time |
| `__set__` | Auto-calls `self.generate()` when None | **No auto-generation** |
| `to_python(value)` | Calls `self.generate()` when None | Returns value as-is |

```python
# Before
doc = MyDoc()
print(doc.seq)  # Auto-generates sequence, e.g.: 1

# After
doc = MyDoc()
print(doc.seq)  # None
await doc.save()  # Auto-generated at save() time
print(doc.seq)  # 1

# Explicit generation also works
doc.seq = await MyDoc.seq.generate()
```

### 5.7 DynamicField

```python
# Before: to_python() calls db.dereference() when _ref is present
# After: Returns raw dict as-is when _ref is present (no dereference)
```

### 5.8 Removed Fields

| Field | Reason |
|---|---|
| `FileField` | GridFS support dropped |
| `ImageField` | GridFS support dropped |
| `GridFSProxy` | GridFS support dropped |
| `ImageGridFsProxy` | GridFS support dropped |
| `GridFSError` | GridFS support dropped |
| `ImproperlyConfigured` | Only used for PIL/ImageField check |

### 5.9 EmbeddedDocumentList (`mongoengine/base/datastructures.py`)

```python
# Before
embedded_list.save()   # def save(): self._instance.save(...)

# After
await embedded_list.save()  # async def save(): await self._instance.save(...)
```

> `EmbeddedDocumentList`'s `filter()`, `exclude()`, `count()`, `get()`, `first()`, `create()`, `delete()`, `update()` are in-memory operations and remain sync.

---

## 6. Context Managers (`mongoengine/context_managers.py`)

### `with` → `async with` Conversions

| Context Manager | Before | After |
|---|---|---|
| `switch_db(cls, alias)` | `with switch_db(...) as cls:` | `async with switch_db(...) as cls:` |
| `switch_collection(cls, name)` | `with switch_collection(...) as cls:` | `async with switch_collection(...) as cls:` |
| `query_counter(alias)` | `with query_counter() as q:` | `async with query_counter() as q:` |
| `run_in_transaction(...)` | `with run_in_transaction():` | `async with run_in_transaction():` |

### Unchanged (remain sync)

`no_sub_classes(cls)`, `set_write_concern(...)`, `set_read_write_concern(...)`

### query_counter Comparison Magic Methods Removed

```python
# Before
with query_counter() as q:
    user.save()
    assert q == 1
    assert q < 5

# After
async with query_counter() as q:
    await user.save()
    assert await q.get_count() == 1
```

**Removed methods**: `__eq__`, `__ne__`, `__lt__`, `__le__`, `__gt__`, `__ge__`, `__int__`
**Added method**: `async def get_count()`

### run_in_transaction Internal Changes

```python
# Before
with conn.start_session(**kwargs) as session:
    with session.start_transaction(**kwargs):

# After
async with conn.start_session(**kwargs) as session:
    await session.start_transaction(**kwargs)
    try:
        yield
        await _commit_with_retry(session)
    except Exception:
        await session.abort_transaction()
        raise
```

---

## 7. DeReference (`mongoengine/dereference.py`)

```python
# Before
DeReference()(items, max_depth=1, instance=None, name=None)  # sync

# After
await DeReference()(items, max_depth=1, instance=None, name=None)  # async
```

Internal changes:
- `__call__`: QuerySet → `[i async for i in items]`, `await self._fetch_objects()`
- `_fetch_objects`: `await collection.objects.in_bulk(refs)`, `async for ref in references`
- `_find_references`: Remains sync (data traversal only)
- `_attach_objects`: Remains sync (data traversal only)

---

## 8. PyMongo Support (`mongoengine/pymongo_support.py`)

| Function | Before | After |
|---|---|---|
| `count_documents(...)` | `def` | `async def` |
| `list_collection_names(...)` | `def` | `async def` |

- All pymongo < 3.7 compat code removed
- `cursor.count()` fallback removed

---

## 9. Signals (`mongoengine/signals.py`)

### Dual Sync/Async Signal System

Each event (except `pre_init`/`post_init`) now has two signals:

| Sync Signal | Async Signal | Emitted From |
|---|---|---|
| `pre_init` | *(none)* | `__init__()` — sync only |
| `post_init` | *(none)* | `__init__()` — sync only |
| `pre_save` | `pre_save_async` | `save()` |
| `pre_save_post_validation` | `pre_save_post_validation_async` | `save()` |
| `post_save` | `post_save_async` | `save()` |
| `pre_delete` | `pre_delete_async` | `delete()` |
| `post_delete` | `post_delete_async` | `delete()` |
| `pre_bulk_insert` | `pre_bulk_insert_async` | `insert()` |
| `post_bulk_insert` | `post_bulk_insert_async` | `insert()` |

- **Sync signals** are emitted via `.send()` — register sync handlers with `.connect()`
- **Async signals** are emitted via `await .send_async()` — register async handlers with `.connect()`
- Both sync and async signals are emitted at each event point in async contexts
- `pre_init` / `post_init` are sync-only because `__init__()` cannot be async

### Migration

```python
# Before (sync handler — still works, no change needed)
signals.pre_save.connect(my_sync_handler, sender=MyDoc)

# New (async handler — use the _async signal)
signals.pre_save_async.connect(my_async_handler, sender=MyDoc)
```

---

## 10. Unsupported Features

| Feature | Reason |
|---|---|
| ReferenceField auto-dereference | Cannot call async DB from `__get__` descriptor |
| GenericReferenceField auto-dereference | Same |
| CachedReferenceField auto-sync (signal) | Removed (use async signal handlers instead) |
| CachedReferenceField.sync_all() | Removed (no async version implemented) |
| SequenceField auto-generate in `__get__`/`__set__` | Cannot call async from descriptor; auto-generated at `save()` |
| LazyReference passthrough | Cannot call async `fetch()` from `__getattr__` |
| `QuerySet.__bool__()` | Cannot be async; raises TypeError |
| `QuerySet.__getitem__(int)` | Cannot be async; raises TypeError |
| `QuerySet.__len__()` | Cannot be async; removed |
| `MapReduceDocument.object` property | Cannot be async; removed (use `get_object()`) |
| `FileField` / `ImageField` | Dropped entirely |
| `exec_js()` | MongoDB `$eval` removed in 4.2 |
| `snapshot()` | No-op since PyMongo 3+ |
| pymongo < 4.10 support | `AsyncMongoClient` requires recent pymongo |
| Python < 3.13 support | Minimum version requirement |
| MongoDB < 7.0 support | Minimum version requirement |
| `get_connection(reconnect=True)` | Deprecated (prevents connection leaks) |
| `no_dereference()` context manager / queryset method | Removed — auto-dereference is disabled; `distinct()` returns raw PKs |

---

## 11. Migration Checklist

Patterns to check when converting sync mongoengine code to async-mongoengine:

### Required Conversions

```python
# 1. Make all test/handler functions async
def test_save(self):  →  async def test_save(self):

# 2. Add await to DB methods
doc.save()            →  await doc.save()
doc.delete()          →  await doc.delete()
doc.update(...)       →  await doc.update(...)
doc.reload()          →  await doc.reload()
MyDoc.objects.get(...)→  await MyDoc.objects.get(...)
MyDoc.objects.first() →  await MyDoc.objects.first()
MyDoc.objects.count() →  await MyDoc.objects.count()
MyDoc.drop_collection() → await MyDoc.drop_collection()

# 3. Iteration
for doc in MyDoc.objects:     →  async for doc in MyDoc.objects:
list(MyDoc.objects)           →  [doc async for doc in MyDoc.objects]

# 4. Indexing
qs[0]                         →  await qs.get_item(0)
qs[1:3]                       →  qs[1:3]  (unchanged)

# 5. Truthiness checks
if qs:                        →  if not await qs.is_empty():
assert qs                     →  assert not await qs.is_empty()

# 6. Length
len(qs)                       →  await qs.count()

# 7. Context managers
with switch_db(Cls, alias):   →  async with switch_db(Cls, alias):
with query_counter() as q:    →  async with query_counter() as q:
    assert q == 1             →      assert await q.get_count() == 1
with run_in_transaction():    →  async with run_in_transaction():

# 8. Disconnect
disconnect()                  →  await disconnect()
disconnect_all()              →  await disconnect_all()

# 9. ReferenceField access
doc.author.name               →  author = await Author.objects.get(pk=doc.author.id)
                                  author.name

# 10. LazyReference
lazy_ref.name                 →  doc = await lazy_ref.fetch()
                                  doc.name

# 11. SequenceField
doc = MyDoc()
assert doc.seq == 1           →  doc = MyDoc()
                                  assert doc.seq is None
                                  await doc.save()
                                  assert doc.seq == 1

# 12. MapReduceDocument
doc.object                    →  await doc.get_object()

# 13. map_reduce (return type)
for doc in qs.map_reduce(...):→  for doc in await qs.map_reduce(...):
# generator → list
```

### pytest Configuration

```ini
# setup.cfg
[tool:pytest]
testpaths = tests
asyncio_mode = auto
asyncio_default_fixture_loop_scope = session
asyncio_default_test_loop_scope = session
```

All async test functions are automatically detected and run with a session-scoped event loop. No need for `@pytest.mark.asyncio` decorators.
