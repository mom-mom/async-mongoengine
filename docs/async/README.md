# async-mongoengine Migration Reference

이 문서는 mongoengine(sync)을 async-mongoengine으로 변환하면서 발생한 **모든 API 변경사항**을 정리한다.
테스트코드 변경, 기존 문서 갱신, 사용자 마이그레이션 가이드 작성 시 이 문서를 기준으로 한다.

---

## 1. 기반 인프라 변경

### 1.1 PyMongo 드라이버

| 항목 | Before | After |
|---|---|---|
| 클라이언트 | `pymongo.MongoClient` | `pymongo.AsyncMongoClient` |
| pymongo 최소 버전 | `>=3.12, <5.0` | `>=4.0` |
| Python 최소 버전 | `>=3.7` | `>=3.9` |
| 패키지 이름 | `mongoengine` | `async-mongoengine` |
| Motor 의존성 | 없음 | 없음 (PyMongo 네이티브 async 사용) |

### 1.2 세션 관리

| 항목 | Before | After |
|---|---|---|
| 저장소 | `threading.local` (deque 기반 stack) | `contextvars.ContextVar` (tuple 기반 stack) |
| 함수 | `_set_session()`, `_get_session()`, `_clear_session()` | 동일 (시그니처 변경 없음) |
| 중첩 지원 | O (deque stack) | O (tuple stack) |

> `threading.local`은 asyncio 환경에서 모든 코루틴이 같은 스레드에서 동작하므로 격리 불가.
> `ContextVar`는 각 async task마다 독립된 값을 가짐.

---

## 2. Connection (`mongoengine/connection.py`)

### 변경된 함수

| 함수 | Before | After | 비고 |
|---|---|---|---|
| `disconnect(alias)` | `def` | `async def` | `await connection.close()` 필요 |
| `disconnect_all()` | `def` | `async def` | 내부에서 `await disconnect()` |

### 변경 없는 함수 (sync 유지)

`connect()`, `register_connection()`, `get_connection()`, `get_db()`, `_get_connection_settings()`

> `get_connection()`과 `get_db()`의 `reconnect=True`는 deprecated 경고를 발생시킨다.
> async에서는 `await disconnect(alias)` 후 `get_connection(alias)`을 호출해야 한다.

### 제거된 코드

- pymongo < 4.0 호환 코드 (`PYMONGO_VERSION < (4,)` 분기)
- `get_db()` 내 `db.authenticate()` 호출 (pymongo 4+에서 불필요)
- UUID representation deprecation warning

---

## 3. Document (`mongoengine/document.py`)

### `def` → `async def` 변환된 메서드

#### Document 인스턴스 메서드

```python
# 모두 await 필요
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

#### Document 클래스 메서드

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

#### Document 내부 메서드

```python
await doc._save_create(doc, force_insert, write_concern)
await doc._save_update(doc, save_condition, write_concern)
```

### 변경 없는 메서드 (sync 유지)

`_get_db()`, `_disconnect()`, `to_mongo()`, `to_dbref()`, `_get_update_doc()`, `_integrate_shard_key()`, `_reload()`, `validate()`, `clean()`

### 동작 변경사항

#### `save()` 내부 순서 변경

```
Before: pre_save → validate → to_mongo() → init_collection → DB write
After:  pre_save → validate → SequenceField generate → FileField flush → to_mongo() → init_collection → DB write
```

- **SequenceField**: `save()` 시 None인 SequenceField 값을 `await field.generate()`로 자동 생성
- **FileField**: `__set__`에서 deferred된 `_pending_value`를 `to_mongo()` 전에 flush

#### `_save_create()` / `_save_update()` 컬렉션 해석

```python
# Before
collection = self._get_collection()  # 항상 클래스 기준

# After
collection = self._collection or await self.__class__._get_collection()
# 인스턴스에 _collection이 설정되어 있으면 (switch_db 등) 그것을 우선 사용
```

#### `_qs` property

```python
# Before
self.__objects = queryset_class(self.__class__, self._get_collection())

# After
self.__objects = queryset_class(self.__class__, self.__class__._collection)
# _collection이 None일 수 있음. QuerySet 내부에서 _ensure_collection()으로 lazy resolve
```

#### `reload()` 변경

```python
# Before
obj = self._qs.filter(...).limit(1).select_related(max_depth=max_depth)
if obj:
    obj = obj[0]

# After
obj = await self._qs.filter(...).limit(1).first()
# select_related() 체이닝 제거 (async에서 체이닝 불가)
# if obj: 대신 if obj is None: (QuerySet.__bool__ 사용 불가)
```

#### `switch_db()` / `switch_collection()` 변경

```python
# Before (sync context manager)
with switch_db(cls, alias) as cls: ...
self._get_collection = lambda: collection  # 인스턴스에 sync override

# After (async context manager)
async with switch_db(cls, alias) as cls: ...
self._collection = collection  # lambda override 제거
```

#### `MapReduceDocument.object` property 제거

```python
# Before
doc.object  # sync property, 내부에서 with_id() 호출

# After - 제거됨
await doc.get_object()  # async 메서드만 존재
```

### `delete()` 내 FileField 처리

```python
# Before
getattr(self, name).delete()  # sync

# After
await getattr(self, name).delete()  # async
```

---

## 4. QuerySet (`mongoengine/queryset/base.py`, `queryset.py`)

### `def` → `async def` 변환된 메서드

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
await qs.exec_js(code, *fields, **options)
await qs.sum(field)
await qs.average(field)
await qs.item_frequencies(field, normalize=False, map_reduce=True)
await qs.is_empty()          # 신규
await qs.get_item(index)     # 신규
```

### 변경 없는 메서드 (sync 유지, QuerySet 반환하는 체이닝 메서드)

`filter()`, `__call__()`, `all()`, `order_by()`, `limit()`, `skip()`, `hint()`, `collation()`, `batch_size()`, `only()`, `exclude()`, `fields()`, `all_fields()`, `search_text()`, `scalar()`, `values_list()`, `as_pymongo()`, `max_time_ms()`, `comment()`, `no_dereference()`, `no_sub_classes()`, `clear_cls_query()`, `none()`, `timeout()`, `allow_disk_use()`, `read_preference()`, `read_concern()`, `where()`, `clone()`, `no_cache()`, `cache()`

### 매직 메서드 변경

| Before | After | 비고 |
|---|---|---|
| `__iter__()` | `__aiter__()` | `for doc in qs` → `async for doc in qs` |
| `__next__()` | `__anext__()` | `next(qs)` → `await qs.__anext__()` |
| `__len__()` | **제거** | `len(qs)` → `await qs.count()` |
| `__bool__()` | **TypeError** 발생 | `if qs:` → `if not await qs.is_empty():` |
| `__getitem__(int)` | **TypeError** 발생 | `qs[0]` → `await qs.get_item(0)` |
| `__getitem__(slice)` | 동일 (sync) | `qs[1:3]` 그대로 사용 가능 |
| `rewind()` | sync 유지 | cursor_obj가 None이면 no-op |

### 신규 메서드

| 메서드 | 설명 |
|---|---|
| `async _ensure_collection()` | `_collection_obj`가 None이면 `await _get_collection()` 호출 |
| `async is_empty()` | `__bool__` 대체. 결과가 없으면 True |
| `async get_item(index)` | `__getitem__(int)` 대체. 인덱스로 문서 조회 |

### QuerySet 클래스별 변경

#### `QuerySet` (캐싱)

```python
# Before
def __iter__(self): ...
def __len__(self): ...
def _iter_results(self): ...     # sync generator
def _populate_cache(self): ...

# After
async def __aiter__(self): ...   # _ensure_collection() 호출 포함
async def _iter_results(self):   # async generator (yield 사용)
async def _populate_cache(self):
# __len__ 제거
```

#### `QuerySetNoCache` (비캐싱)

```python
# Before
def __iter__(self): ...

# After
async def __aiter__(self): ...
async def _get_async_cursor(self): ...  # 신규 helper
```

#### `__repr__()` 변경

```python
# Before: DB에 접근하여 데이터 fetch 후 표시
# After: 캐시된 데이터만 표시, 없으면 placeholder 문자열 반환
```

### `map_reduce()` 반환 타입 변경

```python
# Before: generator (yield MapReduceDocument)
# After: list (return [MapReduceDocument, ...])
```

### `aggregate()` 반환 타입

```python
# Before: pymongo CommandCursor (sync iterable)
# After: pymongo AsyncCommandCursor (async iterable)
# 사용: async for doc in await qs.aggregate(pipeline): ...
```

### QuerySetManager (`mongoengine/queryset/manager.py`)

```python
# Before
queryset = queryset_class(owner, owner._get_collection())

# After
queryset = queryset_class(owner, owner._collection)
# _collection이 None일 수 있음 → QuerySet._ensure_collection()에서 lazy resolve
```

---

## 5. Fields (`mongoengine/fields.py`)

### 5.1 ReferenceField — auto-dereference 제거

```python
# Before: __get__에서 DBRef를 자동으로 dereference하여 Document 반환
ref_value = instance._data.get(self.name)
if auto_dereference and isinstance(ref_value, DBRef):
    instance._data[self.name] = self._lazy_load_ref(cls, ref_value)

# After: raw value (DBRef 또는 ObjectId) 그대로 반환
return super().__get__(instance, owner)
```

**제거된 메서드**: `_lazy_load_ref()` (static method)

**영향**:
- `doc.author` 접근 시 `Document` 대신 `DBRef` 또는 `ObjectId` 반환
- 명시적 역참조 필요: `author = await Author.objects.get(pk=doc.author.id)`

### 5.2 GenericReferenceField — auto-dereference 제거

`ReferenceField`와 동일. `_lazy_load_ref()` 제거, `__get__`에서 dict 그대로 반환.

```python
# Before: doc.ref → Document 인스턴스
# After:  doc.ref → {"_cls": "ClassName", "_ref": DBRef(...)}
```

### 5.3 CachedReferenceField

| 항목 | Before | After |
|---|---|---|
| `__get__` | auto-dereference | raw value 반환 |
| `_lazy_load_ref()` | 존재 | **제거** |
| `to_python(value)` | dict → `db.dereference()` → Document | raw value 그대로 반환 |
| `start_listener()` | signal 등록 (auto_sync) | **no-op** (pass) |
| `sync_all()` | 전체 캐시 동기화 | **제거** |
| `on_document_pre_save()` | signal handler | **제거** |

### 5.4 LazyReferenceField / GenericLazyReferenceField

시그니처 변경 없음. 이들은 원래 `LazyReference` 객체를 반환하므로 async 전환에 적합.

### 5.5 LazyReference (`mongoengine/base/datastructures.py`)

| 항목 | Before | After |
|---|---|---|
| `fetch()` | `def fetch()` | `async def fetch()` |
| `__getattr__` (passthrough) | `self.fetch().attr` 자동 위임 | **AttributeError** 발생 (`await lazy_ref.fetch()` 사용 안내) |
| `__getitem__` (passthrough) | `self.fetch()[key]` 자동 위임 | **KeyError** 발생 (`await lazy_ref.fetch()` 사용 안내) |

```python
# Before
lazy_ref = doc.author  # LazyReference
lazy_ref.name           # passthrough=True면 자동 fetch + attribute 접근

# After
lazy_ref = doc.author   # LazyReference
doc = await lazy_ref.fetch()
doc.name                # 명시적 fetch 후 접근
```

### 5.6 SequenceField

| 항목 | Before | After |
|---|---|---|
| `generate()` | `def` (sync DB 호출) | `async def` |
| `set_next_value(value)` | `def` (sync DB 호출) | `async def` |
| `get_next_value()` | `def` (sync DB 호출) | `async def` |
| `__get__` | None이면 `self.generate()` 자동 호출 | **자동 생성 안 함**. `save()` 시 자동 생성 |
| `__set__` | None이면 `self.generate()` 자동 호출 | **자동 생성 안 함** |
| `to_python(value)` | None이면 `self.generate()` | value 그대로 반환 |

```python
# Before
doc = MyDoc()
print(doc.seq)  # 자동으로 시퀀스 생성, 예: 1

# After
doc = MyDoc()
print(doc.seq)  # None
await doc.save()  # save() 시점에 자동 생성
print(doc.seq)  # 1

# 명시적 생성도 가능
doc.seq = await MyDoc.seq.generate()
```

### 5.7 DynamicField

```python
# Before: to_python()에서 _ref가 있으면 db.dereference() 호출
# After: _ref가 있으면 raw dict 그대로 반환 (dereference 안 함)
```

### 5.8 FileField / GridFSProxy

#### GridFSProxy — `def` → `async def`

```python
await proxy.get()
await proxy.new_file(**kwargs)
await proxy.put(file_obj, **kwargs)
await proxy.write(string)
await proxy.writelines(lines)
await proxy.read(**kwargs)
await proxy.delete()
await proxy.replace(file_obj, **kwargs)
await proxy.close()
```

#### GridFSProxy — 기타 변경

| 항목 | Before | After |
|---|---|---|
| `fs` property | `gridfs.GridFS(...)` | `AsyncGridFS(...)` |
| `__getattr__` | `self.get()` 호출 → gridout 위임 | gridout이 이미 fetch된 경우만 위임, 아니면 AttributeError |
| `__str__` | `self.get()` 후 filename 접근 | `self.gridout`가 있으면 filename, 없으면 `"<no file>"` |

#### FileField.__set__ 변경

```python
# Before: __set__에서 바로 grid_file.delete() + grid_file.put(value) 호출
# After: _pending_value에 저장만 하고 save() 시점에 flush
```

#### ImageGridFsProxy — `@property` → `async def`

```python
# Before
proxy.size       # @property
proxy.format     # @property
proxy.thumbnail  # @property

# After
await proxy.get_size()
await proxy.get_format()
await proxy.get_thumbnail()
```

### 5.9 EmbeddedDocumentList (`mongoengine/base/datastructures.py`)

```python
# Before
embedded_list.save()   # def save(): self._instance.save(...)

# After
await embedded_list.save()  # async def save(): await self._instance.save(...)
```

> `EmbeddedDocumentList`의 `filter()`, `exclude()`, `count()`, `get()`, `first()`, `create()`, `delete()`, `update()`는 in-memory 연산이므로 sync 유지.

---

## 6. Context Managers (`mongoengine/context_managers.py`)

### `with` → `async with` 변환

| Context Manager | Before | After |
|---|---|---|
| `switch_db(cls, alias)` | `with switch_db(...) as cls:` | `async with switch_db(...) as cls:` |
| `switch_collection(cls, name)` | `with switch_collection(...) as cls:` | `async with switch_collection(...) as cls:` |
| `query_counter(alias)` | `with query_counter() as q:` | `async with query_counter() as q:` |
| `run_in_transaction(...)` | `with run_in_transaction():` | `async with run_in_transaction():` |

### sync 유지

`no_dereference(cls)`, `no_sub_classes(cls)`, `set_write_concern(...)`, `set_read_write_concern(...)`

### query_counter 비교 매직 메서드 제거

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

**제거된 메서드**: `__eq__`, `__ne__`, `__lt__`, `__le__`, `__gt__`, `__ge__`, `__int__`
**추가된 메서드**: `async def get_count()`

### run_in_transaction 내부 변경

```python
# Before
with conn.start_session(**kwargs) as session:
    with session.start_transaction(**kwargs):

# After
async with conn.start_session(**kwargs) as session:
    async with session.start_transaction(**kwargs):
```

### no_dereference 내부 구현

```python
# Before: threading.local 기반 dict
# After: contextvars.ContextVar 기반 frozenset
```

---

## 7. DeReference (`mongoengine/dereference.py`)

```python
# Before
DeReference()(items, max_depth=1, instance=None, name=None)  # sync

# After
await DeReference()(items, max_depth=1, instance=None, name=None)  # async
```

내부 변경:
- `__call__`: QuerySet → `[i async for i in items]`, `await self._fetch_objects()`
- `_fetch_objects`: `await collection.objects.in_bulk(refs)`, `async for ref in references`
- `_find_references`: sync 유지 (데이터 순회만)
- `_attach_objects`: sync 유지 (데이터 순회만)

---

## 8. PyMongo Support (`mongoengine/pymongo_support.py`)

| 함수 | Before | After |
|---|---|---|
| `count_documents(...)` | `def` | `async def` |
| `list_collection_names(...)` | `def` | `async def` |

- pymongo < 3.7 호환 코드 전부 제거
- `cursor.count()` fallback 제거

---

## 9. 미지원 기능 정리

### 의도적으로 제거/미지원

| 기능 | 이유 |
|---|---|
| ReferenceField auto-dereference | `__get__` descriptor에서 async DB 호출 불가 |
| GenericReferenceField auto-dereference | 동일 |
| CachedReferenceField auto-sync (signal) | signal handler에서 async 호출 불가 |
| CachedReferenceField.sync_all() | 제거됨 (async 버전 미구현) |
| SequenceField auto-generate in `__get__`/`__set__` | descriptor에서 async 호출 불가, save() 시 자동 생성 |
| LazyReference passthrough | `__getattr__`에서 async fetch 불가 |
| FileField `__set__` 즉시 업로드 | descriptor에서 async 호출 불가, save() 시 flush |
| `QuerySet.__bool__()` | async 불가, TypeError 발생으로 변경 |
| `QuerySet.__getitem__(int)` | async 불가, TypeError 발생으로 변경 |
| `QuerySet.__len__()` | async 불가, 제거 |
| `MapReduceDocument.object` property | async 불가, 제거 (`get_object()` 사용) |
| pymongo < 4.0 지원 | AsyncMongoClient가 pymongo 4+ 전용 |
| `get_connection(reconnect=True)` | deprecated (connection leak 방지) |

---

## 10. 테스트 마이그레이션 체크리스트

테스트코드 변환 시 확인해야 할 패턴:

### 필수 변환 패턴

```python
# 1. 모든 테스트 함수를 async로
def test_save(self):  →  async def test_save(self):

# 2. DB 조작 메서드에 await 추가
doc.save()            →  await doc.save()
doc.delete()          →  await doc.delete()
doc.update(...)       →  await doc.update(...)
doc.reload()          →  await doc.reload()
MyDoc.objects.get(...) →  await MyDoc.objects.get(...)
MyDoc.objects.first()  →  await MyDoc.objects.first()
MyDoc.objects.count()  →  await MyDoc.objects.count()
MyDoc.drop_collection() → await MyDoc.drop_collection()

# 3. 이터레이션
for doc in MyDoc.objects:     →  async for doc in MyDoc.objects:
list(MyDoc.objects)           →  [doc async for doc in MyDoc.objects]

# 4. 인덱싱
qs[0]                         →  await qs.get_item(0)
qs[1:3]                       →  qs[1:3]  (변경 없음)

# 5. truthiness 검사
if qs:                        →  if not await qs.is_empty():
assert qs                     →  assert not await qs.is_empty()

# 6. 길이
len(qs)                       →  await qs.count()

# 7. context manager
with switch_db(Cls, alias):   →  async with switch_db(Cls, alias):
with query_counter() as q:    →  async with query_counter() as q:
    assert q == 1             →      assert await q.get_count() == 1
with run_in_transaction():    →  async with run_in_transaction():

# 8. disconnect
disconnect()                  →  await disconnect()
disconnect_all()              →  await disconnect_all()

# 9. ReferenceField 접근
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

# 12. ImageGridFsProxy properties
proxy.size                    →  await proxy.get_size()
proxy.format                  →  await proxy.get_format()
proxy.thumbnail               →  await proxy.get_thumbnail()

# 13. MapReduceDocument
doc.object                    →  await doc.get_object()

# 14. map_reduce (반환 타입)
for doc in qs.map_reduce(...):  →  for doc in await qs.map_reduce(...):
# generator → list
```

### pytest 설정

```python
import pytest

# pytest-asyncio 사용
@pytest.mark.asyncio
async def test_example():
    connect("testdb")
    try:
        doc = MyDoc(name="test")
        await doc.save()
        ...
    finally:
        await disconnect_all()
```
