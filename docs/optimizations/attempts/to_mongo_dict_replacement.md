# to_mongo: SON-to-dict replacement + cached co_varnames

**Status**: Recommend Accept
**Flag**: `BaseDocument._fast_to_mongo` (default `True`)

## Bottleneck

Profiling `to_mongo()` on a complex document (20 fields, 3-level nesting)
showed that **SON operations consumed ~80% of the total time**:

| SON operation | calls/1000 docs | cumulative |
|---------------|-----------------|------------|
| `__setitem__` | 76,000 | 24ms |
| `__init__` | 10,000 | 9ms |
| `pop` | 10,000 | 6ms |
| `__delitem__` | 20,000 | 6ms |
| `update` | 20,000 | 5ms |
| **Total SON** | | **~50ms** |

`bson.son.SON` maintains insertion order via an internal `_SON__keys`
list. Every `__setitem__` appends to the list, every `__delitem__`
removes from it (linear scan). Python 3.7+ `dict` preserves insertion
order natively with O(1) amortized operations.

Secondary bottleneck: `field.to_mongo.__code__.co_varnames` was
inspected on every field on every call to determine whether the
field's `to_mongo` accepts `use_db_field` and `fields` keyword args.

## Approach

1. **Replace `SON` with `_MongoDict`** -- a trivial `dict` subclass
   that adds only a `to_dict()` method for backward compatibility
   (existing code calls `.to_dict()` on `to_mongo()` results).

2. **Cache `co_varnames` per field class** -- a class-level dict
   `_to_mongo_sig_cache` maps `type(field)` to
   `(accepts_use_db_field, accepts_fields)`. Resolved once per field
   type, reused across all calls.

3. **Avoid set-then-delete for `_cls`** -- only insert `_cls` when
   `allow_inheritance` is true, instead of always inserting then
   popping.

4. **Cache `_import_class` in `ComplexBaseField.to_mongo`** -- the
   three `_import_class` calls (Document, EmbeddedDocument,
   GenericReferenceField) are resolved once and stored as class
   attributes.

5. **Direct `_fields_ordered` iteration** -- iterate the tuple
   directly instead of going through `__iter__` (saves one function
   call per iteration).

## Benchmark Results

Python 3.13.5, Apple Silicon. Median of 5 runs, 1000 iterations each.

**Simple Document (6 fields, 1 embedded doc):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `to_mongo` | 10.03ms | 4.83ms | **2.08x** |
| `to_json` | 15.14ms | 9.51ms | **1.59x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `to_mongo` | 60.97ms | 33.95ms | **1.80x** |
| `to_json` | 89.77ms | 61.73ms | **1.45x** |

**Baseline regression check** (`bench_perf_flags.py`): no regressions
on `validate`, `_from_son`, or `from_json`.

## Compatibility

- All 1108 existing tests pass with the flag ON (default).
- All 1108 existing tests pass with the flag OFF (legacy SON path).
- `_MongoDict` is a `dict` subclass, so all `isinstance(x, dict)`
  checks, PyMongo insert/update operations, and `json_util.dumps`
  work unchanged.
- Fixed `GenericEmbeddedDocumentField.validate` to accept `dict` in
  addition to `SON` for the `choices` validation path.
- Fixed `__setstate__` to recognize `_MongoDict` for pickle
  round-tripping.

## Trade-offs

- Code that explicitly checks `isinstance(result, SON)` on
  `to_mongo()` return values will fail. No such checks exist in the
  codebase or tests; external code should use `isinstance(x, dict)`.
- `_MongoDict.to_dict()` returns a plain `dict` (same behavior as
  `SON.to_dict()`).

## Recommendation

**Accept.** The optimization delivers a consistent ~2x speedup on
`to_mongo` and ~1.5x on `to_json` with no behavioral changes. The
`SON` type is not part of MongoEngine's public API contract for
`to_mongo()` return values.

## Cleanup if accepted

1. Remove `_fast_to_mongo` flag and `_to_mongo_legacy` method
2. Remove `SON` import from `base/document.py` if no longer needed
3. Rename `_to_mongo_fast` to `to_mongo`
4. Consider adding `to_mongo` to the baseline benchmark
