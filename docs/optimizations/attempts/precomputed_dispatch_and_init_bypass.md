# Precomputed Dispatch Tables & Init Bypass

- **Date**: 2026-03-30
- **Status**: Accepted

## Hypothesis

After previous optimizations, remaining hotspots were:
1. `to_mongo()`: per-field `_get_to_mongo_sig()` called on every iteration (8ms for 56K calls)
2. `validate()`: per-field `isinstance(field, embedded_types)` + `_fields.get()` on every call
3. `__init__` (`_init_fast`): `ComplexBaseField.__set__` called unnecessarily for non-EnumField cases, plus `SON()` for `_dynamic_fields` on non-dynamic docs
4. `ComplexBaseField.to_mongo()`: list-to-dict-to-list round-trip for list values
5. `_from_son()`: unnecessary `str(db_key)` on every key (PyMongo always returns strings)

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| Precomputed to_mongo dispatch table per class | **Accepted** (27-40% on to_mongo) | Eliminates per-field `_get_to_mongo_sig()`, `_fields.get()`, and attribute lookups in hot loop |
| Precomputed validate dispatch table per class | **Accepted** (10-13% on validate) | Eliminates per-field `isinstance(field, embedded_types)` and `_fields.get()` lookups |
| `_init_fast` bypass `ComplexBaseField.__set__` | **Accepted** (18-32% on __init__) | Skip `__set__` for ComplexBaseField when inner field is not EnumField; write directly to _data |
| `_dynamic_fields = {}` in `_init_fast` for non-dynamic | **Accepted** (~5% on __init__) | SON() is 20x slower than dict construction |
| Direct list iteration in `ComplexBaseField.to_mongo` | **Accepted** (contributes to to_mongo improvement) | Avoids list→dict(enumerate)→sort→list round-trip |
| Remove `str(db_key)` in `_from_son` | **Accepted** (small, ~2% on _from_son) | PyMongo always returns string keys |
| Skip `weakref.proxy()` when no embedded fields | **Accepted** (small improvement) | Avoids creating proxy when it won't be used |
| `EmbeddedDocumentField.to_python` direct `document_type_obj` access | **Accepted** (avoids property overhead) | Skip `@property` after first resolution |
| Precomputed _from_son defaults | **Rejected** (slight regression) | Extra dict lookups + split iteration overhead worse than simple loop |
| Cache `has_receivers_for` per class | **Not attempted** | Signal check is only ~50ns/call, not worth caching dynamic state |

## Benchmark Results

### Simple (6 fields, 1 embedded doc) — n=1000, repeat=5

| Operation | Before (best) | After (best) | Speedup |
|-----------|--------------|-------------|---------|
| __init__ | 6.82ms | 5.50ms | **1.24x** |
| validate | 3.53ms | 3.23ms | **1.09x** |
| to_mongo | 4.13ms | 2.81ms | **1.47x** |
| _from_son | 4.30ms | 4.33ms | ~same |
| to_json | 8.89ms | 7.45ms | **1.19x** |

### Complex (20 fields, 3-level nesting) — n=1000, repeat=5

| Operation | Before (best) | After (best) | Speedup |
|-----------|--------------|-------------|---------|
| __init__ | 18.68ms | 14.17ms | **1.32x** |
| validate | 27.29ms | 24.56ms | **1.11x** |
| to_mongo | 31.61ms | 22.72ms | **1.39x** |
| _from_son | 28.97ms | 27.46ms | 1.05x |
| to_json | 61.26ms | 51.49ms | **1.19x** |

## Decision

Accepted. Multiple approaches combined for significant gains:

- **to_mongo**: 39-47% faster via precomputed dispatch table + direct list iteration
- **__init__**: 24-32% faster via ComplexBaseField.__set__ bypass + SON→dict
- **validate**: 9-11% faster via precomputed dispatch table
- **to_json**: 19% faster (inherits to_mongo improvement)

Key insight: class-level dispatch tables must use `cls.__dict__.get()` instead of
`cls._attr` to avoid inheriting parent class's cached table in inheritance hierarchies.

The _from_son precomputed defaults approach was rejected because the overhead of
split iteration (static dict + callable list) exceeded the simple single-loop approach
that was already in place.
