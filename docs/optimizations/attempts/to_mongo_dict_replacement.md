# to_mongo: SON-to-dict Replacement

- **Date**: 2026-03-30
- **Status**: Accepted
- **PR**: #14

## Hypothesis

`bson.SON` maintains insertion order via an internal `_SON__keys` list,
making `__setitem__` O(n) and `__delitem__` linear scan. For a complex
document with nested embedded docs, SON operations consumed ~80% of
`to_mongo()` time. Python 3.7+ `dict` preserves insertion order
natively with O(1) amortized operations.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| Replace SON with `_MongoDict` (dict subclass) | **2x faster** | O(1) vs O(n) per operation |
| Cache `co_varnames` per field class | **included** | Avoids repeated `__code__` introspection |
| Conditional `_cls` insertion | **included** | Skip set-then-pop pattern |
| Cache `_import_class` in `ComplexBaseField.to_mongo` | **included** | Eliminate per-call lookups |

## Benchmark Results

**Simple Document (6 fields):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `to_mongo` | 10.03ms | 4.83ms | **2.08x** |
| `to_json` | 15.14ms | 9.51ms | **1.59x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `to_mongo` | 60.97ms | 33.95ms | **1.80x** |
| `to_json` | 89.77ms | 61.73ms | **1.45x** |

## Decision

Accepted. `_MongoDict` is a trivial `dict` subclass with `to_dict()`
for backward compatibility. PyMongo accepts plain `dict` for all
insert/update operations.
