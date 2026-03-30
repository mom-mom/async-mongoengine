# Deserialization and Validation Optimizations

## Summary

Two optimizations were applied to the hot paths in MongoEngine's
document lifecycle:

1. **`_from_son` rewrite** — ~2x faster deserialization from MongoDB
2. **`validate()` inline iteration** — ~1.5x faster field validation

Both are now the default implementation with no feature flags required.

## 1. `_from_son` — Optimized Deserialization

### Background

Every document loaded from MongoDB goes through `_from_son`, which
converts a PyMongo dict into a Document instance. The original
implementation performed multiple passes over the data:

1. Full dict copy with `_db_field_map` key translation
2. Second pass over `_fields` to call `to_python()` and delete old keys
3. Optional third dict copy for STRICT documents
4. `**data` unpacking into `__init__`, which iterates fields twice more
   (defaults + value assignment) and fires `pre_init`/`post_init` signals

### What Changed

| Aspect | Before | After |
|--------|--------|-------|
| Instance creation | `cls(**data)` → full `__init__` | `cls.__new__(cls)` → direct slot init |
| Key translation | `_db_field_map.get(key)` then second pass with `del` | `_reverse_db_field_map.get(key)` in single pass |
| Dict copies | 2-3 intermediate dicts | 0 copies — writes directly to `_data` |
| Field iteration | 4-5 passes total | 2 passes (son items + missing defaults) |
| Signals | `pre_init` / `post_init` fired | Skipped (not needed for DB loads) |

### Benchmark Results

Python 3.13.5, Apple Silicon. Median of 5 runs, 1000 iterations each.

**Simple Document (6 fields, 1 embedded doc):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 21.26ms | 10.37ms | **2.04x** |
| `from_json` | 25.03ms | 14.16ms | **1.76x** |

**Complex Document (20 fields, 3-level nesting, lists of embedded docs):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 126.96ms | 59.37ms | **2.12x** |
| `from_json` | 145.64ms | 77.59ms | **1.87x** |

### Trade-offs

- **`pre_init` / `post_init` signals are not fired** for documents
  loaded from the database. These are rarely used for DB loads.
- **`get_FOO_display()` methods** are not set up (the
  `__set_field_display` call is skipped). If you use `choices` with
  display values, verify behavior.

### Approaches Evaluated and Rejected

| Approach | Result | Reason |
|----------|--------|--------|
| StrictDict for `_data` storage | 8-13% **slower** | `StrictDict`'s `getattr`/`setattr` wrapper overhead exceeds CPython's optimized `dict` |
| Cache `co_varnames` introspection | ~1% improvement | CPython already optimizes `__code__` access; per-field `to_mongo()` call dominates cost |

## 2. `validate()` — Inline Iteration and Cached Imports

### Background

The original `validate()` method had three inefficiencies:

1. Called `_import_class("EmbeddedDocumentField")` and
   `_import_class("GenericEmbeddedDocumentField")` on **every**
   `validate()` invocation
2. Built a temporary `list[tuple[field, value]]` via list comprehension
   before iterating — allocating and discarding a list every call
3. Performed `isinstance(field, ...)` per field against a freshly
   constructed tuple

### What Changed

- **Cached imports**: The `_import_class` results are resolved once and
  stored as a class attribute (`_validate_embedded_types`).
- **Inline iteration**: The intermediate list is eliminated; we iterate
  `_fields_ordered` directly with local variable lookups for `_fields`,
  `_dynamic_fields`, and `_data`.

### Benchmark Results

**Simple Document (6 fields):**

| config | median | speedup |
|--------|--------|---------|
| baseline | 5.95ms | 1.00x |
| **imports+inline** | **3.80ms** | **1.56x** |

**Complex Document (20 fields, 3-level nesting):**

| config | median | speedup |
|--------|--------|---------|
| baseline | 42.44ms | 1.00x |
| **imports+inline** | **27.80ms** | **1.49x** |

### Approaches Evaluated and Rejected

| Approach | Result | Reason |
|----------|--------|--------|
| Cache `isinstance` as `_is_embedded_field` bool | 3-6% **slower** | `getattr(field, "_is_embedded_field", False)` fallback costs more than CPython's native `isinstance` |

## Reproducing the Benchmarks

```bash
uv run python benchmarks/bench_perf_flags.py [n] [repeat]
# Default: n=1000, repeat=5
```

## Future Work

- See [issue #12](https://github.com/mom-mom/async-mongoengine/issues/12)
  for a proposal to integrate `msgspec` for further JSON
  serialization/validation speedups.
