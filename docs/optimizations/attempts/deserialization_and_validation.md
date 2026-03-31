# Deserialization and Validation Optimizations

- **Date**: 2026-03-30
- **Status**: Accepted
- **PR**: #13

## Hypothesis

`_from_son` (the main deserialization path) performed 4-5 passes over
the data and went through full `__init__`. `validate()` rebuilt a
temporary list and called `_import_class` on every invocation.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| `_from_son` via `__new__` + single-pass key translation | **2x faster** | Eliminates 3 dict copies + `__init__` overhead |
| `validate()` cached imports + inline iteration | **1.5x faster** | Removes temp list allocation + repeated `_import_class` |
| StrictDict for `_data` storage | 8-13% **slower** | Wrapper overhead > dict |
| Cache `co_varnames` in metaclass | ~1% improvement | Noise level |
| Cache `isinstance` as bool | 3-6% **slower** | `getattr` fallback > `isinstance` |

## Benchmark Results

**Simple Document (6 fields):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 21.26ms | 10.37ms | **2.04x** |
| `from_json` | 25.03ms | 14.16ms | **1.76x** |
| `validate` | 5.95ms | 3.80ms | **1.56x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 126.96ms | 59.37ms | **2.12x** |
| `from_json` | 145.64ms | 77.59ms | **1.87x** |
| `validate` | 42.44ms | 27.80ms | **1.49x** |

## Decision

Accepted. `_from_son` uses `__new__` with `_reverse_db_field_map` for
O(1) key translation and direct `_data` writes. Falls back to
`__init__` path when `pre_init`/`post_init` signal receivers are
registered. `validate()` caches `_import_class` results and iterates
`_fields_ordered` directly.
