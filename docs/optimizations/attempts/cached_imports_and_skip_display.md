# Cached Imports in Hot Paths + Skip __set_field_display

- **Date**: 2026-03-30
- **Status**: Accepted

## Hypothesis

Profiling revealed three sources of per-call overhead in document
construction and serialization hot paths:

1. `ComplexBaseField.__set__` calls `_import_class("EnumField")` on
   every invocation (12k+ calls per 2k complex doc inits). Even though
   `_import_class` has an internal cache, the function call + dict
   lookup overhead adds up.

2. `ComplexBaseField.__get__` calls
   `_import_class("EmbeddedDocumentListField")` on every access, even
   when the value is not a list/tuple.

3. `__set_field_display` iterates over ALL fields on every document
   construction (`__init__`, `_init_fast`, `_from_son`) to check for
   `choices`, but most document classes have zero fields with choices.

4. `BaseField._to_mongo_safe_call` inspects `co_varnames` on every
   call (34k calls in complex `to_mongo`) instead of caching the
   signature per field class.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| Cache `_import_class` results as class-level attrs on `ComplexBaseField` for `__set__` and `__get__` | **~6-8% faster on __init__** | Eliminates repeated `_import_class` function calls; also moves `_import_class("EmbeddedDocumentListField")` inside the `isinstance(value, (list, tuple))` branch in `__get__` |
| Pre-compute `_has_choices_fields` flag per class, skip `__set_field_display` when False | **~5% faster on _from_son** | Avoids iterating all fields on every document construction for the common case of no choices |
| Cache `_to_mongo_safe_call` signature per field class in `BaseField._safe_call_sig_cache` | **~10% faster on to_mongo** | Avoids `co_varnames` introspection on every call; uses direct dispatch instead of `**ex_vars` dict |

## Benchmark Results

**Simple Document (6 fields, 1 embedded doc):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `__init__` | 7.16ms | 6.74ms | **1.06x** |
| `to_mongo` | 4.53ms | 4.05ms | **1.12x** |
| `_from_son` | 4.89ms | 4.37ms | **1.12x** |
| `to_json` | 9.31ms | 8.76ms | **1.06x** |
| `from_json` | 8.34ms | 7.83ms | **1.07x** |
| `bulk_from_son` | 497ms | 440ms | **1.13x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `__init__` | 19.80ms | 18.24ms | **1.09x** |
| `to_mongo` | 34.31ms | 30.57ms | **1.12x** |
| `_from_son` | 31.58ms | 28.99ms | **1.09x** |
| `to_json` | 63.23ms | 58.63ms | **1.08x** |
| `from_json` | 49.44ms | 46.08ms | **1.07x** |
| `bulk_from_son` | 3398ms | 3033ms | **1.12x** |

No regressions on `validate` (unchanged code path).

## Decision

Accepted. Three complementary micro-optimizations that each contribute
to a combined 8-13% improvement across `to_mongo`, `_from_son`, and
`__init__` paths:

- `ComplexBaseField._set_enum_field_type` / `_get_emb_doc_list_field_type`:
  class-level cached imports resolved once on first use.
- `BaseDocument._has_choices_fields`: lazily computed bool per class,
  gates `__set_field_display` iteration.
- `BaseField._safe_call_sig_cache`: per-class signature cache for
  `_to_mongo_safe_call`, eliminates `co_varnames` introspection and
  `**kwargs` dict construction on every call.
