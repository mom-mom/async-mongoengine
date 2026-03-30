# _from_son: __setattr__ bypass + to_python direct list iteration

- **Date**: 2026-03-30
- **Status**: Accepted

## Hypothesis

Profiling `_from_son` on a complex document (20 fields, 3-level nesting)
revealed three bottlenecks accounting for ~40% of total time:

1. **Custom `__setattr__` overhead** (89k calls, 0.039s / 18%):
   Every internal attribute assignment (`_initialised`, `_created`,
   `_dynamic_fields`, `_data`, etc.) on freshly constructed objects
   goes through the custom `__setattr__`, which checks dynamic field
   status, shard key immutability, and `_created`/`_initialised` state
   via try/except — all unnecessary during `_from_son` initialization.

2. **`_set_instance` closure recreation** (0.040s cumulative / 19%):
   A nested `_set_instance` function and its closure were created on
   every `_from_son` call. Setting `_instance` on embedded documents
   also went through the custom `__setattr__`.

3. **`ComplexBaseField.to_python` list-to-dict-and-back** (0.011s self):
   Every list value (ListField, EmbeddedDocumentListField) was converted
   to a dict via `enumerate()`, processed, then sorted back to a list —
   O(n) dict creation + O(n log n) sort, when a direct list
   comprehension suffices. Additionally, `_import_class("BaseDocument")`
   and `_import_class("Document")` were called on every invocation.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| `object.__setattr__` for all internal attrs in `_from_son` | **1.5-1.7x on _from_son** | Eliminates 89k custom `__setattr__` calls per 1000 docs |
| Module-level `_from_son_set_instance` with `object.__setattr__` | **included above** | Avoids closure recreation + bypasses custom `__setattr__` on embedded docs |
| Replace `SON()` with `{}` for `_dynamic_fields` (non-dynamic docs) | **included above** | SON creation is 10x slower than dict; unused for non-dynamic docs |
| Direct list iteration in `ComplexBaseField.to_python` (flag-gated) | **1.10-1.13x on _from_son** | Avoids list->dict->sort roundtrip; caches `_import_class` results |

## Benchmark Results

Python 3.13.5, Apple Silicon. Median of 5 runs, 1000 iterations each.

**Simple Document (6 fields, 1 embedded doc):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 13.74ms | 6.95ms | **1.98x** |
| `from_json` | 17.17ms | 10.56ms | **1.63x** |
| `bulk_from_son` (x1000) | 1379ms | 704ms | **1.96x** |
| `__init__` | 7.77ms | 7.11ms | 1.09x |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 79.39ms | 45.19ms | **1.76x** |
| `from_json` | 97.56ms | 62.90ms | **1.55x** |
| `bulk_from_son` (x1000) | 8252ms | 4617ms | **1.79x** |
| `__init__` | 22.98ms | 19.78ms | 1.14x |

No regressions on `validate` or `to_mongo`.

## Flags

- `FAST_TO_PYTHON` in `mongoengine/base/fields.py` (default `True`) --
  controls `ComplexBaseField.to_python` fast path.
- The `_from_son` `object.__setattr__` changes have no flag (always active).

## Test Results

All 1108 tests pass with both `FAST_TO_PYTHON=True` (default) and
`FAST_TO_PYTHON=False`.

## Decision

**Accept.** The combined optimization delivers ~1.8-2x speedup on
`_from_son` and ~1.5-1.6x on `from_json` with zero behavioral changes.
Total function calls reduced from ~988k to ~803k per 1000 complex
document deserializations.

## Files Changed

- `mongoengine/base/document.py` -- `_from_son` uses `object.__setattr__`,
  module-level `_from_son_set_instance`, `{}` for non-dynamic `_dynamic_fields`
- `mongoengine/base/fields.py` -- `FAST_TO_PYTHON` flag, `_to_python_fast`
  method with direct list iteration and cached imports
- `benchmarks/bench_hypothesis_fast_to_python.py` -- hypothesis benchmark
