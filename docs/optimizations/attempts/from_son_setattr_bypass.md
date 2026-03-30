# _from_son: __setattr__ Bypass + to_python Direct List Iteration

- **Date**: 2026-03-30
- **Status**: Accepted
- **PR**: #16

## Hypothesis

Profiling `_from_son` on a complex document revealed three bottlenecks
(~40% of total time):

1. Custom `__setattr__` overhead (89k calls) — unnecessary during
   `_from_son` initialization
2. `_set_instance` closure recreated on every call + used custom
   `__setattr__` on embedded docs
3. `ComplexBaseField.to_python` converted lists to dicts and back
   (O(n) dict + O(n log n) sort) when direct iteration suffices

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| `object.__setattr__` for all internal attrs | **1.5-1.7x on _from_son** | Eliminates 89k custom `__setattr__` calls |
| Module-level `_from_son_set_instance` | **included** | Avoids closure recreation |
| `{}` instead of `SON()` for non-dynamic `_dynamic_fields` | **included** | SON creation is 10x slower |
| Direct list iteration in `to_python` | **1.1x additional** | Avoids list→dict→sort roundtrip |

## Benchmark Results

**Simple Document (6 fields):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 13.74ms | 6.95ms | **1.98x** |
| `from_json` | 17.17ms | 10.56ms | **1.63x** |
| `bulk_from_son` (x1000) | 1379ms | 704ms | **1.96x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 79.39ms | 45.19ms | **1.76x** |
| `from_json` | 97.56ms | 62.90ms | **1.55x** |
| `bulk_from_son` (x1000) | 8252ms | 4617ms | **1.79x** |

## Decision

Accepted. `_from_son` uses `object.__setattr__` for all internal slot
assignments. `_from_son_set_instance` is a module-level function.
`ComplexBaseField.to_python` processes lists directly with cached
`_import_class` results.
