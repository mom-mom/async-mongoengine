# Fast __init__ Path

**Status:** Accept

## Bottleneck

`Document.__init__` spends significant time in `__setattr__` and field
descriptor `__set__` overhead:

- `__setattr__` is called ~30k times per 2000 inits (14k for field values,
  16k for internal attributes like `_initialised`, `_created`, etc.)
- Each call checks dynamic field status, shard key immutability, and
  `_created`/`_initialised` state via try/except
- `BaseField.__set__` calls `_import_class("EmbeddedDocument")` on every
  single field assignment (14k calls)
- Change tracking runs even though `_initialised=False` guards it

## Approach

Add a fast `__init__` path (`_fast_init`) that bypasses `__setattr__`
entirely for the common case: non-dynamic, non-STRICT documents without
signal receivers.

The fast path:
1. Writes defaults directly to `_data` dict
2. Calls `field.to_python()` for type conversion
3. For fields with custom `__set__` (e.g. `BinaryField`, `ComplexBaseField`),
   delegates to the field descriptor to preserve conversion logic
4. For standard `BaseField` fields, inlines the null-handling logic and
   writes directly to `_data`
5. Batches `_instance` wiring for embedded documents at the end
6. Caches `_import_class("EmbeddedDocument")` result

Gated behind `FAST_INIT = True` flag in `mongoengine.base.document`.

## Benchmark Results

Python 3.13.5, Apple Silicon. Median of 5 runs, 1000 iterations each.

**Simple Document (6 fields, 1 embedded doc):**

| Config | median | best | speedup |
|--------|--------|------|---------|
| FAST_INIT=False | 10.82ms | 9.59ms | 1.00x |
| **FAST_INIT=True** | **7.55ms** | **7.40ms** | **1.43x** |

**Complex Document (20 fields, 3-level nesting):**

| Config | median | best | speedup |
|--------|--------|------|---------|
| FAST_INIT=False | 26.41ms | 26.03ms | 1.00x |
| **FAST_INIT=True** | **22.56ms** | **22.28ms** | **1.17x** |

No regressions on existing benchmarks (validate, _from_son, from_json).

## Test Results

All 1108 tests pass with FAST_INIT=True (the optimized default).
The flag can be set to False to revert to the original behavior.

## Cleanup Needed

- Remove `FAST_INIT` flag and the conditional in `__init__` once stable
- The `_init_embedded_doc_type` class attribute can remain for caching
- Consider further optimization of `ComplexBaseField.__set__` which
  calls `_import_class("EnumField")` on every invocation

## Files Changed

- `mongoengine/base/document.py` — added `FAST_INIT` flag, `_fast_init`
  method, `_init_embedded_doc_type` cache, imported `BaseField`
- `benchmarks/bench_hypothesis_fast_init.py` — hypothesis benchmark
