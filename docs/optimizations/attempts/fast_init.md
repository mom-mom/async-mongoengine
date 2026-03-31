# Fast __init__ Path

- **Date**: 2026-03-30
- **Status**: Accepted
- **PR**: #15

## Hypothesis

`Document.__init__` spent ~50% of its time in `__setattr__` and field
descriptor overhead. Each field assignment triggered dynamic field
checks, shard key validation, try/except for state flags, and
`_import_class("EmbeddedDocument")` calls in `BaseField.__set__`.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| `_init_fast` bypassing `__setattr__` | **1.2-1.4x faster** | Writes directly to `_data`, delegates to `field.__set__` only for fields with custom descriptors |

## Benchmark Results

**Simple Document (6 fields):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `__init__` | 10.82ms | 7.55ms | **1.43x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `__init__` | 26.41ms | 22.56ms | **1.17x** |

## Decision

Accepted. Fast path is default for non-dynamic, non-STRICT documents
without signal receivers. Falls back to legacy `__init__` path when
any of these conditions apply (dynamic, STRICT, signals, or
`__auto_convert=False`).
