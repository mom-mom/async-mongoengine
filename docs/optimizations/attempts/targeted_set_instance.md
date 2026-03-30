# Targeted _from_son_set_instance

- **Date**: 2026-03-30
- **Status**: Accepted

## Hypothesis

After `_from_son` builds the `_data` dict, `_from_son_set_instance`
recursively walks **every value** in `_data` to find embedded documents
and set `_instance` on them for change tracking. Profiling showed this
accounted for ~25-30% of `_from_son` time on complex documents (85k
recursive `_recurse` calls for 1k iterations).

Most fields (strings, ints, floats, plain lists of scalars, plain dicts)
cannot contain embedded documents, so the recursive walk wastes time
on them. Since the document schema is known at class definition time,
we can pre-compute which fields may contain embedded documents and only
visit those fields.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| Pre-computed `_from_son_embedded_field_info` tuple per class | **1.44-1.50x faster on _from_son** | Only visits fields that can contain embedded docs (EmbeddedDocumentField, EmbeddedDocumentListField, GenericEmbeddedDocumentField, or ComplexBaseField wrapping any of these). Skips all scalar/plain-list/plain-dict fields entirely. |

## Benchmark Results

**Simple Document (6 fields, 1 embedded doc):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 6.89ms | 4.65ms | **1.48x** |
| `from_json` | 10.30ms | 8.11ms | **1.27x** |
| `bulk_from_son` (x1000) | 706.79ms | 472.15ms | **1.50x** |

**Complex Document (20 fields, 3-level nesting):**

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `_from_son` | 44.45ms | 30.88ms | **1.44x** |
| `from_json` | 62.86ms | 48.36ms | **1.30x** |
| `bulk_from_son` (x1000) | 4682.36ms | 3275.85ms | **1.43x** |

No regressions on other operations (`__init__`, `validate`, `to_mongo`, `to_json`).

## Decision

Accepted. At class creation time (lazily on first `_from_son` call), a
tuple of `(field_name, kind)` pairs is built and cached on the class as
`_from_son_embedded_field_info`. The new `_from_son_set_instance_targeted`
function dispatches directly based on field kind:

- `_EMB_DIRECT`: single embedded doc, just set `_instance`
- `_EMB_LIST`: list of embedded docs, iterate and set `_instance`
- `_EMB_GENERIC`: unknown nesting (e.g. MapField(MapField(EmbeddedDocumentField))),
  falls back to recursive walk on that field only

If the class has no embedded fields at all, the `_instance` wiring step
is skipped entirely.
