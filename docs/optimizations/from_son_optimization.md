# `_from_son` Deserialization Optimization

## Summary

We evaluated three approaches to speed up MongoEngine's
validate/serialize/deserialize pipeline. Only one — **`fast_from_son`** —
produced a meaningful improvement: **~2x faster `_from_son`** and
**~1.8x faster `from_json`**.

The optimization bypasses `BaseDocument.__init__` when constructing
Document instances from raw MongoDB dicts, using `__new__` with direct
`_data` writes and a single-pass key translation.

## Background

Every document loaded from MongoDB goes through `_from_son`, which
converts a PyMongo dict into a Document instance. The original
implementation performs multiple passes over the data:

1. Full dict copy with `_db_field_map` key translation
2. Second pass over `_fields` to call `to_python()` and delete old keys
3. Optional third dict copy for STRICT documents
4. `**data` unpacking into `__init__`, which iterates fields twice more
   (defaults + value assignment) and fires `pre_init`/`post_init` signals

For bulk query results, this overhead is multiplied by every document
returned.

## Approaches Evaluated

Three independent optimizations were implemented behind feature flags
in `PERF_FLAGS` so that each could be benchmarked in isolation.

### 1. StrictDict for `_data` storage (`use_strict_dict`)

**Hypothesis**: Replace the plain `dict` used for `_data` with
`StrictDict` (a `__slots__`-based dict wrapper) to reduce memory and
speed up attribute access.

**Result**: **8-13% slower** across all operations. `StrictDict`'s
`__getitem__`/`__setitem__` methods check `_special_fields` on every
access and route through `getattr`/`setattr`, adding overhead that
CPython's highly-optimized `dict` implementation doesn't have.

**Verdict**: Rejected.

### 2. Cache `co_varnames` introspection (`cache_co_varnames`)

**Hypothesis**: `to_mongo()` inspects `field.to_mongo.__code__.co_varnames`
on every call to determine which keyword arguments to pass. Caching this
at class-creation time should eliminate the overhead.

**Result**: **~1% improvement** on `to_mongo` — within noise. CPython
already optimizes `__code__` attribute access efficiently, and the
per-field `to_mongo()` call itself dominates the cost.

**Verdict**: Rejected (negligible benefit, adds complexity).

### 3. Optimized `_from_son` with `__new__` (`fast_from_son`)

**Hypothesis**: Bypass `__init__` entirely, use `_reverse_db_field_map`
for O(1) key translation, and write directly to `_data` in a single pass.

**Result**: **2x faster** for `_from_son`, **1.8x faster** for `from_json`.

**Verdict**: Accepted.

## Benchmark Results

Python 3.13.5, Apple Silicon. Median of 5 runs, 1000 iterations each.

### Simple Document (6 fields, 1 embedded doc)

| Operation | baseline | fast_from_son | speedup |
|-----------|----------|---------------|---------|
| `_from_son` | 21.26ms | 10.37ms | **2.04x** |
| `from_json` | 25.03ms | 14.16ms | **1.76x** |

### Complex Document (20 fields, 3-level nesting, lists of embedded docs)

| Operation | baseline | fast_from_son | speedup |
|-----------|----------|---------------|---------|
| `_from_son` | 126.96ms | 59.37ms | **2.12x** |
| `from_json` | 145.64ms | 77.59ms | **1.87x** |

Speedup increases slightly with document complexity because the
`__init__` bypass saves more work when there are more fields and deeper
nesting.

### Full comparison table (all three approaches)

Tested with n=1000, repeat=5 on Simple and Complex scenarios.

**Simple Document:**

| Operation | baseline | strict_dict | cache_covars | fast_from_son | all_on |
|-----------|----------|-------------|-------------|---------------|--------|
| `__init__` | 10.23ms (1.00x) | 12.11ms (0.84x) | 10.37ms (0.99x) | 10.32ms (1.00x) | 12.50ms (0.82x) |
| `validate` | 5.87ms (1.00x) | 6.57ms (0.90x) | 6.27ms (0.94x) | 5.85ms (1.00x) | 7.04ms (0.84x) |
| `to_mongo` | 10.06ms (1.00x) | 10.78ms (0.95x) | 10.00ms (1.03x) | 10.11ms (1.00x) | 10.77ms (0.95x) |
| `_from_son` | 21.26ms (1.00x) | 23.77ms (0.89x) | 21.38ms (0.99x) | 10.37ms (2.04x) | 12.95ms (1.63x) |
| `to_json` | 15.26ms (1.00x) | 16.05ms (0.96x) | 15.05ms (1.02x) | 15.31ms (0.99x) | 15.91ms (0.95x) |
| `from_json` | 25.03ms (1.00x) | 28.33ms (0.89x) | 25.04ms (1.00x) | 14.16ms (1.76x) | 17.39ms (1.44x) |

**Complex Document:**

| Operation | baseline | strict_dict | cache_covars | fast_from_son | all_on |
|-----------|----------|-------------|-------------|---------------|--------|
| `__init__` | 27.26ms (1.00x) | 31.29ms (0.87x) | 27.16ms (1.00x) | 27.22ms (1.00x) | 31.04ms (0.88x) |
| `validate` | 40.04ms (1.00x) | 44.95ms (0.90x) | 42.11ms (0.95x) | 40.59ms (0.98x) | 46.80ms (0.86x) |
| `to_mongo` | 68.09ms (1.00x) | 73.39ms (0.91x) | 68.47ms (0.98x) | 70.02ms (0.96x) | 73.02ms (0.92x) |
| `_from_son` | 126.96ms (1.00x) | 141.67ms (0.89x) | 128.62ms (0.98x) | 59.37ms (2.12x) | 75.72ms (1.68x) |
| `to_json` | 97.47ms (1.00x) | 104.18ms (0.94x) | 98.85ms (0.98x) | 99.30ms (0.97x) | 103.33ms (0.93x) |
| `from_json` | 145.64ms (1.00x) | 165.06ms (0.88x) | 148.68ms (0.98x) | 77.59ms (1.87x) | 95.15ms (1.53x) |

## How `fast_from_son` Works

The optimized path (`BaseDocument._from_son_fast`) differs from the
original in these ways:

| Aspect | Original `_from_son` | `_from_son_fast` |
|--------|---------------------|------------------|
| Instance creation | `cls(**data)` → full `__init__` | `cls.__new__(cls)` → direct slot init |
| Key translation | `_db_field_map.get(key)` then second pass with `del` | `_reverse_db_field_map.get(key)` in single pass |
| Dict copies | 2-3 intermediate dicts | 0 copies — writes directly to `_data` |
| Field iteration | 4-5 passes total | 2 passes (son items + missing defaults) |
| Signals | `pre_init` / `post_init` fired | Skipped (not needed for DB loads) |

## Usage

```python
import mongoengine

# Enable globally
mongoengine.PERF_FLAGS["fast_from_son"] = True

# All subsequent queries use the fast path
users = await User.objects.all().to_list()
```

## Trade-offs

- **`pre_init` / `post_init` signals are not fired** in the fast path.
  These are rarely used for documents loaded from the database.
- **`get_FOO_display()` methods** are not set up (the `__set_field_display`
  call is skipped). If you use `choices` with display values, verify
  behavior before enabling.
- The flag is **opt-in** and defaults to `False` to maintain backward
  compatibility.

## Reproducing the Benchmarks

```bash
uv run python benchmarks/bench_perf_flags.py [n] [repeat]
# Default: n=1000, repeat=5
```

## Future Work

- See [issue #12](https://github.com/mom-mom/async-mongoengine/issues/12)
  for a proposal to integrate `msgspec` for further JSON
  serialization/validation speedups.
