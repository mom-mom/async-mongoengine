# Extreme Optimization Approaches

- **Date**: 2026-03-30
- **Status**: All Rejected (for now)

## Context

After achieving 2-5x speedup on all core paths through pure Python
optimizations, five extreme approaches were explored in parallel
using independent agents.

## Approaches Tried

### 1. orjson JSON replacement

Replace `bson.json_util.dumps/loads` with orjson (Rust-based JSON encoder).

| Operation | before | after | speedup |
|-----------|--------|-------|---------|
| `to_json` (Simple) | 7.62ms | 4.45ms | **1.71x** |
| `from_json` (Simple) | 7.59ms | 6.39ms | 1.19x |

**Rejected.** The 1.71x on to_json is decent, but from_json shows
minimal improvement. The pre-convert step needed for Extended JSON
compatibility (`$oid`, `$date`, `$numberDecimal` formats) partially
negates the faster JSON encoding.

### 2. Rust/C extension for _from_son inner loop

Built a C extension with `fast_build_data()` for key translation +
simple type passthrough.

| Component | Python | C Extension | Speedup |
|-----------|--------|-------------|---------|
| Inner loop (simple SON) | 1.02 us | 0.57 us | 1.77x |
| Key translation only | 0.94 us | 0.19 us | 5.27x |

**Rejected.** The inner loop is only 42% of `_from_son` total time,
yielding ~18% end-to-end improvement. Maintenance burden (C/Rust
build, per-platform binaries, ABI compat) far exceeds the benefit.

### 3. Skip to_python for identity fields

Added `_identity_type` flag on fields where `to_python` is a no-op
for the common case (StringField → str, IntField → int, etc.).

| Operation | before | after | change |
|-----------|--------|-------|--------|
| Simple `_from_son` | 4.20ms | 4.62ms | **-10% (slower)** |

**Rejected.** The `type(value) is expected_type` check + dict lookup
costs more than just calling the simple `to_python` method. CPython's
function call overhead for trivial methods is already very low.

### 4. __slots__ Document prototype

Created a `SlottedDocument` base class with metaclass-generated
`__slots__` instead of `_data` dict.

| Operation | Regular | Slotted | Speedup |
|-----------|---------|---------|---------|
| `__init__` | 25.58ms | 10.62ms | **2.41x** |
| Memory per instance | 544 bytes | 203 bytes | **-62.7%** |
| `validate` | 10.52ms | 10.55ms | 1.00x |

**Rejected.** The `_data` dict is a fundamental API contract used by
`BaseField.__get__`/`__set__`, change tracking, serialization, dynamic
documents, and user-facing code. Replacing it would require changes
across the entire codebase.

**Idea worth exploring later:** A lightweight read-only "row" type for
cursor iteration that uses slots, leaving Document unchanged.

### 5. msgspec Struct integration

Generate `msgspec.Struct` per Document class for fast JSON + validation.

Agent prototype results (using msgspec's native format, NOT Extended JSON):

| Operation | Current | msgspec | Speedup |
|-----------|---------|---------|---------|
| `to_json` | 9.24 us | 2.31 us | **4.00x** |
| `from_json` | 9.99 us | 3.30 us | **3.03x** |
| `validate` | 3.94 us | 1.04 us | **3.79x** |

When Extended JSON compatibility is enforced (real integration):

| Operation | json_util | msgspec | Speedup |
|-----------|-----------|---------|---------|
| `to_json` | 36.7ms | 31.3ms | **1.17x** |
| `from_json` | 37.6ms | 40.0ms | **0.94x (slower)** |

**Rejected.** The impressive 3-4x speedup only applies when using
msgspec's native format. Maintaining Extended JSON compatibility
(`$oid`, `$date`, `$numberDecimal`) requires pre/post-conversion steps
that negate the performance advantage. A future API that offers a
non-Extended-JSON fast path could unlock this potential.

## Key Insight

The remaining JSON serialization bottleneck is fundamentally about
**Extended JSON format conversion**, not JSON encoding speed. Any
approach that maintains Extended JSON compatibility will be limited
by the O(n) type-checking and dict-wrapping overhead. Breaking through
this barrier requires either:

1. A new API that skips Extended JSON (e.g. `to_fast_json()`)
2. Moving the Extended JSON conversion into a compiled language
3. Avoiding JSON entirely (e.g. returning BSON bytes directly)
