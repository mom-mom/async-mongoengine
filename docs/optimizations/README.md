# Performance Optimization Workflow

This document defines the process for proposing, benchmarking, and
landing (or rejecting) performance optimizations in async-mongoengine.

## Branching Strategy

```
master
  └── perf/optimizations          ← main optimization branch (1 PR to master)
        ├── perf/<name>           ← sub-branch per optimization
        │     → PR to perf/optimizations → code review → squash merge
        ├── perf/<name>
        │     → PR to perf/optimizations → code review → squash merge
        └── ...
```

Each optimization is a single squash-merged commit on `perf/optimizations`.
When all optimizations are done, `perf/optimizations` is merged to master
as one PR (preserving individual optimization commits).

## Flow

```
1. Identify bottleneck
   Run benchmarks/run_all.py to profile current performance.
   Read source code for the slowest operations.

2. Hypothesis
   Write benchmarks/bench_hypothesis_<name>.py with flag-gated
   on/off comparison.

3. Implement (flag-gated, on sub-branch perf/<name>)
   All existing tests must pass with the flag both on and off.

4. Measure
   Run benchmarks/run_all.py:
     - bench_baseline.py must show no regression vs main
     - bench_hypothesis_<name>.py must show improvement

5. PR + Code Review
   Create PR from perf/<name> → perf/optimizations.
   Get code review. Fix issues.

6a. Accept (improvement confirmed)
     - Squash merge to perf/optimizations
     - Record in docs/optimizations/attempts/<name>.md

6b. Reject (no improvement or regression)
     - Close PR
     - Record in docs/optimizations/attempts/<name>.md
       (why it failed — prevents re-attempting the same idea)

7. Clean up: remove flag, merge hypothesis benchmark into baseline
   if accepted. Go to 1.
```

## Version Control

Git is the single source of truth for all benchmark history and
optimization attempts. `run_all.py` compares the current branch
against main by checking out main in a temporary git worktree —
no result files are committed or stored.

## Directory Structure

```
benchmarks/
  bench_baseline.py          # Comprehensive baseline (all core paths)
  bench_hypothesis_<name>.py # Temporary per-hypothesis benchmarks
  run_all.py                 # Runner: baseline + compare vs main via git

docs/optimizations/
  README.md                  # This file
  attempts/                  # One file per optimization attempt
    <name>.md                # What was tried, results, accepted/rejected
```

## Benchmark Conventions

- All benchmarks run **without a MongoDB instance** (pure Python paths).
- Default parameters: `n=1000, repeat=5`.
- Report **median** time and **best-of-N** speedup.
- `gc.disable()` during timed loops, `gc.enable()` after.
- Warm up before measuring (1 untimed call per operation).

## Attempt Documentation Template

Each file in `attempts/` should follow this structure:

```markdown
# <Title>

- **Date**: YYYY-MM-DD
- **Status**: Accepted / Rejected
- **PR**: #NNN (if applicable)

## Hypothesis

What bottleneck was identified and why the proposed change should help.

## Approaches Tried

| Approach | Result | Reason |
|----------|--------|--------|
| ...      | ...    | ...    |

## Benchmark Results

Tables comparing before/after for each approach.

## Decision

Why accepted or rejected. What was learned.
```

## Existing Attempts

- [deserialization_and_validation.md](attempts/deserialization_and_validation.md) —
  `_from_son` rewrite (2x) and `validate()` inline iteration (1.5x). Accepted.
- [to_mongo_dict_replacement.md](attempts/to_mongo_dict_replacement.md) —
  `to_mongo()` SON→dict + cached co_varnames (2x). Accepted.
- [fast_init.md](attempts/fast_init.md) —
  `__init__` fast path bypassing `__setattr__` (1.2-1.4x). Accepted.
- [from_son_setattr_bypass.md](attempts/from_son_setattr_bypass.md) —
  `_from_son` `object.__setattr__` + `to_python` direct list iteration (1.6-2x). Accepted.
