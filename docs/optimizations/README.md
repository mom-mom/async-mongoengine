# Performance Optimization Workflow

This document defines the process for proposing, benchmarking, and
landing (or rejecting) performance optimizations in async-mongoengine.

## Flow

```
1. Baseline benchmark
   Run benchmarks/run_all.py on a feature branch.
   It automatically checks out main via git worktree and runs
   the same benchmark there for comparison.

2. Hypothesis
   Identify a bottleneck and form a hypothesis.
   Write benchmarks/bench_hypothesis_<name>.py with flag-gated
   on/off comparison.

3. Implement (flag-gated)
   All existing tests must pass with the flag off (default).

4. Measure
   Run benchmarks/run_all.py:
     - bench_baseline.py must show no regression vs main
     - bench_hypothesis_<name>.py must show improvement

5a. Accept (improvement confirmed)
     - Remove flag, make default
     - Add regression tests
     - Merge hypothesis benchmark into bench_baseline.py
     - Record in docs/optimizations/attempts/<name>.md

5b. Reject (no improvement or regression)
     - Revert code changes
     - Record in docs/optimizations/attempts/<name>.md
       (why it failed — prevents re-attempting the same idea)

6. Go to 2
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
