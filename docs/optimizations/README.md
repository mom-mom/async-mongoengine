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
        │     → ...
        └── perf/cleanup          ← flag removal + artifact cleanup
```

Each optimization is a single squash-merged commit on `perf/optimizations`.
When all optimizations are done, `perf/optimizations` is merged to master
as one PR (preserving individual optimization commits).

## Flow

```
1. Identify bottleneck
   Run `uv run python benchmarks/run_all.py` to profile current
   performance and compare against main.
   Read source code for the slowest operations.

2. Implement (flag-gated, on sub-branch perf/<name>)
   - Gate behind a simple flag (class attr, module-level bool, etc.)
   - All existing tests must pass with the flag both on and off
   - Write a hypothesis benchmark if the baseline doesn't cover
     the specific scenario well enough

3. Measure
   Run benchmarks/run_all.py to verify:
     - No regression on existing operations
     - >10% improvement on the target operation

4. PR + Code Review
   Create PR from perf/<name> → perf/optimizations.
   Get code review. Fix issues.

5a. Accept (improvement confirmed)
     - Squash merge to perf/optimizations
     - Record in docs/optimizations/attempts/<name>.md

5b. Reject (no improvement or regression)
     - Close PR
     - Record in docs/optimizations/attempts/<name>.md
       (why it failed — prevents re-attempting the same idea)

6. Repeat 1-5 until no more >10% improvements are found.

7. Cleanup
   On a separate perf/cleanup branch:
     - Remove all feature flags, delete legacy code paths
     - Remove hypothesis benchmarks (baseline covers them)
     - Update attempt list in this README
   Squash merge to perf/optimizations.
```

## Agent Usage

Optimization agents can be launched to autonomously identify
bottlenecks, implement, and benchmark. The orchestrator (human or
main Claude session) handles:

- Creating sub-branches from `perf/optimizations`
- Reviewing agent output and applying changes
- Running code review agents on PRs
- Deciding accept/reject

**Worktree constraint**: Agent worktrees created with
`isolation: "worktree"` default to master, not the current branch.
To ensure agents work from `perf/optimizations`, either:

- Include `git checkout perf/optimizations && git pull` in the agent
  prompt, or
- Manually apply agent results onto the correct base branch

## Version Control

Git is the single source of truth. `run_all.py` compares the current
branch against main by checking out main in a temporary git worktree —
no result files are committed or stored.

## Directory Structure

```
benchmarks/
  bench_baseline.py   # Pure-Python baseline (all core paths, no MongoDB)
  bench_mongodb.py    # End-to-end with real MongoDB (save, find, update, aggregate)
  run_all.py          # Runner: baseline + compare vs main via git
                      #   --mongodb flag adds I/O benchmarks

docs/optimizations/
  README.md           # This file
  attempts/           # One file per optimization attempt
    <name>.md         # What was tried, results, accepted/rejected
```

## Benchmark Conventions

- `bench_baseline.py` runs **without MongoDB** (pure Python paths).
  Default: `n=1000, repeat=5`.
- `bench_mongodb.py` requires a **running MongoDB instance**.
  Default: `n=100, repeat=3` (I/O-bound, smaller iterations).
- Report **median** time and **best-of-N** speedup.
- `gc.disable()` during timed loops (baseline only), `gc.enable()` after.
- Warm up before measuring (1 untimed call per operation).
- Improvement threshold: **>10%** to be considered meaningful.

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
