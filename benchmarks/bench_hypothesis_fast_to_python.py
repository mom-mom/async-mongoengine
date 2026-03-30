"""Hypothesis benchmark: fast to_python for ComplexBaseField.

Compares the legacy list->dict->sorted-list path against the new
direct-list-iteration path in ComplexBaseField.to_python.

Usage::

    uv run python benchmarks/bench_hypothesis_fast_to_python.py [n] [repeat]
"""

from __future__ import annotations

import gc
import statistics
import sys
import time
from collections.abc import Callable
from typing import Any

import mongoengine.base.fields as base_fields
from benchmarks.bench_baseline import (
    build_complex,
    build_simple,
    complex_kwargs,
    complex_son,
    simple_kwargs,
    simple_son,
)
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS


def _timed(fn: Callable[[], None], n: int) -> float:
    gc.disable()
    start = time.perf_counter()
    for _ in range(n):
        fn()
    elapsed = time.perf_counter() - start
    gc.enable()
    return elapsed


TARGET_OPS = ["_from_son", "from_json", "to_json"]


def run_scenario(
    label: str,
    build_fn: Callable[[], dict[str, type]],
    kwargs_fn: Callable[[dict[str, type]], dict[str, Any]],
    son_fn: Callable[[], dict[str, Any]],
    n: int,
    repeat: int,
    fast: bool,
) -> dict[str, dict[str, float]]:
    """Run target operations for one scenario."""
    base_fields.FAST_TO_PYTHON = fast

    classes = build_fn()
    doc_cls = classes[list(classes.keys())[-1]]
    kwargs = kwargs_fn(classes)
    son = son_fn()
    doc = doc_cls(**kwargs)
    json_str = doc.to_json(json_options=LEGACY_JSON_OPTIONS)

    # Warm up
    doc_cls._from_son(son)
    doc_cls.from_json(json_str, json_options=LEGACY_JSON_OPTIONS)
    doc.to_json(json_options=LEGACY_JSON_OPTIONS)

    raw: dict[str, list[float]] = {op: [] for op in TARGET_OPS}

    for _ in range(repeat):
        raw["_from_son"].append(_timed(lambda: doc_cls._from_son(son), n))
        raw["from_json"].append(_timed(lambda: doc_cls.from_json(json_str, json_options=LEGACY_JSON_OPTIONS), n))
        raw["to_json"].append(_timed(lambda: doc.to_json(json_options=LEGACY_JSON_OPTIONS), n))

    results: dict[str, dict[str, float]] = {}
    for op in TARGET_OPS:
        med = statistics.median(raw[op])
        best = min(raw[op])
        ops = n / best if best > 0 else float("inf")
        results[op] = {"median": med, "best": best, "ops_sec": ops}
    return results


def main(n: int = 1000, repeat: int = 5) -> None:
    print(f"Hypothesis: fast to_python — Python {sys.version.split()[0]}, n={n}, repeat={repeat}")

    for label, build_fn, kwargs_fn, son_fn in [
        ("Simple (6 fields, 1 embedded doc)", build_simple, simple_kwargs, simple_son),
        ("Complex (20 fields, 3-level nesting)", build_complex, complex_kwargs, complex_son),
    ]:
        print(f"\n{'=' * 65}")
        print(f"  {label}")
        print(f"{'=' * 65}")

        off = run_scenario(label, build_fn, kwargs_fn, son_fn, n, repeat, fast=False)
        on = run_scenario(label, build_fn, kwargs_fn, son_fn, n, repeat, fast=True)

        print(f"\n  {'Operation':<14s}{'legacy':>12s}{'fast':>12s}{'speedup':>10s}")
        print(f"  {'-' * 48}")
        for op in TARGET_OPS:
            leg_med = off[op]["median"] * 1000
            fast_med = on[op]["median"] * 1000
            speedup = off[op]["median"] / on[op]["median"]
            print(f"  {op:<14s}{leg_med:>9.2f}ms{fast_med:>9.2f}ms{speedup:>8.2f}x")

    print()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Hypothesis: fast to_python")
    parser.add_argument("n", nargs="?", type=int, default=1000)
    parser.add_argument("repeat", nargs="?", type=int, default=5)
    args = parser.parse_args()
    main(n=args.n, repeat=args.repeat)
