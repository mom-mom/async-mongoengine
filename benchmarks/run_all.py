"""Run benchmarks on the current branch and compare against main via git.

Adds a temporary git worktree for the main branch, runs the same
benchmark there, and prints a side-by-side comparison.

Usage::

    # Compare current branch vs main
    uv run python benchmarks/run_all.py

    # Current branch only (no comparison)
    uv run python benchmarks/run_all.py --no-compare

    # Override n/repeat
    uv run python benchmarks/run_all.py --n 2000 --repeat 7
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

BENCH_DIR = Path(__file__).parent
REPO_ROOT = BENCH_DIR.parent

REGRESSION_THRESHOLD = 0.90   # flag if current is >10% slower
IMPROVEMENT_THRESHOLD = 1.10  # flag if current is >10% faster


def load_module(path: Path) -> object:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_baseline(n: int, repeat: int) -> dict[str, float]:
    """Run bench_baseline.py and return flattened results dict."""
    mod = load_module(BENCH_DIR / "bench_baseline.py")
    simple = mod.run_scenario("Simple", mod.build_simple, mod.simple_kwargs, mod.simple_son, n, repeat)  # type: ignore[attr-defined]
    mod.print_scenario("Simple (6 fields, 1 embedded doc)", simple)  # type: ignore[attr-defined]

    complex_ = mod.run_scenario("Complex", mod.build_complex, mod.complex_kwargs, mod.complex_son, n, repeat)  # type: ignore[attr-defined]
    mod.print_scenario("Complex (20 fields, 3-level nesting)", complex_)  # type: ignore[attr-defined]

    return mod.results_to_dict(simple, complex_)  # type: ignore[attr-defined]


def run_hypothesis_scripts(n: int, repeat: int) -> None:
    """Run any bench_hypothesis_*.py scripts found in the benchmarks dir."""
    for path in sorted(BENCH_DIR.glob("bench_hypothesis_*.py")):
        print(f"\n{'#' * 60}")
        print(f"  Hypothesis: {path.stem}")
        print(f"{'#' * 60}")
        mod = load_module(path)
        if hasattr(mod, "main"):
            mod.main(n=n, repeat=repeat)  # type: ignore[attr-defined]
        else:
            print(f"  WARNING: {path.name} has no main() function, skipping")


def run_baseline_on_main(n: int, repeat: int) -> dict[str, float] | None:
    """Check out main in a temporary git worktree, run bench_baseline there."""
    worktree_dir = tempfile.mkdtemp(prefix="bench_main_")
    try:
        # Create worktree
        subprocess.run(
            ["git", "worktree", "add", "--detach", worktree_dir, "origin/master"],
            cwd=REPO_ROOT, capture_output=True, check=True,
        )

        # Run benchmark in a subprocess so its imports use the main branch code
        script = f"""\
import sys, json
sys.path.insert(0, {str(worktree_dir)!r})
from benchmarks.bench_baseline import (
    run_scenario, build_simple, simple_kwargs, simple_son,
    build_complex, complex_kwargs, complex_son, results_to_dict,
)
simple = run_scenario("Simple", build_simple, simple_kwargs, simple_son, {n}, {repeat})
complex_ = run_scenario("Complex", build_complex, complex_kwargs, complex_son, {n}, {repeat})
print(json.dumps(results_to_dict(simple, complex_)))
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=worktree_dir, capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            print(f"  WARNING: main branch benchmark failed:\n{result.stderr[:500]}")
            return None

        # Extract the last line (JSON output) from stdout
        lines = result.stdout.strip().split("\n")
        return json.loads(lines[-1])

    except subprocess.CalledProcessError as e:
        print(f"  WARNING: could not create worktree for main: {e.stderr}")
        return None
    except Exception as e:
        print(f"  WARNING: main branch benchmark error: {e}")
        return None
    finally:
        # Clean up worktree
        subprocess.run(
            ["git", "worktree", "remove", "--force", worktree_dir],
            cwd=REPO_ROOT, capture_output=True,
        )
        if os.path.exists(worktree_dir):
            shutil.rmtree(worktree_dir, ignore_errors=True)


def compare(current: dict[str, float], main_results: dict[str, float]) -> None:
    """Print a comparison table: current branch vs main."""
    print(f"\n{'=' * 72}")
    print("  Comparison: current branch vs main (git)")
    print(f"{'=' * 72}\n")
    print(f"  {'metric':<40s}{'main':>10s}{'current':>10s}{'change':>12s}")
    print(f"  {'-' * 72}")

    regressions = []
    improvements = []

    for key in sorted(current.keys()):
        if not key.endswith("/best_ms"):
            continue

        cur = current[key]
        prev = main_results.get(key)
        if prev is None:
            continue

        ratio = prev / cur if cur > 0 else float("inf")
        label = key.replace("/best_ms", "")

        if ratio < REGRESSION_THRESHOLD:
            marker = " !! REGRESSION"
            regressions.append(label)
        elif ratio > IMPROVEMENT_THRESHOLD:
            marker = " ** FASTER"
            improvements.append(label)
        else:
            marker = ""

        print(f"  {label:<40s}{prev:>8.2f}ms{cur:>8.2f}ms{ratio:>8.2f}x{marker}")

    print()
    if regressions:
        print(f"  REGRESSIONS ({len(regressions)}):")
        for r in regressions:
            print(f"    - {r}")
    if improvements:
        print(f"  IMPROVEMENTS ({len(improvements)}):")
        for i in improvements:
            print(f"    - {i}")
    if not regressions and not improvements:
        print("  No significant changes (within 10% threshold).")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all benchmarks and compare vs main")
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--no-compare", action="store_true", help="Skip comparison against main")
    args = parser.parse_args()

    print(f"Python {sys.version.split()[0]}")
    print(f"n={args.n}, repeat={args.repeat}\n")

    # --- Current branch ---
    branch = subprocess.run(
        ["git", "branch", "--show-current"], cwd=REPO_ROOT,
        capture_output=True, text=True,
    ).stdout.strip() or "HEAD"
    print(f"--- Current branch: {branch} ---\n")
    current = run_baseline(args.n, args.repeat)

    # --- Hypothesis benchmarks ---
    run_hypothesis_scripts(args.n, args.repeat)

    # --- Compare vs main ---
    if not args.no_compare and branch != "master":
        print(f"\n--- Running baseline on main for comparison ---\n")
        main_results = run_baseline_on_main(args.n, args.repeat)
        if main_results:
            compare(current, main_results)
    elif branch == "master":
        print("\nAlready on master — nothing to compare against.")
    else:
        print("\nComparison skipped (--no-compare).")


if __name__ == "__main__":
    main()
