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

    all_results: list[tuple[str, dict]] = []
    for name, build_fn, kwargs_fn, son_fn, label in mod.ALL_SCENARIOS:  # type: ignore[attr-defined]
        results = mod.run_scenario(name, build_fn, kwargs_fn, son_fn, n, repeat)  # type: ignore[attr-defined]
        mod.print_scenario(label, results)  # type: ignore[attr-defined]
        all_results.append((name.lower().replace(" ", "_"), results))

    return mod.results_to_dict(*all_results)  # type: ignore[attr-defined]


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

        # Run the current branch's benchmark script but with mongoengine
        # imported from the main branch worktree.  The benchmark harness
        # (bench_baseline.py) stays the same — only the library code differs.
        bench_file = str(BENCH_DIR / "bench_baseline.py")
        script = f"""\
import sys, json, importlib.util

# Main branch's mongoengine takes priority over the virtualenv's copy
sys.path.insert(0, {str(worktree_dir)!r})

# Explicitly load the current branch's benchmark script (not the worktree's)
spec = importlib.util.spec_from_file_location("bench_baseline", {str(bench_file)!r})
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

all_results = []
for name, build_fn, kwargs_fn, son_fn, label in mod.ALL_SCENARIOS:
    r = mod.run_scenario(name, build_fn, kwargs_fn, son_fn, {n}, {repeat})
    all_results.append((name.lower().replace(" ", "_"), r))
print(json.dumps(mod.results_to_dict(*all_results)))
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=worktree_dir, capture_output=True, text=True, timeout=600,
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


def run_mongodb(n: int, repeat: int) -> None:
    """Run the MongoDB I/O benchmark if available."""
    mongodb_bench = BENCH_DIR / "bench_mongodb.py"
    if not mongodb_bench.exists():
        return
    try:
        mod = load_module(mongodb_bench)
        if hasattr(mod, "main"):
            mod.main(n=n, repeat=repeat)  # type: ignore[attr-defined]
    except Exception as e:
        print(f"\n  MongoDB benchmark skipped: {e}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all benchmarks and compare vs main")
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--no-compare", action="store_true", help="Skip comparison against main")
    parser.add_argument("--mongodb", action="store_true", help="Include MongoDB I/O benchmarks (requires running mongod)")
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

    # --- MongoDB I/O ---
    if args.mongodb:
        run_mongodb(min(args.n, 100), min(args.repeat, 3))

    # --- Compare vs main ---
    if not args.no_compare and branch != "master":
        print("\n--- Running baseline on main for comparison ---\n")
        main_results = run_baseline_on_main(args.n, args.repeat)
        if main_results:
            compare(current, main_results)
    elif branch == "master":
        print("\nAlready on master — nothing to compare against.")
    else:
        print("\nComparison skipped (--no-compare).")


if __name__ == "__main__":
    main()
