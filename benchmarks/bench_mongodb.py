"""End-to-end MongoDB benchmark measuring real I/O + serialization.

Requires a running MongoDB instance on localhost:27017.

Paths covered:
  - save           Document → validate → to_mongo → insert_one
  - find_one       find_one → _from_son
  - find_list      find → _from_son × N
  - update         modify + save (update_one)
  - aggregate      pipeline → cursor → _from_son

Usage::

    uv run python benchmarks/bench_mongodb.py [n] [repeat]
"""

from __future__ import annotations

import asyncio
import datetime
import decimal
import statistics
import sys
import time
from collections.abc import Callable, Coroutine
from typing import Any

import mongoengine
from mongoengine import Document, EmbeddedDocument, connect
from mongoengine.fields import (
    BooleanField,
    DateTimeField,
    DecimalField,
    DictField,
    EmailField,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    FloatField,
    IntField,
    ListField,
    StringField,
)

# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------


class BenchSkill(EmbeddedDocument):
    name = StringField(required=True, max_length=100)
    level = IntField(min_value=1, max_value=10)
    endorsed = BooleanField(default=False)
    meta = {"allow_inheritance": False}


class BenchAddress(EmbeddedDocument):
    street = StringField(required=True)
    city = StringField(required=True)
    state = StringField(max_length=2)
    zip_code = StringField(max_length=10)
    country = StringField(default="US")
    meta = {"allow_inheritance": False}


class BenchEmployee(Document):
    first_name = StringField(required=True, max_length=100)
    last_name = StringField(required=True, max_length=100)
    email = EmailField(required=True)
    age = IntField(min_value=18, max_value=100)
    rating = FloatField()
    is_active = BooleanField(default=True)
    joined_at = DateTimeField()
    annual_bonus = DecimalField(precision=2)
    home_address = EmbeddedDocumentField(BenchAddress)
    employment_skills = EmbeddedDocumentListField(BenchSkill)
    tags = ListField(StringField(max_length=50))
    scores = ListField(FloatField())
    metadata = DictField()
    meta = {"collection": "bench_employee", "allow_inheritance": False}


# ---------------------------------------------------------------------------
# Data factory
# ---------------------------------------------------------------------------


def make_employee(i: int = 0) -> BenchEmployee:
    return BenchEmployee(
        first_name=f"Employee_{i}",
        last_name="Benchmark",
        email=f"emp{i}@example.com",
        age=25 + (i % 40),
        rating=3.0 + (i % 20) / 10,
        is_active=i % 3 != 0,
        joined_at=datetime.datetime(2020, 1, 1) + datetime.timedelta(days=i),
        annual_bonus=decimal.Decimal(f"{50000 + i * 100}.00"),
        home_address=BenchAddress(
            street=f"{100 + i} Main St",
            city="Benchmark City",
            state="CA",
            zip_code=f"{90000 + i % 1000:05d}",
        ),
        employment_skills=[
            BenchSkill(name="Python", level=min(10, 5 + i % 6), endorsed=i % 2 == 0),
            BenchSkill(name="MongoDB", level=min(10, 3 + i % 8), endorsed=i % 3 == 0),
        ],
        tags=["engineering", "team-" + str(i % 5)],
        scores=[80.0 + i % 20, 90.0 - i % 15],
        metadata={"department": f"dept-{i % 10}", "floor": i % 5},
    )


# ---------------------------------------------------------------------------
# Async timing
# ---------------------------------------------------------------------------


async def _timed_async(fn: Callable[[], Coroutine[Any, Any, Any]], n: int) -> float:
    start = time.perf_counter()
    for _ in range(n):
        await fn()
    return time.perf_counter() - start


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

SEED_SIZE = 500
IO_OPS = ["save", "find_one", "find_list", "update", "aggregate"]


async def run_io_benchmarks(n: int, repeat: int) -> dict[str, dict[str, float]]:
    """Run all MongoDB I/O benchmarks."""
    await BenchEmployee.drop_collection()

    # Seed the collection
    print(f"  Seeding {SEED_SIZE} documents... ", end="", flush=True)
    for i in range(SEED_SIZE):
        await make_employee(i).save()
    print("done")

    raw: dict[str, list[float]] = {op: [] for op in IO_OPS}

    for r in range(repeat):
        print(f"  Run {r + 1}/{repeat}... ", end="", flush=True)

        # save: create new documents
        async def bench_save() -> None:
            doc = make_employee(SEED_SIZE + r * n)
            doc.id = None  # force new insert
            await doc.save()

        raw["save"].append(await _timed_async(bench_save, n))

        # find_one: random lookup by index
        async def bench_find_one() -> None:
            await BenchEmployee.objects(age=30).first()

        raw["find_one"].append(await _timed_async(bench_find_one, n))

        # find_list: fetch multiple documents
        async def bench_find_list() -> None:
            await BenchEmployee.objects(is_active=True).limit(50).to_list()

        raw["find_list"].append(await _timed_async(bench_find_list, n))

        # update: modify and save
        async def bench_update() -> None:
            doc = await BenchEmployee.objects.first()
            doc.rating = 4.5
            await doc.save()

        raw["update"].append(await _timed_async(bench_update, n))

        # aggregate: pipeline with match + group
        async def bench_aggregate() -> None:
            pipeline = [
                {"$match": {"is_active": True}},
                {"$group": {"_id": "$home_address.state", "avg_rating": {"$avg": "$rating"}}},
            ]
            agg = BenchEmployee.objects.aggregate(pipeline)
            async for _ in agg:
                pass

        raw["aggregate"].append(await _timed_async(bench_aggregate, n))

        print("done")

    # Cleanup
    await BenchEmployee.drop_collection()

    results: dict[str, dict[str, float]] = {}
    for op in IO_OPS:
        med = statistics.median(raw[op])
        best = min(raw[op])
        ops = n / best if best > 0 else float("inf")
        results[op] = {"median": med, "best": best, "ops_sec": ops}
    return results


def print_results(results: dict[str, dict[str, float]], n: int, repeat: int) -> None:
    print(f"\n{'=' * 60}")
    print(f"  MongoDB I/O Benchmark (n={n}, repeat={repeat})")
    print(f"{'=' * 60}\n")
    print(f"  {'Operation':<16s}{'median':>12s}{'best':>12s}{'ops/sec':>12s}")
    print(f"  {'-' * 52}")
    for op in IO_OPS:
        r = results[op]
        print(f"  {op:<16s}{r['median'] * 1000:>9.2f}ms{r['best'] * 1000:>9.2f}ms{r['ops_sec']:>10.0f}")
    print()

    # Show per-op averages
    total_best = sum(r["best"] for r in results.values())
    print(f"  Total best time for {n} iterations of each op: {total_best * 1000:.1f}ms")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def async_main(n: int = 100, repeat: int = 3) -> None:
    print(f"MongoDB I/O benchmark — Python {sys.version.split()[0]}")
    print(f"n={n}, repeat={repeat}\n")

    connect("bench_async_mongoengine")

    results = await run_io_benchmarks(n, repeat)
    print_results(results, n, repeat)

    await mongoengine.disconnect()


def main(n: int = 100, repeat: int = 3) -> None:
    asyncio.run(async_main(n, repeat))


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    repeat = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    main(n, repeat)
