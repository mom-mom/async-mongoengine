"""Benchmark: fast_from_son optimisation vs baseline _from_son.

Measures the performance improvement of the optimised ``_from_son``
implementation (``PERF_FLAGS["fast_from_son"] = True``) against the
original path across two document complexity levels.

Usage::

    uv run python benchmarks/bench_perf_flags.py [n] [repeat]

Does NOT require a running MongoDB instance.
"""

from __future__ import annotations

import datetime
import decimal
import gc
import statistics
import sys
import textwrap
import time
import uuid
from collections.abc import Callable
from typing import Any

from bson import ObjectId

import mongoengine
from mongoengine.base.perf_config import PERF_FLAGS
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS

# ---------------------------------------------------------------------------
# Document class builders
# ---------------------------------------------------------------------------


def _build_simple_classes() -> dict[str, type]:
    """Simple: 6 fields, 1 embedded doc."""
    _ts = str(int(time.monotonic_ns()))
    ns: dict[str, Any] = {}
    exec(
        textwrap.dedent(f"""\
        from mongoengine import EmbeddedDocument, Document
        from mongoengine.fields import (
            StringField, IntField, FloatField, BooleanField,
            ListField, EmbeddedDocumentField,
        )

        class Addr_{_ts}(EmbeddedDocument):
            street = StringField()
            city = StringField()
            zip_code = StringField()
            meta = {{"allow_inheritance": False}}

        class Person_{_ts}(Document):
            name = StringField(required=True, max_length=200)
            age = IntField(min_value=0, max_value=200)
            score = FloatField()
            active = BooleanField(default=True)
            tags = ListField(StringField())
            address = EmbeddedDocumentField(Addr_{_ts})
            meta = {{"collection": "bench_simple", "allow_inheritance": False}}
        """),
        ns,
    )
    return {"address_cls": ns[f"Addr_{_ts}"], "doc_cls": ns[f"Person_{_ts}"]}


def _build_complex_classes() -> dict[str, type]:
    """Complex: 20+ fields, 3 levels of nesting, diverse field types."""
    _ts = str(int(time.monotonic_ns()))
    ns: dict[str, Any] = {}
    exec(
        textwrap.dedent(f"""\
        import datetime, decimal, uuid
        from mongoengine import EmbeddedDocument, Document
        from mongoengine.fields import (
            StringField, IntField, FloatField, BooleanField,
            ListField, EmbeddedDocumentField, DictField,
            DateTimeField, DecimalField, UUIDField,
            URLField, EmailField, EmbeddedDocumentListField,
        )

        class Skill_{_ts}(EmbeddedDocument):
            name = StringField(required=True, max_length=100)
            level = IntField(min_value=1, max_value=10)
            endorsed = BooleanField(default=False)
            meta = {{"allow_inheritance": False}}

        class Address_{_ts}(EmbeddedDocument):
            street = StringField(required=True)
            city = StringField(required=True)
            state = StringField(max_length=2)
            zip_code = StringField(max_length=10)
            country = StringField(default="US")
            meta = {{"allow_inheritance": False}}

        class Employment_{_ts}(EmbeddedDocument):
            company = StringField(required=True, max_length=200)
            title = StringField(max_length=200)
            start_date = DateTimeField()
            end_date = DateTimeField()
            salary = DecimalField(precision=2)
            is_current = BooleanField(default=False)
            skills = EmbeddedDocumentListField(Skill_{_ts})
            meta = {{"allow_inheritance": False}}

        class Employee_{_ts}(Document):
            first_name = StringField(required=True, max_length=100)
            last_name = StringField(required=True, max_length=100)
            email = EmailField(required=True)
            website = URLField()
            age = IntField(min_value=18, max_value=100)
            rating = FloatField()
            employee_id = UUIDField()
            is_active = BooleanField(default=True)
            joined_at = DateTimeField()
            annual_bonus = DecimalField(precision=2)
            home_address = EmbeddedDocumentField(Address_{_ts})
            work_address = EmbeddedDocumentField(Address_{_ts})
            employment_history = EmbeddedDocumentListField(Employment_{_ts})
            tags = ListField(StringField(max_length=50))
            scores = ListField(FloatField())
            nicknames = ListField(StringField())
            metadata = DictField()
            preferences = DictField()
            meta = {{"collection": "bench_complex", "allow_inheritance": False}}
        """),
        ns,
    )
    return {
        "skill_cls": ns[f"Skill_{_ts}"],
        "address_cls": ns[f"Address_{_ts}"],
        "employment_cls": ns[f"Employment_{_ts}"],
        "doc_cls": ns[f"Employee_{_ts}"],
    }


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


def make_simple_data(classes: dict[str, type]) -> dict[str, Any]:
    return {
        "name": "Alice Wonderland",
        "age": 30,
        "score": 95.5,
        "active": True,
        "tags": ["python", "mongodb", "async"],
        "address": classes["address_cls"](
            street="123 Main St", city="NYC", zip_code="10001",
        ),
    }


def make_simple_son() -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "name": "Alice Wonderland",
        "age": 30,
        "score": 95.5,
        "active": True,
        "tags": ["python", "mongodb", "async"],
        "address": {"street": "123 Main St", "city": "NYC", "zip_code": "10001"},
    }


def make_complex_data(classes: dict[str, type]) -> dict[str, Any]:
    skill_cls = classes["skill_cls"]
    addr_cls = classes["address_cls"]
    emp_cls = classes["employment_cls"]
    return {
        "first_name": "Alice",
        "last_name": "Wonderland",
        "email": "alice@example.com",
        "website": "https://alice.dev",
        "age": 30,
        "rating": 4.85,
        "employee_id": uuid.uuid4(),
        "is_active": True,
        "joined_at": datetime.datetime(2020, 1, 15, 9, 30, 0),
        "annual_bonus": decimal.Decimal("15000.50"),
        "home_address": addr_cls(
            street="123 Main St", city="New York", state="NY",
            zip_code="10001", country="US",
        ),
        "work_address": addr_cls(
            street="456 Office Blvd", city="San Francisco", state="CA",
            zip_code="94102", country="US",
        ),
        "employment_history": [
            emp_cls(
                company="TechCorp Inc.", title="Senior Engineer",
                start_date=datetime.datetime(2020, 1, 15), end_date=None,
                salary=decimal.Decimal("150000.00"), is_current=True,
                skills=[
                    skill_cls(name="Python", level=9, endorsed=True),
                    skill_cls(name="MongoDB", level=8, endorsed=True),
                    skill_cls(name="Docker", level=7, endorsed=False),
                ],
            ),
            emp_cls(
                company="StartupXYZ", title="Full Stack Developer",
                start_date=datetime.datetime(2017, 6, 1),
                end_date=datetime.datetime(2019, 12, 31),
                salary=decimal.Decimal("95000.00"), is_current=False,
                skills=[
                    skill_cls(name="JavaScript", level=6, endorsed=True),
                    skill_cls(name="React", level=5, endorsed=False),
                ],
            ),
        ],
        "tags": ["engineering", "backend", "python", "senior", "team-lead"],
        "scores": [95.5, 88.3, 92.1, 97.0, 85.6],
        "nicknames": ["Ali", "Wonder"],
        "metadata": {"department": "Engineering", "floor": 3, "badge_id": "ENG-1234", "clearance": "L2"},
        "preferences": {"theme": "dark", "notifications": True, "language": "en", "timezone": "US/Eastern"},
    }


def make_complex_son() -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "first_name": "Alice",
        "last_name": "Wonderland",
        "email": "alice@example.com",
        "website": "https://alice.dev",
        "age": 30,
        "rating": 4.85,
        "employee_id": str(uuid.uuid4()),
        "is_active": True,
        "joined_at": datetime.datetime(2020, 1, 15, 9, 30, 0),
        "annual_bonus": decimal.Decimal("15000.50"),
        "home_address": {
            "street": "123 Main St", "city": "New York", "state": "NY",
            "zip_code": "10001", "country": "US",
        },
        "work_address": {
            "street": "456 Office Blvd", "city": "San Francisco", "state": "CA",
            "zip_code": "94102", "country": "US",
        },
        "employment_history": [
            {
                "company": "TechCorp Inc.", "title": "Senior Engineer",
                "start_date": datetime.datetime(2020, 1, 15), "end_date": None,
                "salary": decimal.Decimal("150000.00"), "is_current": True,
                "skills": [
                    {"name": "Python", "level": 9, "endorsed": True},
                    {"name": "MongoDB", "level": 8, "endorsed": True},
                    {"name": "Docker", "level": 7, "endorsed": False},
                ],
            },
            {
                "company": "StartupXYZ", "title": "Full Stack Developer",
                "start_date": datetime.datetime(2017, 6, 1),
                "end_date": datetime.datetime(2019, 12, 31),
                "salary": decimal.Decimal("95000.00"), "is_current": False,
                "skills": [
                    {"name": "JavaScript", "level": 6, "endorsed": True},
                    {"name": "React", "level": 5, "endorsed": False},
                ],
            },
        ],
        "tags": ["engineering", "backend", "python", "senior", "team-lead"],
        "scores": [95.5, 88.3, 92.1, 97.0, 85.6],
        "nicknames": ["Ali", "Wonder"],
        "metadata": {"department": "Engineering", "floor": 3, "badge_id": "ENG-1234", "clearance": "L2"},
        "preferences": {"theme": "dark", "notifications": True, "language": "en", "timezone": "US/Eastern"},
    }


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def _timed(fn: Callable[[], None], n: int) -> float:
    gc.disable()
    start = time.perf_counter()
    for _ in range(n):
        fn()
    elapsed = time.perf_counter() - start
    gc.enable()
    return elapsed


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

OP_NAMES = ["_from_son", "from_json"]


def run_scenario(
    scenario_name: str,
    build_fn: Callable[[], dict[str, type]],
    make_data_fn: Callable[[dict[str, type]], dict[str, Any]],
    make_son_fn: Callable[[], dict[str, Any]],
    n: int,
    repeat: int,
) -> None:
    configs = [
        ("baseline", False),
        ("fast_from_son", True),
    ]

    all_results: dict[str, dict[str, list[float]]] = {}

    print(f"\n{'=' * 66}")
    print(f"  {scenario_name}   (n={n}, repeat={repeat})")
    print(f"{'=' * 66}\n")

    for config_name, flag_value in configs:
        sys.stdout.write(f"  {config_name:<20s} ... ")
        sys.stdout.flush()

        PERF_FLAGS["fast_from_son"] = flag_value
        classes = build_fn()
        doc_cls = classes["doc_cls"]

        son = make_son_fn()
        data = make_data_fn(classes)
        doc = doc_cls(**data)
        json_str = doc.to_json(json_options=LEGACY_JSON_OPTIONS)

        results: dict[str, list[float]] = {op: [] for op in OP_NAMES}
        for _ in range(repeat):
            results["_from_son"].append(
                _timed(lambda: doc_cls._from_son(son), n),
            )
            results["from_json"].append(
                _timed(lambda: doc_cls.from_json(json_str, json_options=LEGACY_JSON_OPTIONS), n),
            )

        all_results[config_name] = results
        print("done")

    PERF_FLAGS["fast_from_son"] = False

    # --- Table ---
    print()
    print(f"  {'Operation':<14s}{'baseline':>14s}{'fast_from_son':>14s}{'speedup':>12s}")
    print(f"  {'-' * 54}")

    for op in OP_NAMES:
        base_med = statistics.median(all_results["baseline"][op])
        fast_med = statistics.median(all_results["fast_from_son"][op])
        base_best = min(all_results["baseline"][op])
        fast_best = min(all_results["fast_from_son"][op])
        speedup = base_best / fast_best if fast_best > 0 else float("inf")
        print(
            f"  {op:<14s}"
            f"{base_med * 1000:>11.2f}ms"
            f"{fast_med * 1000:>11.2f}ms"
            f"{speedup:>10.2f}x"
        )

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(n: int = 1000, repeat: int = 5) -> None:
    print(f"fast_from_son benchmark — Python {sys.version.split()[0]}")

    run_scenario(
        "Simple (6 fields, 1 embedded doc)",
        _build_simple_classes, make_simple_data, make_simple_son,
        n, repeat,
    )
    run_scenario(
        "Complex (20 fields, 3-level nesting, lists of embedded docs)",
        _build_complex_classes, make_complex_data, make_complex_son,
        n, repeat,
    )

    print("Speedup = best-of-N / best-of-N.")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    repeat = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    main(n=n, repeat=repeat)
