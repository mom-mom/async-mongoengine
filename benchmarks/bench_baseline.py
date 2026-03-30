"""Baseline benchmark covering all core Document lifecycle paths.

Measures every significant pure-Python path so that future optimizations
can be checked for regressions.  Does NOT require a running MongoDB instance.

Paths covered:
  - __init__       Document construction from kwargs
  - validate       Field validation
  - to_mongo       Python → BSON-ready dict
  - _from_son      BSON dict → Document (deserialization)
  - to_json        Document → JSON string
  - from_json      JSON string → Document
  - bulk_from_son  _from_son × 1000 (simulates cursor iteration)

Usage::

    uv run python benchmarks/bench_baseline.py [n] [repeat]
"""

from __future__ import annotations

import datetime
import decimal
import gc
import json
import statistics
import sys
import textwrap
import time
import uuid
from collections.abc import Callable
from typing import Any

from bson import ObjectId

import mongoengine
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS

# ---------------------------------------------------------------------------
# Document class builders
# ---------------------------------------------------------------------------

_SIMPLE_TMPL = """\
from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import (
    StringField, IntField, FloatField, BooleanField,
    ListField, EmbeddedDocumentField,
)

class Addr_{ts}(EmbeddedDocument):
    street = StringField()
    city = StringField()
    zip_code = StringField()
    meta = {{"allow_inheritance": False}}

class Person_{ts}(Document):
    name = StringField(required=True, max_length=200)
    age = IntField(min_value=0, max_value=200)
    score = FloatField()
    active = BooleanField(default=True)
    tags = ListField(StringField())
    address = EmbeddedDocumentField(Addr_{ts})
    meta = {{"collection": "bench_simple", "allow_inheritance": False}}
"""

_COMPLEX_TMPL = """\
import datetime, decimal, uuid
from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import (
    StringField, IntField, FloatField, BooleanField,
    ListField, EmbeddedDocumentField, DictField,
    DateTimeField, DecimalField, UUIDField,
    URLField, EmailField, EmbeddedDocumentListField,
)

class Skill_{ts}(EmbeddedDocument):
    name = StringField(required=True, max_length=100)
    level = IntField(min_value=1, max_value=10)
    endorsed = BooleanField(default=False)
    meta = {{"allow_inheritance": False}}

class Address_{ts}(EmbeddedDocument):
    street = StringField(required=True)
    city = StringField(required=True)
    state = StringField(max_length=2)
    zip_code = StringField(max_length=10)
    country = StringField(default="US")
    meta = {{"allow_inheritance": False}}

class Employment_{ts}(EmbeddedDocument):
    company = StringField(required=True, max_length=200)
    title = StringField(max_length=200)
    start_date = DateTimeField()
    end_date = DateTimeField()
    salary = DecimalField(precision=2)
    is_current = BooleanField(default=False)
    skills = EmbeddedDocumentListField(Skill_{ts})
    meta = {{"allow_inheritance": False}}

class Employee_{ts}(Document):
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
    home_address = EmbeddedDocumentField(Address_{ts})
    work_address = EmbeddedDocumentField(Address_{ts})
    employment_history = EmbeddedDocumentListField(Employment_{ts})
    tags = ListField(StringField(max_length=50))
    scores = ListField(FloatField())
    nicknames = ListField(StringField())
    metadata = DictField()
    preferences = DictField()
    meta = {{"collection": "bench_complex", "allow_inheritance": False}}
"""


def _build_classes(template: str, class_names: list[str]) -> dict[str, type]:
    ts = str(int(time.monotonic_ns()))
    ns: dict[str, Any] = {}
    exec(textwrap.dedent(template.format(ts=ts)), ns)
    return {name: ns[f"{name}_{ts}"] for name in class_names}


def build_simple() -> dict[str, type]:
    return _build_classes(_SIMPLE_TMPL, ["Addr", "Person"])


def build_complex() -> dict[str, type]:
    return _build_classes(_COMPLEX_TMPL, ["Skill", "Address", "Employment", "Employee"])


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


def simple_kwargs(classes: dict[str, type]) -> dict[str, Any]:
    return {
        "name": "Alice Wonderland",
        "age": 30,
        "score": 95.5,
        "active": True,
        "tags": ["python", "mongodb", "async"],
        "address": classes["Addr"](street="123 Main St", city="NYC", zip_code="10001"),
    }


def simple_son() -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "name": "Alice Wonderland",
        "age": 30,
        "score": 95.5,
        "active": True,
        "tags": ["python", "mongodb", "async"],
        "address": {"street": "123 Main St", "city": "NYC", "zip_code": "10001"},
    }


def complex_kwargs(classes: dict[str, type]) -> dict[str, Any]:
    S, A, E = classes["Skill"], classes["Address"], classes["Employment"]
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
        "home_address": A(street="123 Main St", city="New York", state="NY", zip_code="10001", country="US"),
        "work_address": A(street="456 Office Blvd", city="San Francisco", state="CA", zip_code="94102", country="US"),
        "employment_history": [
            E(
                company="TechCorp Inc.", title="Senior Engineer",
                start_date=datetime.datetime(2020, 1, 15), end_date=None,
                salary=decimal.Decimal("150000.00"), is_current=True,
                skills=[S(name="Python", level=9, endorsed=True), S(name="MongoDB", level=8, endorsed=True), S(name="Docker", level=7, endorsed=False)],
            ),
            E(
                company="StartupXYZ", title="Full Stack Developer",
                start_date=datetime.datetime(2017, 6, 1), end_date=datetime.datetime(2019, 12, 31),
                salary=decimal.Decimal("95000.00"), is_current=False,
                skills=[S(name="JavaScript", level=6, endorsed=True), S(name="React", level=5, endorsed=False)],
            ),
        ],
        "tags": ["engineering", "backend", "python", "senior", "team-lead"],
        "scores": [95.5, 88.3, 92.1, 97.0, 85.6],
        "nicknames": ["Ali", "Wonder"],
        "metadata": {"department": "Engineering", "floor": 3, "badge_id": "ENG-1234", "clearance": "L2"},
        "preferences": {"theme": "dark", "notifications": True, "language": "en", "timezone": "US/Eastern"},
    }


def complex_son() -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "first_name": "Alice", "last_name": "Wonderland",
        "email": "alice@example.com", "website": "https://alice.dev",
        "age": 30, "rating": 4.85, "employee_id": str(uuid.uuid4()),
        "is_active": True,
        "joined_at": datetime.datetime(2020, 1, 15, 9, 30, 0),
        "annual_bonus": decimal.Decimal("15000.50"),
        "home_address": {"street": "123 Main St", "city": "New York", "state": "NY", "zip_code": "10001", "country": "US"},
        "work_address": {"street": "456 Office Blvd", "city": "San Francisco", "state": "CA", "zip_code": "94102", "country": "US"},
        "employment_history": [
            {
                "company": "TechCorp Inc.", "title": "Senior Engineer",
                "start_date": datetime.datetime(2020, 1, 15), "end_date": None,
                "salary": decimal.Decimal("150000.00"), "is_current": True,
                "skills": [{"name": "Python", "level": 9, "endorsed": True}, {"name": "MongoDB", "level": 8, "endorsed": True}, {"name": "Docker", "level": 7, "endorsed": False}],
            },
            {
                "company": "StartupXYZ", "title": "Full Stack Developer",
                "start_date": datetime.datetime(2017, 6, 1), "end_date": datetime.datetime(2019, 12, 31),
                "salary": decimal.Decimal("95000.00"), "is_current": False,
                "skills": [{"name": "JavaScript", "level": 6, "endorsed": True}, {"name": "React", "level": 5, "endorsed": False}],
            },
        ],
        "tags": ["engineering", "backend", "python", "senior", "team-lead"],
        "scores": [95.5, 88.3, 92.1, 97.0, 85.6],
        "nicknames": ["Ali", "Wonder"],
        "metadata": {"department": "Engineering", "floor": 3, "badge_id": "ENG-1234", "clearance": "L2"},
        "preferences": {"theme": "dark", "notifications": True, "language": "en", "timezone": "US/Eastern"},
    }


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------

BULK_SIZE = 1000


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

ALL_OPS = ["__init__", "validate", "to_mongo", "_from_son", "to_json", "from_json", "bulk_from_son"]


def run_scenario(
    label: str,
    build_fn: Callable[[], dict[str, type]],
    kwargs_fn: Callable[[dict[str, type]], dict[str, Any]],
    son_fn: Callable[[], dict[str, Any]],
    n: int,
    repeat: int,
) -> dict[str, dict[str, float]]:
    """Run all operations for one scenario. Returns {op: {median, best, ops_sec}}."""
    classes = build_fn()
    doc_cls = classes[list(classes.keys())[-1]]  # last class is the top-level Document

    kwargs = kwargs_fn(classes)
    son = son_fn()
    doc = doc_cls(**kwargs)
    json_str = doc.to_json(json_options=LEGACY_JSON_OPTIONS)
    bulk_sons = [son_fn() for _ in range(BULK_SIZE)]

    # Warm up
    doc_cls(**kwargs)
    doc.validate()
    doc.to_mongo()
    doc_cls._from_son(son)
    doc.to_json(json_options=LEGACY_JSON_OPTIONS)
    doc_cls.from_json(json_str, json_options=LEGACY_JSON_OPTIONS)
    [doc_cls._from_son(s) for s in bulk_sons[:3]]

    raw: dict[str, list[float]] = {op: [] for op in ALL_OPS}

    for _ in range(repeat):
        raw["__init__"].append(_timed(lambda: doc_cls(**kwargs), n))
        raw["validate"].append(_timed(doc.validate, n))
        raw["to_mongo"].append(_timed(doc.to_mongo, n))
        raw["_from_son"].append(_timed(lambda: doc_cls._from_son(son), n))
        raw["to_json"].append(_timed(lambda: doc.to_json(json_options=LEGACY_JSON_OPTIONS), n))
        raw["from_json"].append(_timed(lambda: doc_cls.from_json(json_str, json_options=LEGACY_JSON_OPTIONS), n))
        # Bulk: one "iteration" = deserialize BULK_SIZE documents
        raw["bulk_from_son"].append(_timed(lambda: [doc_cls._from_son(s) for s in bulk_sons], n // 10 or 1))

    results: dict[str, dict[str, float]] = {}
    for op in ALL_OPS:
        med = statistics.median(raw[op])
        best = min(raw[op])
        iter_count = n if op != "bulk_from_son" else (n // 10 or 1)
        ops = iter_count / best if best > 0 else float("inf")
        results[op] = {"median": med, "best": best, "ops_sec": ops}

    return results


def print_scenario(label: str, results: dict[str, dict[str, float]]) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}\n")
    print(f"  {'Operation':<16s}{'median':>12s}{'best':>12s}{'ops/sec':>12s}")
    print(f"  {'-' * 52}")
    for op in ALL_OPS:
        r = results[op]
        suffix = f" (x{BULK_SIZE})" if op == "bulk_from_son" else ""
        print(f"  {op + suffix:<16s}{r['median'] * 1000:>9.2f}ms{r['best'] * 1000:>9.2f}ms{r['ops_sec']:>10.0f}")
    print()


# ---------------------------------------------------------------------------
# JSON output for run_all.py comparison
# ---------------------------------------------------------------------------


def results_to_dict(
    simple: dict[str, dict[str, float]],
    complex_: dict[str, dict[str, float]],
) -> dict[str, Any]:
    """Flatten results into a JSON-serializable dict."""
    out: dict[str, Any] = {}
    for scenario, data in [("simple", simple), ("complex", complex_)]:
        for op, metrics in data.items():
            out[f"{scenario}/{op}/median_ms"] = round(metrics["median"] * 1000, 3)
            out[f"{scenario}/{op}/best_ms"] = round(metrics["best"] * 1000, 3)
            out[f"{scenario}/{op}/ops_sec"] = round(metrics["ops_sec"], 1)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(n: int = 1000, repeat: int = 5, json_output: str | None = None) -> None:
    print(f"Baseline benchmark — Python {sys.version.split()[0]}, n={n}, repeat={repeat}")

    simple_results = run_scenario("Simple", build_simple, simple_kwargs, simple_son, n, repeat)
    print_scenario("Simple (6 fields, 1 embedded doc)", simple_results)

    complex_results = run_scenario("Complex", build_complex, complex_kwargs, complex_son, n, repeat)
    print_scenario("Complex (20 fields, 3-level nesting)", complex_results)

    if json_output:
        data = results_to_dict(simple_results, complex_results)
        with open(json_output, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Results written to {json_output}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Baseline benchmark")
    parser.add_argument("n", nargs="?", type=int, default=1000)
    parser.add_argument("repeat", nargs="?", type=int, default=5)
    parser.add_argument("--json", dest="json_output", help="Write results to JSON file")
    args = parser.parse_args()
    main(n=args.n, repeat=args.repeat, json_output=args.json_output)
