"""Benchmark: to_mongo() optimization hypothesis.

Compares baseline vs optimized to_mongo implementation by toggling
the FAST_TO_MONGO flag on BaseDocument.

Usage::

    uv run python benchmarks/bench_hypothesis_to_mongo.py [n] [repeat]

Does NOT require a running MongoDB instance.
"""

from __future__ import annotations

import datetime
import decimal
import gc
import statistics
import sys
import time
import uuid

from mongoengine import Document, EmbeddedDocument
from mongoengine.base.document import BaseDocument
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
    URLField,
    UUIDField,
)
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS

# ---------------------------------------------------------------------------
# Document definitions
# ---------------------------------------------------------------------------


class Skill(EmbeddedDocument):
    name = StringField(required=True, max_length=100)
    level = IntField(min_value=1, max_value=10)
    endorsed = BooleanField(default=False)
    meta = {"allow_inheritance": False}


class Address(EmbeddedDocument):
    street = StringField(required=True)
    city = StringField(required=True)
    state = StringField(max_length=2)
    zip_code = StringField(max_length=10)
    country = StringField(default="US")
    meta = {"allow_inheritance": False}


class Employment(EmbeddedDocument):
    company = StringField(required=True, max_length=200)
    title = StringField(max_length=200)
    start_date = DateTimeField()
    end_date = DateTimeField()
    salary = DecimalField(precision=2)
    is_current = BooleanField(default=False)
    skills = EmbeddedDocumentListField(Skill)
    meta = {"allow_inheritance": False}


class SimpleDoc(Document):
    name = StringField(required=True, max_length=200)
    age = IntField(min_value=0, max_value=200)
    score = FloatField()
    active = BooleanField(default=True)
    tags = ListField(StringField())
    address = EmbeddedDocumentField(Address)
    meta = {"collection": "bench_simple_hyp", "allow_inheritance": False}


class ComplexDoc(Document):
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
    home_address = EmbeddedDocumentField(Address)
    work_address = EmbeddedDocumentField(Address)
    employment_history = EmbeddedDocumentListField(Employment)
    tags = ListField(StringField(max_length=50))
    scores = ListField(FloatField())
    nicknames = ListField(StringField())
    metadata = DictField()
    preferences = DictField()
    meta = {"collection": "bench_complex_hyp", "allow_inheritance": False}


# ---------------------------------------------------------------------------
# Sample documents
# ---------------------------------------------------------------------------


def make_simple_doc() -> SimpleDoc:
    return SimpleDoc(
        name="Alice Wonderland",
        age=30,
        score=95.5,
        active=True,
        tags=["python", "mongodb", "async"],
        address=Address(street="123 Main St", city="NYC", zip_code="10001"),
    )


def make_complex_doc() -> ComplexDoc:
    return ComplexDoc(
        first_name="Alice",
        last_name="Wonderland",
        email="alice@example.com",
        website="https://alice.dev",
        age=30,
        rating=4.85,
        employee_id=uuid.uuid4(),
        is_active=True,
        joined_at=datetime.datetime(2020, 1, 15, 9, 30, 0),
        annual_bonus=decimal.Decimal("15000.50"),
        home_address=Address(
            street="123 Main St",
            city="New York",
            state="NY",
            zip_code="10001",
            country="US",
        ),
        work_address=Address(
            street="456 Office Blvd",
            city="San Francisco",
            state="CA",
            zip_code="94102",
            country="US",
        ),
        employment_history=[
            Employment(
                company="TechCorp Inc.",
                title="Senior Engineer",
                start_date=datetime.datetime(2020, 1, 15),
                salary=decimal.Decimal("150000.00"),
                is_current=True,
                skills=[
                    Skill(name="Python", level=9, endorsed=True),
                    Skill(name="MongoDB", level=8, endorsed=True),
                    Skill(name="Docker", level=7, endorsed=False),
                ],
            ),
            Employment(
                company="StartupXYZ",
                title="Full Stack Developer",
                start_date=datetime.datetime(2017, 6, 1),
                end_date=datetime.datetime(2019, 12, 31),
                salary=decimal.Decimal("95000.00"),
                is_current=False,
                skills=[
                    Skill(name="JavaScript", level=6, endorsed=True),
                    Skill(name="React", level=5, endorsed=False),
                ],
            ),
        ],
        tags=["engineering", "backend", "python", "senior", "team-lead"],
        scores=[95.5, 88.3, 92.1, 97.0, 85.6],
        nicknames=["Ali", "Wonder"],
        metadata={"department": "Engineering", "floor": 3, "badge_id": "ENG-1234"},
        preferences={"theme": "dark", "notifications": True, "language": "en"},
    )


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


def _timed(fn, n: int) -> float:
    gc.disable()
    start = time.perf_counter()
    for _ in range(n):
        fn()
    elapsed = time.perf_counter() - start
    gc.enable()
    return elapsed


def bench(label: str, fn, n: int, repeat: int) -> float:
    """Run benchmark, return median time in seconds."""
    times = [_timed(fn, n) for _ in range(repeat)]
    med = statistics.median(times)
    best = min(times)
    ops = n / best if best > 0 else float("inf")
    print(f"  {label:<30s}{med * 1000:>9.2f}ms{best * 1000:>9.2f}ms{ops:>10.0f}")
    return med


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(n: int = 1000, repeat: int = 5) -> None:
    print(f"to_mongo hypothesis benchmark — Python {sys.version.split()[0]}")
    print(f"n={n}, repeat={repeat}\n")

    simple_doc = make_simple_doc()
    complex_doc = make_complex_doc()

    # Also test to_json which calls to_mongo internally
    json_opts = LEGACY_JSON_OPTIONS

    for scenario, doc in [("Simple", simple_doc), ("Complex", complex_doc)]:
        print(f"\n{'=' * 64}")
        print(f"  {scenario}")
        print(f"{'=' * 64}")

        # --- Baseline (flag OFF) ---
        BaseDocument._fast_to_mongo = False
        print(f"\n  {'[flag OFF]':<30s}{'median':>9s}{'best':>9s}{'ops/sec':>10s}")
        print(f"  {'-' * 58}")

        # Warm up
        doc.to_mongo()
        doc.to_json(json_options=json_opts)

        off_to_mongo = bench("to_mongo", doc.to_mongo, n, repeat)
        off_to_json = bench("to_json", lambda: doc.to_json(json_options=json_opts), n, repeat)

        # --- Optimized (flag ON) ---
        BaseDocument._fast_to_mongo = True
        print(f"\n  {'[flag ON]':<30s}{'median':>9s}{'best':>9s}{'ops/sec':>10s}")
        print(f"  {'-' * 58}")

        # Warm up
        doc.to_mongo()
        doc.to_json(json_options=json_opts)

        on_to_mongo = bench("to_mongo", doc.to_mongo, n, repeat)
        on_to_json = bench("to_json", lambda: doc.to_json(json_options=json_opts), n, repeat)

        # --- Summary ---
        print(f"\n  Speedup: to_mongo {off_to_mongo / on_to_mongo:.2f}x, to_json {off_to_json / on_to_json:.2f}x")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    repeat = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    main(n=n, repeat=repeat)
