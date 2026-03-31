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

Scenarios:
  - Simple          6 fields, 1 embedded doc
  - Complex         20 fields, 3-level nesting
  - All Fields      30 fields, every supported field type
  - Deep Nesting    5-level nested embedded documents

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

from bson import Decimal128, ObjectId

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

_ALL_FIELDS_TMPL = """\
import datetime, decimal, uuid, enum
from bson import ObjectId, Decimal128
from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import (
    StringField, IntField, FloatField, BooleanField,
    DateTimeField, DateField, ComplexDateTimeField,
    DecimalField, Decimal128Field,
    UUIDField, ObjectIdField, BinaryField,
    ListField, SortedListField,
    DictField, MapField,
    EmbeddedDocumentField, EmbeddedDocumentListField,
    GenericEmbeddedDocumentField,
    DynamicField, EnumField,
    URLField, EmailField,
    GeoPointField, PointField, LineStringField, PolygonField,
    MultiPointField, MultiLineStringField, MultiPolygonField,
)

class Priority_{ts}(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Label_{ts}(EmbeddedDocument):
    name = StringField(required=True)
    color = StringField(default="#000000")

class AllFields_{ts}(Document):
    # --- String variants ---
    title = StringField(required=True, max_length=200)
    email = EmailField()
    website = URLField()
    # --- Numeric ---
    count = IntField(min_value=0)
    rating = FloatField()
    price = DecimalField(precision=2)
    precise_amount = Decimal128Field()
    # --- Boolean ---
    is_published = BooleanField(default=False)
    # --- Date/Time variants ---
    created_at = DateTimeField()
    event_date = DateField()
    precise_time = ComplexDateTimeField()
    # --- ID types ---
    external_id = UUIDField()
    ref_id = ObjectIdField()
    # --- Binary ---
    thumbnail = BinaryField(max_bytes=4096)
    # --- Collection types ---
    tags = ListField(StringField(max_length=50))
    sorted_scores = SortedListField(IntField())
    options = DictField()
    counters = MapField(IntField())
    # --- Embedded documents ---
    primary_label = EmbeddedDocumentField(Label_{ts})
    extra_labels = EmbeddedDocumentListField(Label_{ts})
    flexible_data = GenericEmbeddedDocumentField()
    # --- Dynamic ---
    extra = DynamicField()
    # --- Enum ---
    priority = EnumField(Priority_{ts})
    # --- Geo (legacy) ---
    pin = GeoPointField()
    # --- Geo (GeoJSON) ---
    location = PointField()
    route = LineStringField()
    boundary = PolygonField()
    waypoints = MultiPointField()
    trails = MultiLineStringField()
    zones = MultiPolygonField()
    meta = {{"collection": "bench_all_fields", "allow_inheritance": False}}
"""

_DEEP_NESTING_TMPL = """\
import datetime, decimal
from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import (
    StringField, IntField, FloatField, BooleanField,
    DateTimeField, DecimalField,
    ListField, EmbeddedDocumentField, EmbeddedDocumentListField,
    DictField, MapField, SortedListField,
)

# Level 5 (deepest leaf)
class Metric_{ts}(EmbeddedDocument):
    name = StringField(required=True)
    value = FloatField()
    unit = StringField(default="unit")
    meta = {{"allow_inheritance": False}}

# Level 4
class Sensor_{ts}(EmbeddedDocument):
    sensor_id = StringField(required=True)
    type = StringField()
    metrics = EmbeddedDocumentListField(Metric_{ts})
    calibration = DictField()
    meta = {{"allow_inheritance": False}}

# Level 3
class Device_{ts}(EmbeddedDocument):
    name = StringField(required=True)
    model = StringField()
    active = BooleanField(default=True)
    sensors = EmbeddedDocumentListField(Sensor_{ts})
    tags = ListField(StringField())
    properties = MapField(StringField())
    meta = {{"allow_inheritance": False}}

# Level 2
class Room_{ts}(EmbeddedDocument):
    name = StringField(required=True)
    floor = IntField()
    devices = EmbeddedDocumentListField(Device_{ts})
    environment = DictField()
    meta = {{"allow_inheritance": False}}

# Level 1
class Floor_{ts}(EmbeddedDocument):
    number = IntField(required=True)
    name = StringField()
    rooms = EmbeddedDocumentListField(Room_{ts})
    summary = EmbeddedDocumentField(Device_{ts})
    meta = {{"allow_inheritance": False}}

# Level 0 (root Document)
class Building_{ts}(Document):
    name = StringField(required=True, max_length=200)
    address = StringField()
    floors = EmbeddedDocumentListField(Floor_{ts})
    main_entrance = EmbeddedDocumentField(Room_{ts})
    global_metrics = EmbeddedDocumentListField(Metric_{ts})
    config = DictField()
    meta = {{"collection": "bench_deep_nesting", "allow_inheritance": False}}
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


def build_all_fields() -> dict[str, type]:
    return _build_classes(_ALL_FIELDS_TMPL, ["Priority", "Label", "AllFields"])


def build_deep_nesting() -> dict[str, type]:
    return _build_classes(
        _DEEP_NESTING_TMPL,
        ["Metric", "Sensor", "Device", "Room", "Floor", "Building"],
    )


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


def simple_son(classes: dict[str, type]) -> dict[str, Any]:
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


def complex_son(classes: dict[str, type]) -> dict[str, Any]:
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


def all_fields_kwargs(classes: dict[str, type]) -> dict[str, Any]:
    Label = classes["Label"]
    Priority = classes["Priority"]
    return {
        # String variants
        "title": "Benchmark Document",
        "email": "bench@example.com",
        "website": "https://bench.example.com",
        # Numeric
        "count": 42,
        "rating": 4.75,
        "price": decimal.Decimal("99.99"),
        "precise_amount": decimal.Decimal("123456.789012345"),
        # Boolean
        "is_published": True,
        # Date/Time
        "created_at": datetime.datetime(2024, 6, 15, 10, 30, 0),
        "event_date": datetime.date(2024, 7, 1),
        "precise_time": datetime.datetime(2024, 6, 15, 10, 30, 0, 123456),
        # IDs
        "external_id": uuid.uuid4(),
        "ref_id": ObjectId(),
        # Binary
        "thumbnail": b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
        # Collections
        "tags": ["python", "mongodb", "benchmark", "performance"],
        "sorted_scores": [85, 92, 78, 95, 88, 73, 91],
        "options": {"theme": "dark", "lang": "en", "version": 2, "beta": True},
        "counters": {"views": 1500, "likes": 342, "shares": 47, "comments": 89},
        # Embedded
        "primary_label": Label(name="important", color="#ff0000"),
        "extra_labels": [
            Label(name="urgent", color="#ff6600"),
            Label(name="review", color="#0066ff"),
            Label(name="docs", color="#00cc00"),
        ],
        "flexible_data": Label(name="flexible", color="#00ff00"),
        # Dynamic
        "extra": {"nested": {"key": "value", "num": 42}, "list": [1, "two", 3.0]},
        # Enum
        "priority": Priority.HIGH,
        # Geo (legacy)
        "pin": [40.7128, -74.0060],
        # Geo (GeoJSON)
        "location": {"type": "Point", "coordinates": [-74.0060, 40.7128]},
        "route": {
            "type": "LineString",
            "coordinates": [[-74.0060, 40.7128], [-73.9857, 40.7484], [-73.9712, 40.7831]],
        },
        "boundary": {
            "type": "Polygon",
            "coordinates": [[
                [-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8],
                [-74.0, 40.8], [-74.0, 40.7],
            ]],
        },
        "waypoints": {
            "type": "MultiPoint",
            "coordinates": [[-74.0060, 40.7128], [-73.9857, 40.7484], [-73.9550, 40.7690]],
        },
        "trails": {
            "type": "MultiLineString",
            "coordinates": [
                [[-74.0, 40.7], [-73.95, 40.75], [-73.9, 40.8]],
                [[-73.8, 40.7], [-73.75, 40.75], [-73.7, 40.8]],
            ],
        },
        "zones": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74.0, 40.8], [-74.0, 40.7]]],
                [[[-73.8, 40.7], [-73.7, 40.7], [-73.7, 40.8], [-73.8, 40.8], [-73.8, 40.7]]],
            ],
        },
    }


def all_fields_son(classes: dict[str, type]) -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        # String variants
        "title": "Benchmark Document",
        "email": "bench@example.com",
        "website": "https://bench.example.com",
        # Numeric (DecimalField stores as float, Decimal128Field stores as Decimal128)
        "count": 42,
        "rating": 4.75,
        "price": 99.99,
        "precise_amount": Decimal128(decimal.Decimal("123456.789012345")),
        # Boolean
        "is_published": True,
        # Date/Time (DateField stores as datetime in MongoDB, ComplexDateTimeField as string)
        "created_at": datetime.datetime(2024, 6, 15, 10, 30, 0),
        "event_date": datetime.datetime(2024, 7, 1, 0, 0, 0),
        "precise_time": "2024,06,15,10,30,00,123456",
        # IDs
        "external_id": str(uuid.uuid4()),
        "ref_id": ObjectId(),
        # Binary
        "thumbnail": b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
        # Collections (SortedListField stores sorted)
        "tags": ["python", "mongodb", "benchmark", "performance"],
        "sorted_scores": [73, 78, 85, 88, 91, 92, 95],
        "options": {"theme": "dark", "lang": "en", "version": 2, "beta": True},
        "counters": {"views": 1500, "likes": 342, "shares": 47, "comments": 89},
        # Embedded (GenericEmbeddedDocumentField stores _cls)
        "primary_label": {"name": "important", "color": "#ff0000"},
        "extra_labels": [
            {"name": "urgent", "color": "#ff6600"},
            {"name": "review", "color": "#0066ff"},
            {"name": "docs", "color": "#00cc00"},
        ],
        "flexible_data": {
            "_cls": classes["Label"].__name__,
            "name": "flexible",
            "color": "#00ff00",
        },
        # Dynamic
        "extra": {"nested": {"key": "value", "num": 42}, "list": [1, "two", 3.0]},
        # Enum (stores raw value)
        "priority": "high",
        # Geo (legacy)
        "pin": [40.7128, -74.0060],
        # Geo (GeoJSON — stored as full GeoJSON objects)
        "location": {"type": "Point", "coordinates": [-74.0060, 40.7128]},
        "route": {
            "type": "LineString",
            "coordinates": [[-74.0060, 40.7128], [-73.9857, 40.7484], [-73.9712, 40.7831]],
        },
        "boundary": {
            "type": "Polygon",
            "coordinates": [[
                [-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8],
                [-74.0, 40.8], [-74.0, 40.7],
            ]],
        },
        "waypoints": {
            "type": "MultiPoint",
            "coordinates": [[-74.0060, 40.7128], [-73.9857, 40.7484], [-73.9550, 40.7690]],
        },
        "trails": {
            "type": "MultiLineString",
            "coordinates": [
                [[-74.0, 40.7], [-73.95, 40.75], [-73.9, 40.8]],
                [[-73.8, 40.7], [-73.75, 40.75], [-73.7, 40.8]],
            ],
        },
        "zones": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74.0, 40.8], [-74.0, 40.7]]],
                [[[-73.8, 40.7], [-73.7, 40.7], [-73.7, 40.8], [-73.8, 40.8], [-73.8, 40.7]]],
            ],
        },
    }


def deep_nesting_kwargs(classes: dict[str, type]) -> dict[str, Any]:
    Metric = classes["Metric"]
    Sensor = classes["Sensor"]
    Device = classes["Device"]
    Room = classes["Room"]
    Floor = classes["Floor"]

    return {
        "name": "HQ Building",
        "address": "100 Tech Blvd, San Francisco, CA",
        "floors": [
            Floor(
                number=1, name="Ground Floor",
                rooms=[
                    Room(
                        name="Lobby", floor=1,
                        devices=[
                            Device(
                                name="Thermostat-1", model="Nest-v3", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="TEMP-001", type="temperature",
                                        metrics=[
                                            Metric(name="current", value=22.5, unit="celsius"),
                                            Metric(name="target", value=23.0, unit="celsius"),
                                        ],
                                        calibration={"offset": 0.1, "last_cal": "2024-01-15"},
                                    ),
                                    Sensor(
                                        sensor_id="HUM-001", type="humidity",
                                        metrics=[
                                            Metric(name="current", value=45.0, unit="percent"),
                                        ],
                                        calibration={"offset": 0.5},
                                    ),
                                ],
                                tags=["hvac", "lobby"],
                                properties={"zone": "A", "priority": "high"},
                            ),
                            Device(
                                name="Camera-1", model="Ring-Pro", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="MOT-001", type="motion",
                                        metrics=[
                                            Metric(name="sensitivity", value=0.8, unit="ratio"),
                                        ],
                                        calibration={},
                                    ),
                                ],
                                tags=["security"],
                                properties={"resolution": "4k"},
                            ),
                        ],
                        environment={"lighting": "bright", "access": "public"},
                    ),
                    Room(
                        name="Conference A", floor=1,
                        devices=[
                            Device(
                                name="Display-1", model="Samsung-65", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="PWR-001", type="power",
                                        metrics=[
                                            Metric(name="watts", value=120.0, unit="W"),
                                            Metric(name="hours", value=2450.0, unit="h"),
                                        ],
                                        calibration={"accuracy": 0.99},
                                    ),
                                ],
                                tags=["av", "display"],
                                properties={"input": "hdmi"},
                            ),
                        ],
                        environment={"capacity": 12, "projector": True},
                    ),
                ],
                summary=Device(
                    name="Floor-Controller-1", model="Hub-v2", active=True,
                    sensors=[], tags=["controller"], properties={},
                ),
            ),
            Floor(
                number=2, name="Engineering",
                rooms=[
                    Room(
                        name="Open Space", floor=2,
                        devices=[
                            Device(
                                name="AirSensor-1", model="Awair-2", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="CO2-001", type="co2",
                                        metrics=[
                                            Metric(name="ppm", value=450.0, unit="ppm"),
                                        ],
                                        calibration={"baseline": 400},
                                    ),
                                    Sensor(
                                        sensor_id="VOC-001", type="voc",
                                        metrics=[
                                            Metric(name="ppb", value=150.0, unit="ppb"),
                                        ],
                                        calibration={},
                                    ),
                                ],
                                tags=["air-quality"],
                                properties={"coverage": "50sqm"},
                            ),
                        ],
                        environment={"desks": 40, "windows": True},
                    ),
                    Room(
                        name="Server Room", floor=2,
                        devices=[
                            Device(
                                name="TempMonitor-1", model="SensorPush", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="TEMP-002", type="temperature",
                                        metrics=[
                                            Metric(name="current", value=18.5, unit="celsius"),
                                            Metric(name="max_24h", value=19.2, unit="celsius"),
                                            Metric(name="min_24h", value=17.8, unit="celsius"),
                                        ],
                                        calibration={"offset": 0.05, "certified": True},
                                    ),
                                ],
                                tags=["critical", "monitoring"],
                                properties={"alert_threshold": "25"},
                            ),
                            Device(
                                name="UPS-1", model="APC-3000", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="PWR-002", type="power",
                                        metrics=[
                                            Metric(name="load", value=67.5, unit="percent"),
                                            Metric(name="runtime", value=45.0, unit="min"),
                                        ],
                                        calibration={},
                                    ),
                                ],
                                tags=["power", "critical"],
                                properties={"capacity": "3000VA"},
                            ),
                        ],
                        environment={"rack_count": 8, "cooling": "dedicated"},
                    ),
                ],
                summary=Device(
                    name="Floor-Controller-2", model="Hub-v2", active=True,
                    sensors=[
                        Sensor(
                            sensor_id="AGG-001", type="aggregate",
                            metrics=[
                                Metric(name="device_count", value=4.0, unit="count"),
                            ],
                            calibration={},
                        ),
                    ],
                    tags=["controller"],
                    properties={},
                ),
            ),
            Floor(
                number=3, name="Executive",
                rooms=[
                    Room(
                        name="Board Room", floor=3,
                        devices=[
                            Device(
                                name="AV-System-1", model="Crestron-X", active=True,
                                sensors=[
                                    Sensor(
                                        sensor_id="AV-001", type="av",
                                        metrics=[
                                            Metric(name="uptime", value=99.8, unit="percent"),
                                        ],
                                        calibration={"last_check": "2024-03-01"},
                                    ),
                                ],
                                tags=["av", "premium"],
                                properties={"screens": "3", "audio_zones": "4"},
                            ),
                        ],
                        environment={"capacity": 20, "video_conf": True},
                    ),
                ],
                summary=Device(
                    name="Floor-Controller-3", model="Hub-v2", active=True,
                    sensors=[], tags=["controller"], properties={},
                ),
            ),
        ],
        "main_entrance": Room(
            name="Main Entrance", floor=0,
            devices=[
                Device(
                    name="AccessCtrl-1", model="Kisi-Pro", active=True,
                    sensors=[
                        Sensor(
                            sensor_id="NFC-001", type="nfc",
                            metrics=[
                                Metric(name="reads_today", value=342.0, unit="count"),
                            ],
                            calibration={},
                        ),
                    ],
                    tags=["access", "security"],
                    properties={"type": "badge-reader"},
                ),
            ],
            environment={"doors": 2, "turnstiles": 3},
        ),
        "global_metrics": [
            Metric(name="total_devices", value=47.0, unit="count"),
            Metric(name="uptime", value=99.7, unit="percent"),
            Metric(name="alerts_today", value=3.0, unit="count"),
        ],
        "config": {
            "timezone": "US/Pacific",
            "alert_email": "ops@example.com",
            "maintenance_day": "Sunday",
        },
    }


def deep_nesting_son(classes: dict[str, type]) -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "name": "HQ Building",
        "address": "100 Tech Blvd, San Francisco, CA",
        "floors": [
            {
                "number": 1, "name": "Ground Floor",
                "rooms": [
                    {
                        "name": "Lobby", "floor": 1,
                        "devices": [
                            {
                                "name": "Thermostat-1", "model": "Nest-v3", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "TEMP-001", "type": "temperature",
                                        "metrics": [
                                            {"name": "current", "value": 22.5, "unit": "celsius"},
                                            {"name": "target", "value": 23.0, "unit": "celsius"},
                                        ],
                                        "calibration": {"offset": 0.1, "last_cal": "2024-01-15"},
                                    },
                                    {
                                        "sensor_id": "HUM-001", "type": "humidity",
                                        "metrics": [
                                            {"name": "current", "value": 45.0, "unit": "percent"},
                                        ],
                                        "calibration": {"offset": 0.5},
                                    },
                                ],
                                "tags": ["hvac", "lobby"],
                                "properties": {"zone": "A", "priority": "high"},
                            },
                            {
                                "name": "Camera-1", "model": "Ring-Pro", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "MOT-001", "type": "motion",
                                        "metrics": [
                                            {"name": "sensitivity", "value": 0.8, "unit": "ratio"},
                                        ],
                                        "calibration": {},
                                    },
                                ],
                                "tags": ["security"],
                                "properties": {"resolution": "4k"},
                            },
                        ],
                        "environment": {"lighting": "bright", "access": "public"},
                    },
                    {
                        "name": "Conference A", "floor": 1,
                        "devices": [
                            {
                                "name": "Display-1", "model": "Samsung-65", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "PWR-001", "type": "power",
                                        "metrics": [
                                            {"name": "watts", "value": 120.0, "unit": "W"},
                                            {"name": "hours", "value": 2450.0, "unit": "h"},
                                        ],
                                        "calibration": {"accuracy": 0.99},
                                    },
                                ],
                                "tags": ["av", "display"],
                                "properties": {"input": "hdmi"},
                            },
                        ],
                        "environment": {"capacity": 12, "projector": True},
                    },
                ],
                "summary": {
                    "name": "Floor-Controller-1", "model": "Hub-v2", "active": True,
                    "sensors": [], "tags": ["controller"], "properties": {},
                },
            },
            {
                "number": 2, "name": "Engineering",
                "rooms": [
                    {
                        "name": "Open Space", "floor": 2,
                        "devices": [
                            {
                                "name": "AirSensor-1", "model": "Awair-2", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "CO2-001", "type": "co2",
                                        "metrics": [
                                            {"name": "ppm", "value": 450.0, "unit": "ppm"},
                                        ],
                                        "calibration": {"baseline": 400},
                                    },
                                    {
                                        "sensor_id": "VOC-001", "type": "voc",
                                        "metrics": [
                                            {"name": "ppb", "value": 150.0, "unit": "ppb"},
                                        ],
                                        "calibration": {},
                                    },
                                ],
                                "tags": ["air-quality"],
                                "properties": {"coverage": "50sqm"},
                            },
                        ],
                        "environment": {"desks": 40, "windows": True},
                    },
                    {
                        "name": "Server Room", "floor": 2,
                        "devices": [
                            {
                                "name": "TempMonitor-1", "model": "SensorPush", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "TEMP-002", "type": "temperature",
                                        "metrics": [
                                            {"name": "current", "value": 18.5, "unit": "celsius"},
                                            {"name": "max_24h", "value": 19.2, "unit": "celsius"},
                                            {"name": "min_24h", "value": 17.8, "unit": "celsius"},
                                        ],
                                        "calibration": {"offset": 0.05, "certified": True},
                                    },
                                ],
                                "tags": ["critical", "monitoring"],
                                "properties": {"alert_threshold": "25"},
                            },
                            {
                                "name": "UPS-1", "model": "APC-3000", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "PWR-002", "type": "power",
                                        "metrics": [
                                            {"name": "load", "value": 67.5, "unit": "percent"},
                                            {"name": "runtime", "value": 45.0, "unit": "min"},
                                        ],
                                        "calibration": {},
                                    },
                                ],
                                "tags": ["power", "critical"],
                                "properties": {"capacity": "3000VA"},
                            },
                        ],
                        "environment": {"rack_count": 8, "cooling": "dedicated"},
                    },
                ],
                "summary": {
                    "name": "Floor-Controller-2", "model": "Hub-v2", "active": True,
                    "sensors": [
                        {
                            "sensor_id": "AGG-001", "type": "aggregate",
                            "metrics": [
                                {"name": "device_count", "value": 4.0, "unit": "count"},
                            ],
                            "calibration": {},
                        },
                    ],
                    "tags": ["controller"],
                    "properties": {},
                },
            },
            {
                "number": 3, "name": "Executive",
                "rooms": [
                    {
                        "name": "Board Room", "floor": 3,
                        "devices": [
                            {
                                "name": "AV-System-1", "model": "Crestron-X", "active": True,
                                "sensors": [
                                    {
                                        "sensor_id": "AV-001", "type": "av",
                                        "metrics": [
                                            {"name": "uptime", "value": 99.8, "unit": "percent"},
                                        ],
                                        "calibration": {"last_check": "2024-03-01"},
                                    },
                                ],
                                "tags": ["av", "premium"],
                                "properties": {"screens": "3", "audio_zones": "4"},
                            },
                        ],
                        "environment": {"capacity": 20, "video_conf": True},
                    },
                ],
                "summary": {
                    "name": "Floor-Controller-3", "model": "Hub-v2", "active": True,
                    "sensors": [], "tags": ["controller"], "properties": {},
                },
            },
        ],
        "main_entrance": {
            "name": "Main Entrance", "floor": 0,
            "devices": [
                {
                    "name": "AccessCtrl-1", "model": "Kisi-Pro", "active": True,
                    "sensors": [
                        {
                            "sensor_id": "NFC-001", "type": "nfc",
                            "metrics": [
                                {"name": "reads_today", "value": 342.0, "unit": "count"},
                            ],
                            "calibration": {},
                        },
                    ],
                    "tags": ["access", "security"],
                    "properties": {"type": "badge-reader"},
                },
            ],
            "environment": {"doors": 2, "turnstiles": 3},
        },
        "global_metrics": [
            {"name": "total_devices", "value": 47.0, "unit": "count"},
            {"name": "uptime", "value": 99.7, "unit": "percent"},
            {"name": "alerts_today", "value": 3.0, "unit": "count"},
        ],
        "config": {
            "timezone": "US/Pacific",
            "alert_email": "ops@example.com",
            "maintenance_day": "Sunday",
        },
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
    son_fn: Callable[[dict[str, type]], dict[str, Any]],
    n: int,
    repeat: int,
) -> dict[str, dict[str, float]]:
    """Run all operations for one scenario. Returns {op: {median, best, ops_sec}}."""
    classes = build_fn()
    doc_cls = classes[list(classes.keys())[-1]]  # last class is the top-level Document

    kwargs = kwargs_fn(classes)
    son = son_fn(classes)
    doc = doc_cls(**kwargs)
    json_str = doc.to_json(json_options=LEGACY_JSON_OPTIONS)
    bulk_sons = [son_fn(classes) for _ in range(BULK_SIZE)]

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
    *scenario_results: tuple[str, dict[str, dict[str, float]]],
) -> dict[str, Any]:
    """Flatten results into a JSON-serializable dict."""
    out: dict[str, Any] = {}
    for scenario, data in scenario_results:
        for op, metrics in data.items():
            out[f"{scenario}/{op}/median_ms"] = round(metrics["median"] * 1000, 3)
            out[f"{scenario}/{op}/best_ms"] = round(metrics["best"] * 1000, 3)
            out[f"{scenario}/{op}/ops_sec"] = round(metrics["ops_sec"], 1)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


ALL_SCENARIOS = [
    ("Simple", build_simple, simple_kwargs, simple_son, "Simple (6 fields, 1 embedded doc)"),
    ("Complex", build_complex, complex_kwargs, complex_son, "Complex (20 fields, 3-level nesting)"),
    ("AllFields", build_all_fields, all_fields_kwargs, all_fields_son, "All Fields (30 fields, every field type)"),
    ("DeepNesting", build_deep_nesting, deep_nesting_kwargs, deep_nesting_son, "Deep Nesting (5-level, 3 floors x rooms x devices x sensors x metrics)"),
]


def main(n: int = 1000, repeat: int = 5, json_output: str | None = None) -> None:
    print(f"Baseline benchmark — Python {sys.version.split()[0]}, n={n}, repeat={repeat}")

    all_results: list[tuple[str, dict[str, dict[str, float]]]] = []
    for name, build_fn, kwargs_fn, son_fn, label in ALL_SCENARIOS:
        results = run_scenario(name, build_fn, kwargs_fn, son_fn, n, repeat)
        print_scenario(label, results)
        all_results.append((name.lower().replace(" ", "_"), results))

    if json_output:
        data = results_to_dict(*all_results)
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
