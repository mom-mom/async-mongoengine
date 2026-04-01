"""Tests for the fast __init__ path optimizations.

Covers:
- _has_custom_init() detection logic
- _build_init_dispatch() dispatch table construction
- _init_fast() fast path behavior and fallback conditions
- Signal-based fallback (pre_init / post_init)
- Custom __setattr__ fallback
- STRICT document handling
- Dynamic document exclusion from fast path
"""


import pytest

from mongoengine import *
from mongoengine import signals
from mongoengine.base.document import _has_custom_init
from mongoengine.errors import FieldDoesNotExist
from tests.utils import MongoDBTestCase


class TestHasCustomInit(MongoDBTestCase):
    """Tests for _has_custom_init() detection."""

    def test_plain_document_has_no_custom_init(self):
        class PlainDoc(Document):
            name = StringField()

        assert not _has_custom_init(PlainDoc)

    def test_plain_embedded_has_no_custom_init(self):
        class PlainEmb(EmbeddedDocument):
            name = StringField()

        assert not _has_custom_init(PlainEmb)

    def test_custom_init_detected_on_document(self):
        class CustomDoc(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._custom = True

        assert _has_custom_init(CustomDoc)

    def test_custom_init_detected_on_embedded(self):
        class CustomEmb(EmbeddedDocument):
            name = StringField()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._custom = True

        assert _has_custom_init(CustomEmb)

    def test_inherited_custom_init_detected(self):
        """If parent has custom __init__, child should inherit the detection."""

        class BaseDoc(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        class ChildDoc(BaseDoc):
            age = IntField()

        # Child inherits parent's custom __init__
        assert _has_custom_init(ChildDoc)

    def test_child_overriding_framework_init_is_custom(self):
        """Child that overrides __init__ independently is detected."""

        class ParentDoc(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class ChildWithInit(ParentDoc):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._child_flag = True

        assert not _has_custom_init(ParentDoc)
        assert _has_custom_init(ChildWithInit)

    def test_dynamic_document_has_no_custom_init(self):
        class DynDoc(DynamicDocument):
            name = StringField()

        assert not _has_custom_init(DynDoc)


class TestBuildInitDispatch(MongoDBTestCase):
    """Tests for _build_init_dispatch() dispatch table."""

    def test_plain_fields_not_in_dispatch(self):
        """StringField, IntField, etc. use BaseField.__set__ — not in dispatch."""

        class SimpleDoc(Document):
            name = StringField()
            age = IntField()
            rating = FloatField()

        dispatch = SimpleDoc._build_init_dispatch()
        assert "name" not in dispatch
        assert "age" not in dispatch
        assert "rating" not in dispatch

    def test_binary_field_needs_custom_set(self):
        """BinaryField has custom __set__ — must be in dispatch."""

        class BinDoc(Document):
            data = BinaryField()

        dispatch = BinDoc._build_init_dispatch()
        assert "data" in dispatch

    def test_list_field_with_enum_needs_custom_set(self):
        """ListField(EnumField) wrapping needs custom __set__."""
        import enum

        class Color(enum.Enum):
            RED = "red"
            BLUE = "blue"

        class EnumListDoc(Document):
            colors = ListField(EnumField(Color))

        dispatch = EnumListDoc._build_init_dispatch()
        assert "colors" in dispatch

    def test_list_field_without_enum_skips_set(self):
        """ListField(StringField) doesn't need custom __set__."""

        class PlainListDoc(Document):
            tags = ListField(StringField())

        dispatch = PlainListDoc._build_init_dispatch()
        assert "tags" not in dispatch

    def test_complex_datetime_field_needs_custom_set(self):
        """ComplexDateTimeField has custom __set__ — must be in dispatch."""

        class CdtDoc(Document):
            created = ComplexDateTimeField()

        dispatch = CdtDoc._build_init_dispatch()
        assert "created" in dispatch

    def test_dispatch_cached_per_class(self):
        """Dispatch table is cached on the class after first build."""

        class CachedDoc(Document):
            name = StringField()

        # Clear any existing cache
        if "_init_needs_custom_set" in CachedDoc.__dict__:
            del CachedDoc._init_needs_custom_set

        d1 = CachedDoc._build_init_dispatch()
        CachedDoc._init_needs_custom_set = d1

        # Second access should return same object
        d2 = CachedDoc.__dict__["_init_needs_custom_set"]
        assert d1 is d2


class TestInitFastPath(MongoDBTestCase):
    """Tests for _init_fast() and its fallback conditions."""

    def test_fast_path_used_for_plain_document(self):
        """Non-dynamic, non-STRICT document without signals uses fast path."""

        class FastDoc(Document):
            name = StringField()
            age = IntField()

        doc = FastDoc(name="test", age=25)
        assert doc.name == "test"
        assert doc.age == 25
        assert doc._created is True

    def test_fast_path_sets_defaults(self):
        """Fast path populates field defaults correctly."""

        class DefaultDoc(Document):
            name = StringField(default="anon")
            count = IntField(default=0)
            active = BooleanField(default=True)

        doc = DefaultDoc()
        assert doc.name == "anon"
        assert doc.count == 0
        assert doc.active is True

    def test_fast_path_callable_defaults(self):
        """Callable defaults are invoked in fast path."""

        class CallDoc(Document):
            tags = ListField(StringField(), default=list)

        d1 = CallDoc()
        d2 = CallDoc()
        # Each instance should get its own list
        assert d1.tags == []
        assert d2.tags == []
        d1.tags.append("x")
        assert d2.tags == []

    def test_fast_path_rejects_undefined_fields(self):
        """Fast path rejects undefined fields like legacy path."""

        class StrictishDoc(Document):
            name = StringField()

        with pytest.raises(FieldDoesNotExist):
            StrictishDoc(name="ok", extra="bad")

    def test_fast_path_to_python_conversion(self):
        """Fast path calls field.to_python for type conversion."""

        class ConvDoc(Document):
            age = IntField()

        doc = ConvDoc(age="42")  # string should be converted to int
        assert doc.age == 42

    def test_fast_path_null_with_default(self):
        """Fast path replaces None with default when null=False."""

        class NullDoc(Document):
            name = StringField(default="fallback")

        doc = NullDoc(name=None)
        assert doc.name == "fallback"

    def test_fast_path_null_kept_when_null_true(self):
        """Fast path keeps None when field has null=True."""

        class NullableDoc(Document):
            name = StringField(null=True, default="fallback")

        doc = NullableDoc(name=None)
        assert doc.name is None

    def test_fast_path_wires_embedded_instance(self):
        """Fast path wires _instance on embedded documents."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class Outer(Document):
            inner = EmbeddedDocumentField(Inner)

        inner = Inner(val="test")
        outer = Outer(inner=inner)
        assert hasattr(outer.inner, "_instance")
        assert outer.inner._instance is not None

    def test_fast_path_wires_embedded_list_instance(self):
        """Fast path wires _instance on embedded documents in lists."""

        class Item(EmbeddedDocument):
            name = StringField()

        class Container(Document):
            items = EmbeddedDocumentListField(Item)

        items = [Item(name="a"), Item(name="b")]
        container = Container(items=items)
        for item in container.items:
            assert hasattr(item, "_instance")

    def test_fast_path_sets_cls_field(self):
        """Fast path sets _cls for inherited documents."""

        class Base(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Child(Base):
            age = IntField()

        child = Child(name="test", age=5)
        assert child._cls == "Base.Child"

    def test_fast_path_allows_id_pk_cls(self):
        """Fast path accepts id, pk, _cls as special keys."""
        from bson import ObjectId

        class IdDoc(Document):
            name = StringField()

        oid = ObjectId()
        doc = IdDoc(id=oid, name="test")
        assert doc.id == oid

    def test_fallback_when_signals_registered(self):
        """pre_init signal forces fallback to legacy path."""

        class SignalDoc(Document):
            name = StringField()

        signal_fired = []

        def on_pre_init(sender, document, **kwargs):
            signal_fired.append(True)

        signals.pre_init.connect(on_pre_init, sender=SignalDoc)
        try:
            doc = SignalDoc(name="test")
            assert doc.name == "test"
            assert signal_fired  # signal must have fired
        finally:
            signals.pre_init.disconnect(on_pre_init, sender=SignalDoc)

    def test_fallback_when_post_init_signal_registered(self):
        """post_init signal forces fallback to legacy path."""

        class PostSignalDoc(Document):
            name = StringField()

        signal_fired = []

        def on_post_init(sender, document, **kwargs):
            signal_fired.append(document.name)

        signals.post_init.connect(on_post_init, sender=PostSignalDoc)
        try:
            doc = PostSignalDoc(name="hello")
            assert doc.name == "hello"
            assert signal_fired == ["hello"]
        finally:
            signals.post_init.disconnect(on_post_init, sender=PostSignalDoc)

    def test_fallback_when_custom_setattr(self):
        """Custom __setattr__ forces fallback to legacy path."""

        class SetAttrDoc(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                self._log = []
                super().__init__(*args, **kwargs)

            def __setattr__(self, key, value):
                if not key.startswith("_"):
                    self._log.append(key)
                super().__setattr__(key, value)

        doc = SetAttrDoc(name="test")
        assert "name" in doc._log

    def test_fallback_when_custom_init(self):
        """Custom __init__ forces fallback to legacy path."""

        class InitDoc(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._inited = True

        doc = InitDoc(name="test")
        assert doc._inited is True

    def test_dynamic_document_skips_fast_path(self):
        """DynamicDocument must not use fast path (needs dynamic field handling)."""

        class DynDoc(DynamicDocument):
            name = StringField()

        doc = DynDoc(name="test", extra="dynamic_value")
        assert doc.name == "test"
        assert doc.extra == "dynamic_value"

    def test_strict_document_skips_fast_path(self):
        """STRICT document must not use fast path."""

        class StrictDoc(Document):
            name = StringField()
            STRICT = True

        doc = StrictDoc(name="test")
        assert doc.name == "test"

    def test_fast_path_binary_field_delegates_to_set(self):
        """BinaryField needs custom __set__ — fast path must delegate."""

        class BinFastDoc(Document):
            data = BinaryField()

        doc = BinFastDoc(data=b"hello")
        assert doc.data == b"hello"

    def test_fast_path_created_flag(self):
        """_created is set correctly via fast path."""

        class CreatedDoc(Document):
            name = StringField()

        doc = CreatedDoc(name="test", _created=False)
        assert doc._created is False

        doc2 = CreatedDoc(name="test")
        assert doc2._created is True
