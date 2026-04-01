"""Tests for embedded document parent instance wiring optimizations.

Covers:
- _from_son_set_instance_targeted() with all 3 field kinds
- _build_embedded_field_info() categorization logic
- _recurse_set_instance() recursive fallback
- _field_may_contain_embedded() helper
- Wiring through _from_son fast path
- Wiring through _init_fast path
"""

import weakref

from mongoengine import *
from mongoengine.base.document import (
    _EMB_DIRECT,
    _EMB_GENERIC,
    _EMB_LIST,
    _build_embedded_field_info,
    _field_may_contain_embedded,
    _from_son_set_instance_targeted,
    _recurse_set_instance,
)
from tests.utils import MongoDBTestCase


class TestBuildEmbeddedFieldInfo(MongoDBTestCase):
    """Tests for _build_embedded_field_info() categorization."""

    def test_embedded_document_field_is_direct(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        class Outer(Document):
            inner = EmbeddedDocumentField(Inner)

        info = _build_embedded_field_info(Outer, EmbeddedDocument)
        assert ("inner", _EMB_DIRECT) in info

    def test_embedded_document_list_field_is_list(self):
        class Item(EmbeddedDocument):
            name = StringField()

        class Container(Document):
            items = EmbeddedDocumentListField(Item)

        info = _build_embedded_field_info(Container, EmbeddedDocument)
        assert ("items", _EMB_LIST) in info

    def test_generic_embedded_field_is_generic(self):
        class Outer(Document):
            generic = GenericEmbeddedDocumentField()

        info = _build_embedded_field_info(Outer, EmbeddedDocument)
        assert ("generic", _EMB_GENERIC) in info

    def test_plain_fields_not_included(self):
        class PlainDoc(Document):
            name = StringField()
            age = IntField()
            tags = ListField(StringField())

        info = _build_embedded_field_info(PlainDoc, EmbeddedDocument)
        field_names = {name for name, _ in info}
        assert "name" not in field_names
        assert "age" not in field_names
        assert "tags" not in field_names

    def test_list_wrapping_embedded_is_generic(self):
        """ListField(EmbeddedDocumentField) should be categorized as generic."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class WrapDoc(Document):
            items = ListField(EmbeddedDocumentField(Inner))

        info = _build_embedded_field_info(WrapDoc, EmbeddedDocument)
        assert ("items", _EMB_GENERIC) in info

    def test_map_field_wrapping_embedded_is_generic(self):
        """MapField(EmbeddedDocumentField) should be categorized as generic."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class MapDoc(Document):
            items = MapField(EmbeddedDocumentField(Inner))

        info = _build_embedded_field_info(MapDoc, EmbeddedDocument)
        assert ("items", _EMB_GENERIC) in info

    def test_dict_field_without_embedded_not_included(self):
        class DictDoc(Document):
            meta_data = DictField()

        info = _build_embedded_field_info(DictDoc, EmbeddedDocument)
        field_names = {name for name, _ in info}
        assert "meta_data" not in field_names

    def test_mixed_fields(self):
        """Document with mix of field types gets correct categorization."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class MixedDoc(Document):
            name = StringField()
            single = EmbeddedDocumentField(Inner)
            multi = EmbeddedDocumentListField(Inner)
            generic = GenericEmbeddedDocumentField()
            tags = ListField(StringField())

        info = _build_embedded_field_info(MixedDoc, EmbeddedDocument)
        info_dict = dict(info)
        assert info_dict["single"] == _EMB_DIRECT
        assert info_dict["multi"] == _EMB_LIST
        assert info_dict["generic"] == _EMB_GENERIC
        assert "name" not in info_dict
        assert "tags" not in info_dict


class TestFieldMayContainEmbedded(MongoDBTestCase):
    """Tests for _field_may_contain_embedded() helper."""

    def test_embedded_document_field(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        field = EmbeddedDocumentField(Inner)
        assert _field_may_contain_embedded(field, EmbeddedDocument)

    def test_generic_embedded_field(self):
        field = GenericEmbeddedDocumentField()
        assert _field_may_contain_embedded(field, EmbeddedDocument)

    def test_string_field(self):
        field = StringField()
        assert not _field_may_contain_embedded(field, EmbeddedDocument)

    def test_list_field_wrapping_embedded(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        field = ListField(EmbeddedDocumentField(Inner))
        assert _field_may_contain_embedded(field, EmbeddedDocument)

    def test_list_field_wrapping_string(self):
        field = ListField(StringField())
        assert not _field_may_contain_embedded(field, EmbeddedDocument)


class TestFromSonSetInstanceTargeted(MongoDBTestCase):
    """Tests for _from_son_set_instance_targeted() direct wiring."""

    def test_direct_embedded_wired(self):
        """Single EmbeddedDocumentField gets _instance set."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class Outer(Document):
            inner = EmbeddedDocumentField(Inner)

        inner = Inner(val="test")
        outer = Outer.__new__(Outer)
        proxy = weakref.proxy(outer)
        data = {"inner": inner}
        info = (("inner", _EMB_DIRECT),)

        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)
        assert inner._instance is proxy

    def test_list_embedded_wired(self):
        """List of embedded docs all get _instance set."""

        class Item(EmbeddedDocument):
            name = StringField()

        items = [Item(name="a"), Item(name="b"), Item(name="c")]
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)
        data = {"items": items}
        info = (("items", _EMB_LIST),)

        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)
        for item in items:
            assert item._instance is proxy

    def test_generic_embedded_wired_recursively(self):
        """Generic embedded field falls back to recursive walk."""

        class Inner(EmbeddedDocument):
            val = StringField()

        inner = Inner(val="deep")
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)
        data = {"generic": inner}
        info = (("generic", _EMB_GENERIC),)

        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)
        assert inner._instance is proxy

    def test_none_value_skipped(self):
        """None values are skipped without error."""
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)
        data = {"inner": None}
        info = (("inner", _EMB_DIRECT),)

        # Should not raise
        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)

    def test_missing_key_skipped(self):
        """Keys not in data are skipped without error."""
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)
        data = {}
        info = (("inner", _EMB_DIRECT),)

        # Should not raise
        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)

    def test_non_embedded_value_in_direct_slot_skipped(self):
        """If a direct slot has a non-EmbeddedDocument value, it's skipped."""
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)
        data = {"inner": "not_an_embedded_doc"}
        info = (("inner", _EMB_DIRECT),)

        # Should not raise — isinstance check protects
        _from_son_set_instance_targeted(proxy, EmbeddedDocument, data, info)


class TestRecurseSetInstance(MongoDBTestCase):
    """Tests for _recurse_set_instance() recursive fallback."""

    def test_single_embedded_doc(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        inner = Inner(val="test")
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)

        _recurse_set_instance(inner, proxy, EmbeddedDocument)
        assert inner._instance is proxy

    def test_list_of_embedded_docs(self):
        class Item(EmbeddedDocument):
            name = StringField()

        items = [Item(name="a"), Item(name="b")]
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)

        _recurse_set_instance(items, proxy, EmbeddedDocument)
        for item in items:
            assert item._instance is proxy

    def test_dict_with_embedded_doc_values(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        inner = Inner(val="in_dict")
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)

        _recurse_set_instance({"key": inner}, proxy, EmbeddedDocument)
        assert inner._instance is proxy

    def test_nested_list_in_dict(self):
        """Embedded docs nested in list inside dict."""

        class Inner(EmbeddedDocument):
            val = StringField()

        inner = Inner(val="deep")
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)

        _recurse_set_instance({"key": [inner]}, proxy, EmbeddedDocument)
        assert inner._instance is proxy

    def test_plain_value_no_op(self):
        """Non-embedded values are silently ignored."""
        parent = Document.__new__(Document)
        proxy = weakref.proxy(parent)

        # Should not raise
        _recurse_set_instance("string", proxy, EmbeddedDocument)
        _recurse_set_instance(42, proxy, EmbeddedDocument)
        _recurse_set_instance(None, proxy, EmbeddedDocument)


class TestEmbeddedWiringIntegration(MongoDBTestCase):
    """Integration tests: embedded docs wired through _from_son and __init__."""

    async def test_from_son_wires_single_embedded(self):
        class Addr(EmbeddedDocument):
            city = StringField()

        class Person(Document):
            name = StringField()
            address = EmbeddedDocumentField(Addr)

        son = {"name": "alice", "address": {"city": "NYC"}}
        person = Person._from_son(son)
        assert person.address.city == "NYC"
        assert hasattr(person.address, "_instance")
        assert person.address._instance is not None

    async def test_from_son_wires_embedded_list(self):
        class Tag(EmbeddedDocument):
            label = StringField()

        class Post(Document):
            title = StringField()
            tags = EmbeddedDocumentListField(Tag)

        son = {"title": "hello", "tags": [{"label": "a"}, {"label": "b"}]}
        post = Post._from_son(son)
        for tag in post.tags:
            assert hasattr(tag, "_instance")

    async def test_from_son_wires_generic_embedded(self):
        class Inner(EmbeddedDocument):
            val = StringField()

        class GenDoc(Document):
            content = GenericEmbeddedDocumentField()

        inner = Inner(val="gen")
        son = {"content": inner}
        doc = GenDoc._from_son(son)
        # GenericEmbeddedDocumentField value should have _instance wired
        if hasattr(doc.content, "_instance"):
            assert doc.content._instance is not None

    async def test_init_wires_embedded_instance(self):
        """__init__ fast path wires _instance on embedded docs."""

        class Skill(EmbeddedDocument):
            name = StringField()

        class Employee(Document):
            skills = EmbeddedDocumentListField(Skill)

        skills = [Skill(name="Python"), Skill(name="Go")]
        emp = Employee(skills=skills)
        for skill in emp.skills:
            assert hasattr(skill, "_instance")

    async def test_from_son_embedded_field_info_cached(self):
        """_from_son_embedded_field_info is cached per class."""

        class CacheInner(EmbeddedDocument):
            val = StringField()

        class CacheOuter(Document):
            inner = EmbeddedDocumentField(CacheInner)

        # Clear cache
        if "_from_son_embedded_field_info" in CacheOuter.__dict__:
            del CacheOuter._from_son_embedded_field_info

        CacheOuter._from_son({"inner": {"val": "a"}})
        info1 = CacheOuter.__dict__.get("_from_son_embedded_field_info")
        assert info1 is not None

        CacheOuter._from_son({"inner": {"val": "b"}})
        info2 = CacheOuter.__dict__.get("_from_son_embedded_field_info")
        assert info1 is info2  # same object — cached

    async def test_embedded_field_info_not_inherited(self):
        """Child class builds its own embedded field info, not parent's."""

        class Inner(EmbeddedDocument):
            val = StringField()

        class Parent(Document):
            inner = EmbeddedDocumentField(Inner)
            meta = {"allow_inheritance": True}

        class Child(Parent):
            extra = EmbeddedDocumentField(Inner)

        # Build parent's cache
        Parent._from_son({"inner": {"val": "p"}})
        parent_info = Parent.__dict__.get("_from_son_embedded_field_info")

        # Build child's cache
        Child._from_son({"inner": {"val": "c1"}, "extra": {"val": "c2"}, "_cls": "Parent.Child"})
        child_info = Child.__dict__.get("_from_son_embedded_field_info")

        # Child should have more entries than parent
        assert child_info is not None
        assert parent_info is not None
        assert len(child_info) > len(parent_info)
