"""Tests for _generate_async_fields() — pre-validation async field generation.

Covers:
- SequenceField value generation at top level
- SequenceField in embedded documents (EmbeddedDocumentField)
- SequenceField in embedded document lists (EmbeddedDocumentListField)
- SequenceField in ListField(EmbeddedDocumentField)
- Skipping when value is already set
- No-op when no SequenceField exists
"""

from mongoengine import *
from mongoengine.document import _generate_async_fields
from tests.utils import MongoDBTestCase


class TestGenerateAsyncFields(MongoDBTestCase):
    async def test_top_level_sequence_field_generated(self):
        """SequenceField at document root gets a value generated."""

        class SeqDoc(Document):
            seq = SequenceField()
            name = StringField()

        await SeqDoc.drop_collection()

        doc = SeqDoc(name="test")
        assert doc.seq is None
        await _generate_async_fields(doc)
        assert doc.seq is not None
        assert isinstance(doc.seq, int)

    async def test_sequence_field_not_overwritten_if_set(self):
        """SequenceField is skipped if value already provided."""

        class SeqDoc2(Document):
            seq = SequenceField()
            name = StringField()

        doc = SeqDoc2(name="test")
        doc._data["seq"] = 42
        await _generate_async_fields(doc)
        assert doc._data["seq"] == 42

    async def test_sequence_in_embedded_document(self):
        """SequenceField in EmbeddedDocumentField gets generated."""

        class InnerSeq(EmbeddedDocument):
            seq = SequenceField()
            label = StringField()

        class OuterSeq(Document):
            inner = EmbeddedDocumentField(InnerSeq)
            name = StringField()

        await OuterSeq.drop_collection()

        inner = InnerSeq(label="test")
        doc = OuterSeq(name="outer", inner=inner)
        assert inner._data.get("seq") is None
        await _generate_async_fields(doc)
        assert inner._data.get("seq") is not None

    async def test_sequence_in_embedded_list(self):
        """SequenceField in EmbeddedDocumentListField items gets generated."""

        class ListInner(EmbeddedDocument):
            seq = SequenceField()
            val = StringField()

        class ListOuter(Document):
            items = EmbeddedDocumentListField(ListInner)

        await ListOuter.drop_collection()

        items = [ListInner(val="a"), ListInner(val="b")]
        doc = ListOuter(items=items)
        await _generate_async_fields(doc)
        for item in items:
            assert item._data.get("seq") is not None

    async def test_sequence_in_list_of_embedded(self):
        """SequenceField in ListField(EmbeddedDocumentField) gets generated."""

        class WrapInner(EmbeddedDocument):
            seq = SequenceField()
            val = StringField()

        class WrapOuter(Document):
            items = ListField(EmbeddedDocumentField(WrapInner))

        await WrapOuter.drop_collection()

        items = [WrapInner(val="x"), WrapInner(val="y")]
        doc = WrapOuter(items=items)
        await _generate_async_fields(doc)
        for item in items:
            assert item._data.get("seq") is not None

    async def test_no_sequence_field_is_noop(self):
        """Documents without SequenceField should pass through unchanged."""

        class PlainDoc(Document):
            name = StringField()
            age = IntField()

        doc = PlainDoc(name="test", age=25)
        await _generate_async_fields(doc)
        assert doc.name == "test"
        assert doc.age == 25

    async def test_none_embedded_skipped(self):
        """None embedded document value is safely skipped."""

        class InnerOpt(EmbeddedDocument):
            seq = SequenceField()

        class OuterOpt(Document):
            inner = EmbeddedDocumentField(InnerOpt)

        doc = OuterOpt()
        assert doc.inner is None
        await _generate_async_fields(doc)  # should not raise

    async def test_sequence_values_increment(self):
        """Multiple calls generate incrementing sequence values."""

        class IncDoc(Document):
            seq = SequenceField()

        await IncDoc.drop_collection()

        doc1 = IncDoc()
        await _generate_async_fields(doc1)
        doc2 = IncDoc()
        await _generate_async_fields(doc2)
        assert doc2._data["seq"] > doc1._data["seq"]
