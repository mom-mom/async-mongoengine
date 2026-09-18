"""Runtime behaviour of the generic ``Document[PK]`` and field descriptor classes.

The type parameters exist for static typing only (see ``docs/typing.md``).
These tests pin down that adding ``Generic`` to the MRO changed nothing at
runtime: metaclass field collection, custom primary keys, pickling,
``isinstance`` checks, equality / hashing and field metadata handling.
"""

import inspect
import pickle
import typing

import pytest
from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine.base import (
    BaseDocument,
    BaseField,
    BaseList,
    EmbeddedDocumentList,
    LazyReference,
    TopLevelDocumentMetaclass,
)
from tests.utils import MongoDBTestCase


class PickleCoded(Document[str]):
    """Module-level so that pickle can import it by qualified name."""

    code = StringField(primary_key=True)
    tags = ListField(StringField())


class PickleEmbeddedAddress(EmbeddedDocument):
    city = StringField()


class PickleWithEmbedded(DynamicDocument):
    name = StringField()
    addresses = EmbeddedDocumentListField(PickleEmbeddedAddress)


class TestGenericDocumentRuntime(MongoDBTestCase):
    async def test_custom_pk_document_saves_and_loads(self):
        class Coded(Document[str]):
            code = StringField(primary_key=True)
            name = StringField()

        await Coded.drop_collection()
        doc = await Coded(code="k1", name="first").save()
        assert doc.pk == "k1"
        assert doc.id == "k1"

        loaded = await Coded.objects.get(code="k1")
        assert loaded.id == "k1"
        assert loaded.name == "first"
        assert loaded == doc
        assert await Coded.objects.count() == 1

        loaded.name = "second"
        await loaded.save()
        assert (await Coded.objects.get(pk="k1")).name == "second"

    async def test_custom_pk_dynamic_document(self):
        class Dyn(DynamicDocument[int]):
            id = IntField(primary_key=True)

        await Dyn.drop_collection()
        doc = await Dyn(id=7, extra="x").save()
        loaded = await Dyn.objects.get(id=7)
        assert loaded.pk == 7
        assert loaded.extra == "x"
        assert loaded == doc

    def test_type_parameters_do_not_leak_into_fields(self):
        class Item(Document):
            name = StringField()

            meta = {"allow_inheritance": True}

        class Coded(Document[str]):
            code = StringField(primary_key=True)

        class Dyn(DynamicDocument):
            pass

        class Sub(Item):
            pass

        assert Document.__type_params__[0].__name__ == "PK"
        assert DynamicDocument.__type_params__[0].__name__ == "PK"
        for cls in (Item, Coded, Dyn, Sub):
            assert cls.__type_params__ == ()
            for internal in ("__type_params__", "__orig_bases__", "__parameters__"):
                assert internal not in cls._fields
                assert internal not in cls._fields_ordered
                assert internal not in cls._db_field_map
        assert set(Item._fields) == {"_cls", "id", "name"}
        assert set(Sub._fields) == {"_cls", "id", "name"}
        assert set(Coded._fields) == {"code"}
        assert set(Dyn._fields) == {"id"}
        assert Coded.__orig_bases__ == (Document[str],)

    def test_mro_and_isinstance_checks(self):
        class Coded(Document[str]):
            code = StringField(primary_key=True)

        class Dyn(DynamicDocument):
            pass

        class Address(EmbeddedDocument):
            city = StringField()

        assert typing.Generic in Document.__mro__
        assert Document.__mro__.index(BaseDocument) < Document.__mro__.index(typing.Generic)
        assert type(Coded) is TopLevelDocumentMetaclass
        assert type(Dyn) is TopLevelDocumentMetaclass
        assert isinstance(Coded(code="a"), Document)
        assert isinstance(Coded(code="a"), BaseDocument)
        assert isinstance(Dyn(), Document)
        assert issubclass(Coded, Document)
        assert issubclass(DynamicDocument, Document)
        assert not isinstance(Address(), Document)
        assert not hasattr(Address(), "id")
        assert not hasattr(Address(), "pk")

    def test_generic_alias_subclassing_keeps_id_alias(self):
        class Coded(Document[str]):
            code = StringField(primary_key=True)

        class Auto(Document[ObjectId]):
            name = StringField()

        # ``id`` is aliased to the custom primary-key field ...
        assert Coded.id is Coded._fields["code"]
        assert Coded(code="k").id == "k"
        assert Coded(code="k").pk == "k"
        # ... and still auto-injected otherwise.
        assert "id" in Auto._fields
        assert Auto().id is None
        assert Auto().pk is None

    async def test_pickling(self):
        await PickleCoded.drop_collection()
        doc = PickleCoded(code="p", tags=["a"])
        # Pickling works before the document is saved.
        restored = pickle.loads(pickle.dumps(doc))
        assert restored.code == "p"
        assert restored.tags == ["a"]

        await doc.save()
        restored = pickle.loads(pickle.dumps(doc))
        assert restored == doc
        assert restored.pk == "p"
        assert isinstance(restored.tags, BaseList)
        restored.tags.append("b")
        await restored.save()
        assert (await PickleCoded.objects.get(pk="p")).tags == ["a", "b"]

        await PickleWithEmbedded.drop_collection()
        with_embedded = PickleWithEmbedded(name="n", addresses=[PickleEmbeddedAddress(city="c")], extra=1)
        await with_embedded.save()
        restored = pickle.loads(pickle.dumps(with_embedded))
        assert restored == with_embedded
        assert isinstance(restored.addresses, EmbeddedDocumentList)
        assert restored.addresses.get(city="c").city == "c"
        assert restored.extra == 1

    def test_equality_and_hashing_unchanged(self):
        class Item(Document):
            name = StringField()

        class Address(EmbeddedDocument):
            city = StringField()

        unsaved_a, unsaved_b = Item(), Item()
        assert unsaved_a == unsaved_a
        assert unsaved_a != unsaved_b
        assert unsaved_a != "not a document"
        oid = ObjectId()
        assert Item(id=oid) == Item(id=oid)
        assert Item(id=oid) != Item(id=ObjectId())
        assert Item(id=oid) == DBRef("item", oid)
        assert Item(id=oid) != DBRef("other", oid)
        assert hash(Item(id=oid)) == hash(oid)
        assert isinstance(hash(unsaved_a), int)

        assert Address(city="x") == Address(city="x")
        assert Address(city="x") != Address(city="y")
        with pytest.raises(TypeError):
            hash(Address(city="x"))
        # The comparison now lives next to the attributes it uses.
        assert "__eq__" not in BaseDocument.__dict__
        assert "__eq__" in Document.__dict__
        assert "__eq__" in EmbeddedDocument.__dict__

    def test_field_metadata_kwargs_and_conflicts(self):
        field = StringField(verbose_name="Name", help_text="Full name")
        assert field.verbose_name == "Name"
        assert field.help_text == "Full name"

        # Real attribute conflicts still raise, exactly as before.
        with pytest.raises(TypeError, match="already has attribute"):
            StringField(name="x")
        with pytest.raises(TypeError, match="already has attribute"):
            IntField(to_python=lambda v: v)
        with pytest.raises(TypeError, match="already has attribute"):
            BooleanField(db_field="ok", required_=True, validate=None)

    def test_runtime_constructor_signatures_unchanged(self):
        # Classes whose overloads are declared under TYPE_CHECKING keep the
        # inherited runtime __init__ (and its positional parameters).
        assert BooleanField.__init__ is BaseField.__init__
        assert DateTimeField.__init__ is BaseField.__init__
        assert PointField.__init__ is GeoJsonBaseField.__init__
        assert "db_field" in inspect.signature(BooleanField).parameters
        positional = BooleanField("flag_db", True)
        assert positional.db_field == "flag_db"
        assert positional.required is True

        # Adding ``Generic`` is the only MRO change for fields.
        assert typing.Generic in BaseField.__mro__
        assert [c.__name__ for c in StringField.__mro__] == ["StringField", "BaseField", "Generic", "object"]

    def test_generic_datastructures(self):
        class Address(EmbeddedDocument):
            city = StringField()

        class Item(Document):
            tags = ListField(StringField())
            addresses = EmbeddedDocumentListField(Address)
            owner = LazyReferenceField("Item")

        item = Item(tags=["a"], addresses=[Address(city="x"), Address(city="y")])
        assert isinstance(item.tags, BaseList)
        assert isinstance(item.addresses, EmbeddedDocumentList)
        assert item.addresses.filter(city="x").count() == 1
        assert item.addresses.first().city == "x"
        assert item.addresses[-1].city == "y"

        ref = LazyReference(Item, ObjectId())
        assert ref.document_type is Item
        assert isinstance(ref, DBRef)
        assert typing.Generic in BaseList.__mro__
        assert typing.Generic in LazyReference.__mro__
