import pickle

import pytest
from bson import ObjectId

from mongoengine import *
from mongoengine.errors import FieldDoesNotExist
from tests import fixtures
from tests.fixtures import (
    PickleDynamicEmbedded,
    PickleDynamicTest,
    PickleEmbedded,
    PickleSignalsTest,
    PickleTest,
)
from tests.utils import MongoDBTestCase


class TestInstanceSerialization(MongoDBTestCase):
    async def test_document_hash(self):
        """Test document in list, dict, set."""

        class User(Document):
            pass

        class BlogPost(Document):
            pass

        # Clear old data
        await User.drop_collection()
        await BlogPost.drop_collection()

        u1 = await User.objects.create()
        u2 = await User.objects.create()
        u3 = await User.objects.create()
        u4 = User()  # New object

        b1 = await BlogPost.objects.create()
        b2 = await BlogPost.objects.create()

        # Make sure docs are properly identified in a list (__eq__ is used
        # for the comparison).
        all_user_list = [doc async for doc in User.objects.all()]
        assert u1 in all_user_list
        assert u2 in all_user_list
        assert u3 in all_user_list
        assert u4 not in all_user_list  # New object
        assert b1 not in all_user_list  # Other object
        assert b2 not in all_user_list  # Other object

        # Make sure docs can be used as keys in a dict (__hash__ is used
        # for hashing the docs).
        all_user_dic = {}
        async for u in User.objects.all():
            all_user_dic[u] = "OK"

        assert all_user_dic.get(u1, False) == "OK"
        assert all_user_dic.get(u2, False) == "OK"
        assert all_user_dic.get(u3, False) == "OK"
        assert all_user_dic.get(u4, False) is False  # New object
        assert all_user_dic.get(b1, False) is False  # Other object
        assert all_user_dic.get(b2, False) is False  # Other object

        # Make sure docs are properly identified in a set (__hash__ is used
        # for hashing the docs).
        all_user_set = {doc async for doc in User.objects.all()}
        assert u1 in all_user_set
        assert u4 not in all_user_set
        assert b1 not in all_user_list
        assert b2 not in all_user_list

        # Make sure duplicate docs aren't accepted in the set
        assert len(all_user_set) == 3
        all_user_set.add(u1)
        all_user_set.add(u2)
        all_user_set.add(u3)
        assert len(all_user_set) == 3

    async def test_picklable(self):
        pickle_doc = PickleTest(number=1, string="One", lists=["1", "2"])
        pickle_doc.embedded = PickleEmbedded()
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved
        await pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc

        # Test pickling changed data
        pickle_doc.lists.append("3")
        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc
        resurrected.string = "Two"
        await resurrected.save()

        pickle_doc = await PickleTest.objects.first()
        assert resurrected == pickle_doc
        assert pickle_doc.string == "Two"
        assert pickle_doc.lists == ["1", "2", "3"]

    async def test_regular_document_pickle(self):
        pickle_doc = PickleTest(number=1, string="One", lists=["1", "2"])
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved
        await pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)

        # Test that when a document's definition changes the new
        # definition is used
        fixtures.PickleTest = fixtures.NewDocumentPickleTest

        resurrected = pickle.loads(pickled_doc)
        assert resurrected.__class__ == fixtures.NewDocumentPickleTest
        assert resurrected._fields_ordered == fixtures.NewDocumentPickleTest._fields_ordered
        assert resurrected._fields_ordered != pickle_doc._fields_ordered

        # The local PickleTest is still a ref to the original
        fixtures.PickleTest = PickleTest

    async def test_dynamic_document_pickle(self):
        pickle_doc = PickleDynamicTest(name="test", number=1, string="One", lists=["1", "2"])
        pickle_doc.embedded = PickleDynamicEmbedded(foo="Bar")
        pickled_doc = pickle.dumps(pickle_doc)  # make sure pickling works even before the doc is saved

        await pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc
        assert resurrected._fields_ordered == pickle_doc._fields_ordered
        assert resurrected._dynamic_fields.keys() == pickle_doc._dynamic_fields.keys()

        assert resurrected.embedded == pickle_doc.embedded
        assert resurrected.embedded._fields_ordered == pickle_doc.embedded._fields_ordered
        assert resurrected.embedded._dynamic_fields.keys() == pickle_doc.embedded._dynamic_fields.keys()

    async def test_picklable_on_signals(self):
        pickle_doc = PickleSignalsTest(number=1, string="One", lists=["1", "2"])
        pickle_doc.embedded = PickleEmbedded()
        await pickle_doc.save()
        await pickle_doc.delete()

    async def test_shard_key_mutability_after_from_json(self):
        """Ensure that a document ID can be modified after from_json.

        If you instantiate a document by using from_json/_from_son and you
        indicate that this should be considered a new document (vs a doc that
        already exists in the database), then you should be able to modify
        fields that are part of its shard key (note that this is not permitted
        on docs that are already persisted).

        See https://github.com/mongoengine/mongoengine/issues/771 for details.
        """

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {"shard_key": ("id", "name")}

        p = Person.from_json('{"name": "name", "age": 27}', created=True)
        assert p._created is True
        p.name = "new name"
        p.id = "12345"
        assert p.name == "new name"
        assert p.id == "12345"

    async def test_shard_key_mutability_after_from_son(self):
        """Ensure that a document ID can be modified after _from_son.

        See `test_shard_key_mutability_after_from_json` above for more details.
        """

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {"shard_key": ("id", "name")}

        p = Person._from_son({"name": "name", "age": 27}, created=True)
        assert p._created is True
        p.name = "new name"
        p.id = "12345"
        assert p.name == "new name"
        assert p.id == "12345"

    async def test_from_son_null_replaced_by_default(self):
        """Ensure _from_son replaces BSON null with field default when null=False."""

        class Doc(Document):
            name = StringField(default="fallback")
            count = IntField(default=0)

        doc = Doc._from_son({"name": None, "count": None})
        assert doc.name == "fallback"
        assert doc.count == 0

    async def test_from_son_null_kept_when_null_true(self):
        """Ensure _from_son keeps None when field has null=True."""

        class Doc(Document):
            name = StringField(null=True, default="fallback")

        doc = Doc._from_son({"name": None})
        assert doc.name is None

    async def test_from_son_created_true_rejects_extra_keys_on_non_strict(self):
        """Ensure _from_son(created=True) rejects undefined fields even when strict=False."""

        class Doc(Document):
            name = StringField()
            meta = {"strict": False}

        with pytest.raises(FieldDoesNotExist):
            Doc._from_son({"name": "ok", "extra": "bad"}, created=True)

    async def test_from_json_created_false_without_an_id(self):
        class Person(Document):
            name = StringField()

        await Person.objects.delete()

        p = Person.from_json('{"name": "name"}', created=False)
        assert p._created is False
        assert p.id is None

        # Make sure the document is subsequently persisted correctly.
        await p.save()
        assert p.id is not None
        saved_p = await Person.objects.get(id=p.id)
        assert saved_p.name == "name"

    async def test_from_json_created_false_with_an_id(self):
        """See https://github.com/mongoengine/mongoengine/issues/1854"""

        class Person(Document):
            name = StringField()

        await Person.objects.delete()

        p = Person.from_json('{"_id": "5b85a8b04ec5dc2da388296e", "name": "name"}', created=False)
        assert p._created is False
        assert p._changed_fields == []
        assert p.name == "name"
        assert p.id == ObjectId("5b85a8b04ec5dc2da388296e")
        await p.save()

        with pytest.raises(DoesNotExist):
            # Since the object is considered as already persisted (thanks to
            # `created=False` and an existing ID), and we haven't changed any
            # fields (i.e. `_changed_fields` is empty), the document is
            # considered unchanged and hence the `save()` call above did
            # nothing.
            await Person.objects.get(id=p.id)

        assert not p._created
        p.name = "a new name"
        assert p._changed_fields == ["name"]
        await p.save()
        saved_p = await Person.objects.get(id=p.id)
        assert saved_p.name == p.name

    async def test_from_json_created_true_with_an_id(self):
        class Person(Document):
            name = StringField()

        await Person.objects.delete()

        p = Person.from_json('{"_id": "5b85a8b04ec5dc2da388296e", "name": "name"}', created=True)
        assert p._created
        assert p._changed_fields == []
        assert p.name == "name"
        assert p.id == ObjectId("5b85a8b04ec5dc2da388296e")
        await p.save()

        saved_p = await Person.objects.get(id=p.id)
        assert saved_p == p
        assert saved_p.name == "name"

    async def test_from_son_respects_custom_init(self):
        """_from_son must call user-defined __init__ so that runtime
        attributes set during construction are present on the instance."""

        class UserWithInit(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.runtime_flag = "initialized"

        # Direct construction sets the attribute
        direct = UserWithInit(name="alice")
        assert direct.runtime_flag == "initialized"

        # _from_son must also call __init__ and set the attribute
        loaded = UserWithInit._from_son({"name": "alice"})
        assert loaded.runtime_flag == "initialized"

    async def test_init_respects_custom_setattr(self):
        """Fast __init__ path must fall back to legacy path when the
        subclass overrides __setattr__, so the override is honoured."""

        class TrackedDoc(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                self._set_log = []
                super().__init__(*args, **kwargs)

            def __setattr__(self, key, value):
                if not key.startswith("_"):
                    self._set_log.append(key)
                super().__setattr__(key, value)

        doc = TrackedDoc(name="test")
        assert "name" in doc._set_log

    async def test_from_son_respects_custom_setattr(self):
        """_from_son must fall back to __init__ path when the subclass
        overrides __setattr__."""

        class TrackedDoc2(Document):
            name = StringField()

            def __init__(self, *args, **kwargs):
                self._set_log = []
                super().__init__(*args, **kwargs)

            def __setattr__(self, key, value):
                if not key.startswith("_"):
                    self._set_log.append(key)
                super().__setattr__(key, value)

        loaded = TrackedDoc2._from_son({"name": "alice"})
        assert "name" in loaded._set_log

    async def test_choices_display_with_inheritance(self):
        """_has_choices_fields cache must not leak from parent to child.
        A child class that adds a choices field must still get
        get_<field>_display() even if the parent was instantiated first."""

        class ParentDoc(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class ChildDoc(ParentDoc):
            status = StringField(choices=[("a", "Active"), ("i", "Inactive")])

        # Instantiate parent first to cache _has_choices_fields=False on parent
        _parent = ParentDoc(name="parent")

        # Child must still get the display method
        child = ChildDoc(name="child", status="a")
        assert hasattr(child, "get_status_display")
        assert child.get_status_display() == "Active"

        # Also verify via _from_son
        child2 = ChildDoc._from_son({"name": "child2", "status": "a"})
        assert hasattr(child2, "get_status_display")
        assert child2.get_status_display() == "Active"
