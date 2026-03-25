import copy
import pickle
import uuid
import weakref
from datetime import datetime
from unittest.mock import AsyncMock

import bson
import pytest
from bson import DBRef, ObjectId
from pymongo.errors import DuplicateKeyError

from mongoengine import *
from mongoengine import signals
from mongoengine.base import _DocumentRegistry
from mongoengine.connection import get_db
from mongoengine.context_managers import query_counter, switch_db
from mongoengine.errors import (
    FieldDoesNotExist,
    InvalidDocumentError,
    InvalidQueryError,
    NotRegistered,
    NotUniqueError,
    SaveConditionError,
)
from mongoengine.pymongo_support import PYMONGO_VERSION
from mongoengine.queryset import NULLIFY, Q
from tests import fixtures
from tests.fixtures import (
    PickleDynamicEmbedded,
    PickleDynamicTest,
    PickleEmbedded,
    PickleSignalsTest,
    PickleTest,
)
from tests.utils import (
    MongoDBTestCase,
    db_ops_tracker,
    get_as_pymongo,
)


class TestDocumentInstance(MongoDBTestCase):
    def setup_method(self, method=None):
        class Job(EmbeddedDocument):
            name = StringField()
            years = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = EmbeddedDocumentField(Job)

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person
        self.Job = Job

    async def _assert_db_equal(self, docs):
        assert await (await self.Person._get_collection()).find().sort("id").to_list(None) == sorted(
            docs, key=lambda doc: doc["_id"]
        )

    def _assert_has_instance(self, field, instance):
        assert hasattr(field, "_instance")
        assert field._instance is not None
        if isinstance(field._instance, weakref.ProxyType):
            assert field._instance.__eq__(instance)
        else:
            assert field._instance == instance

    async def test_capped_collection(self):
        """Ensure that capped collections work properly."""

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10, "max_size": 4096}

        await Log.drop_collection()

        # Ensure that the collection handles up to its maximum
        for _ in range(10):
            await Log().save()

        assert await Log.objects.count() == 10

        # Check that extra documents don't increase the size
        await Log().save()
        assert await Log.objects.count() == 10

        options = await Log.objects._collection.options()
        assert options["capped"] is True
        assert options["max"] == 10
        assert options["size"] == 4096

        # Check that the document cannot be redefined with different options
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 11}

        # Accessing Document.objects creates the collection
        with pytest.raises(InvalidCollectionError):
            await Log._get_collection()

    async def test_capped_collection_default(self):
        """Ensure that capped collections defaults work properly."""

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10}

        await Log.drop_collection()

        # Create a doc to create the collection
        await Log().save()

        options = await Log.objects._collection.options()
        assert options["capped"] is True
        assert options["max"] == 10
        assert options["size"] == 10 * 2**20

        # Check that the document with default value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10}

        # Create the collection by accessing Document.objects
        Log.objects

    async def test_capped_collection_no_max_size_problems(self):
        """Ensure that capped collections with odd max_size work properly.
        MongoDB rounds up max_size to next multiple of 256, recreating a doc
        with the same spec failed in mongoengine <0.10
        """

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_size": 10000}

        await Log.drop_collection()

        # Create a doc to create the collection
        await Log().save()

        options = await Log.objects._collection.options()
        assert options["capped"] is True
        assert options["size"] >= 10000

        # Check that the document with odd max_size value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_size": 10000}

        # Create the collection by accessing Document.objects
        Log.objects

    async def test_repr(self):
        """Ensure that unicode representation works"""

        class Article(Document):
            title = StringField()

            def __unicode__(self):
                return self.title

        doc = Article(title="привет мир")

        assert "<Article: привет мир>" == repr(doc)

    async def test_repr_none(self):
        """Ensure None values are handled correctly."""

        class Article(Document):
            title = StringField()

            def __str__(self):
                return None

        doc = Article(title="привет мир")
        assert "<Article: None>" == repr(doc)

    async def test_queryset_resurrects_dropped_collection(self):
        await self.Person.drop_collection()
        assert [doc async for doc in self.Person.objects()] == []

        # Ensure works correctly with inhertited classes
        class Actor(self.Person):
            pass

        Actor.objects()
        await self.Person.drop_collection()
        assert [doc async for doc in Actor.objects()] == []

    async def test_polymorphic_references(self):
        """Ensure that the correct subclasses are returned from a query
        when using references / generic references
        """

        class Animal(Document):
            meta = {"allow_inheritance": True}

        class Fish(Animal):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        class Zoo(Document):
            animals = ListField(ReferenceField(Animal))

        await Zoo.drop_collection()
        await Animal.drop_collection()

        await Animal().save()
        await Fish().save()
        await Mammal().save()
        await Dog().save()
        await Human().save()

        # Save a reference to each animal
        zoo = Zoo(animals=[a async for a in Animal.objects])
        await zoo.save()
        await zoo.reload()

        zoos = await Zoo.objects.select_related()
        classes = [a.__class__ for a in zoos[0].animals]
        assert classes == [Animal, Fish, Mammal, Dog, Human]

        await Zoo.drop_collection()

        class Zoo(Document):
            animals = ListField(GenericReferenceField())

        # Save a reference to each animal
        zoo = Zoo(animals=[a async for a in Animal.objects])
        await zoo.save()
        await zoo.reload()

        zoos = await Zoo.objects.select_related()
        classes = [a.__class__ for a in zoos[0].animals]
        assert classes == [Animal, Fish, Mammal, Dog, Human]

    async def test_reference_inheritance(self):
        class Stats(Document):
            created = DateTimeField(default=datetime.now)

            meta = {"allow_inheritance": False}

        class CompareStats(Document):
            generated = DateTimeField(default=datetime.now)
            stats = ListField(ReferenceField(Stats))

        await Stats.drop_collection()
        await CompareStats.drop_collection()

        list_stats = []

        for i in range(10):
            s = Stats()
            await s.save()
            list_stats.append(s)

        cmp_stats = CompareStats(stats=list_stats)
        await cmp_stats.save()

        cmp_stats_list = await CompareStats.objects.select_related()
        assert list_stats == cmp_stats_list[0].stats

    async def test_db_field_load(self):
        """Ensure we load data correctly from the right db field."""

        class Person(Document):
            name = StringField(required=True)
            _rank = StringField(required=False, db_field="rank")

            @property
            def rank(self):
                return self._rank or "Private"

        await Person.drop_collection()

        await Person(name="Jack", _rank="Corporal").save()

        await Person(name="Fred").save()

        assert (await Person.objects.get(name="Jack")).rank == "Corporal"
        assert (await Person.objects.get(name="Fred")).rank == "Private"

    async def test_db_embedded_doc_field_load(self):
        """Ensure we load embedded document data correctly."""

        class Rank(EmbeddedDocument):
            title = StringField(required=True)

        class Person(Document):
            name = StringField(required=True)
            rank_ = EmbeddedDocumentField(Rank, required=False, db_field="rank")

            @property
            def rank(self):
                if self.rank_ is None:
                    return "Private"
                return self.rank_.title

        await Person.drop_collection()

        await Person(name="Jack", rank_=Rank(title="Corporal")).save()
        await Person(name="Fred").save()

        assert (await Person.objects.get(name="Jack")).rank == "Corporal"
        assert (await Person.objects.get(name="Fred")).rank == "Private"

    async def test_custom_id_field(self):
        """Ensure that documents may be created with custom primary keys."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

            meta = {"allow_inheritance": True}

        await User.drop_collection()

        assert User._fields["username"].db_field == "_id"
        assert User._meta["id_field"] == "username"

        await User.objects.create(username="test", name="test user")
        user = await User.objects.first()
        assert user.id == "test"
        assert user.pk == "test"
        user_dict = await User.objects._collection.find_one()
        assert user_dict["_id"] == "test"

    async def test_change_custom_id_field_in_subclass(self):
        """Subclasses cannot override which field is the primary key."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()
            meta = {"allow_inheritance": True}

        with pytest.raises(ValueError, match="Cannot override primary key field"):

            class EmailUser(User):
                email = StringField(primary_key=True)

    async def test_custom_id_field_is_required(self):
        """Ensure the custom primary key field is required."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

        with pytest.raises(ValidationError) as exc_info:
            await User(name="test").save()
        assert "Field is required: ['username']" in str(exc_info.value)

    async def test_document_not_registered(self):
        class Place(Document):
            name = StringField()

            meta = {"allow_inheritance": True}

        class NicePlace(Place):
            pass

        await Place.drop_collection()

        await Place(name="London").save()
        await NicePlace(name="Buckingham Palace").save()

        # Mimic Place and NicePlace definitions being in a different file
        # and the NicePlace model not being imported in at query time.
        _DocumentRegistry.unregister("Place.NicePlace")

        with pytest.raises(NotRegistered):
            [doc async for doc in Place.objects.all()]

    async def test_document_registry_regressions(self):
        class Location(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Area(Location):
            location = ReferenceField("Location", dbref=True)

        await Location.drop_collection()

        assert Area == _DocumentRegistry.get("Area")
        assert Area == _DocumentRegistry.get("Location.Area")

    async def test_creation(self):
        """Ensure that document may be created using keyword arguments."""
        person = self.Person(name="Test User", age=30)
        assert person.name == "Test User"
        assert person.age == 30

    async def test__qs_property_does_not_raise(self):
        # ensures no regression of #2500
        class MyDocument(Document):
            pass

        await MyDocument.drop_collection()
        object = MyDocument()
        await object._qs().insert([MyDocument()])
        assert await MyDocument.objects.count() == 1

    async def test_to_dbref(self):
        """Ensure that you can get a dbref of a document."""
        person = self.Person(name="Test User", age=30)
        with pytest.raises(OperationError):
            person.to_dbref()
        await person.save()
        person.to_dbref()

    async def test_key_like_attribute_access(self):
        person = self.Person(age=30)
        assert person["age"] == 30
        with pytest.raises(KeyError):
            person["unknown_attr"]

    async def test_save_abstract_document(self):
        """Saving an abstract document should fail."""

        class Doc(Document):
            name = StringField()
            meta = {"abstract": True}

        with pytest.raises(InvalidDocumentError):
            await Doc(name="aaa").save()

    async def test_reload(self):
        """Ensure that attributes may be reloaded."""
        person = self.Person(name="Test User", age=20)
        await person.save()

        person_obj = await self.Person.objects.first()
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        await person_obj.save()

        assert person.name == "Test User"
        assert person.age == 20

        await person.reload("age")
        assert person.name == "Test User"
        assert person.age == 21

        await person.reload()
        assert person.name == "Mr Test User"
        assert person.age == 21

        await person.reload()
        assert person.name == "Mr Test User"
        assert person.age == 21

    async def test_reload_sharded(self):
        class Animal(Document):
            superphylum = StringField()
            meta = {"shard_key": ("superphylum",)}

        await Animal.drop_collection()
        doc = await Animal.objects.create(superphylum="Deuterostomia")

        async with query_counter() as q:
            await doc.reload()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.animal"}).to_list(None))[0]
            assert set(query_op["command"]["filter"].keys()) == {
                "_id",
                "superphylum",
            }

    async def test_reload_sharded_with_db_field(self):
        class Person(Document):
            nationality = StringField(db_field="country")
            meta = {"shard_key": ("nationality",)}

        await Person.drop_collection()
        doc = await Person.objects.create(nationality="Poland")

        async with query_counter() as q:
            await doc.reload()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.person"}).to_list(None))[0]
            assert set(query_op["command"]["filter"].keys()) == {"_id", "country"}

    async def test_reload_sharded_nested(self):
        class SuperPhylum(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            superphylum = EmbeddedDocumentField(SuperPhylum)
            meta = {"shard_key": ("superphylum.name",)}

        await Animal.drop_collection()
        doc = Animal(superphylum=SuperPhylum(name="Deuterostomia"))
        await doc.save()
        await doc.reload()
        await Animal.drop_collection()

    async def test_save_update_shard_key_routing(self):
        """Ensures updating a doc with a specified shard_key includes it in
        the query.
        """

        class Animal(Document):
            is_mammal = BooleanField()
            name = StringField()
            meta = {"shard_key": ("is_mammal", "id")}

        await Animal.drop_collection()
        doc = Animal(is_mammal=True, name="Dog")
        await doc.save()

        async with query_counter() as q:
            doc.name = "Cat"
            await doc.save()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.animal"}).to_list(None))[0]
            assert query_op["op"] == "update"
            assert set(query_op["command"]["q"].keys()) == {"_id", "is_mammal"}

        await Animal.drop_collection()

    async def test_save_create_shard_key_routing(self):
        """Ensures inserting a doc with a specified shard_key includes it in
        the query.
        """

        class Animal(Document):
            _id = UUIDField(binary=False, primary_key=True, default=uuid.uuid4)
            is_mammal = BooleanField()
            name = StringField()
            meta = {"shard_key": ("is_mammal",)}

        await Animal.drop_collection()
        doc = Animal(is_mammal=True, name="Dog")

        async with query_counter() as q:
            await doc.save()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.animal"}).to_list(None))[0]
            assert query_op["op"] == "command"
            assert query_op["command"]["findAndModify"] == "animal"
            assert set(query_op["command"]["query"].keys()) == {"_id", "is_mammal"}

        await Animal.drop_collection()

    async def test_reload_with_changed_fields(self):
        """Ensures reloading will not affect changed fields"""

        class User(Document):
            name = StringField()
            number = IntField()

        await User.drop_collection()

        user = await User(name="Bob", number=1).save()
        user.name = "John"
        user.number = 2

        assert user._get_changed_fields() == ["name", "number"]
        await user.reload("number")
        assert user._get_changed_fields() == ["name"]
        await user.save()
        await user.reload()
        assert user.name == "John"

    async def test_reload_referencing(self):
        """Ensures reloading updates weakrefs correctly."""

        class Embedded(EmbeddedDocument):
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.drop_collection()
        doc = Doc()
        doc.dict_field = {"hello": "world"}
        doc.list_field = ["1", 2, {"hello": "world"}]

        embedded_1 = Embedded()
        embedded_1.dict_field = {"hello": "world"}
        embedded_1.list_field = ["1", 2, {"hello": "world"}]
        doc.embedded_field = embedded_1
        await doc.save()

        doc = await doc.reload(10)
        doc.list_field.append(1)
        doc.dict_field["woot"] = "woot"
        doc.embedded_field.list_field.append(1)
        doc.embedded_field.dict_field["woot"] = "woot"

        changed = doc._get_changed_fields()
        assert changed == [
            "list_field",
            "dict_field.woot",
            "embedded_field.list_field",
            "embedded_field.dict_field.woot",
        ]
        await doc.save()

        assert len(doc.list_field) == 4
        doc = await doc.reload(10)
        assert doc._get_changed_fields() == []
        assert len(doc.list_field) == 4
        assert len(doc.dict_field) == 2
        assert len(doc.embedded_field.list_field) == 4
        assert len(doc.embedded_field.dict_field) == 2

        doc.list_field.append(1)
        await doc.save()
        doc.dict_field["extra"] = 1
        doc = await doc.reload(10, "list_field")
        assert doc._get_changed_fields() == ["dict_field.extra"]
        assert len(doc.list_field) == 5
        assert len(doc.dict_field) == 3
        assert len(doc.embedded_field.list_field) == 4
        assert len(doc.embedded_field.dict_field) == 2

    async def test_reload_doesnt_exist(self):
        class Foo(Document):
            pass

        f = Foo()
        with pytest.raises(Foo.DoesNotExist):
            await f.reload()

        await f.save()
        await f.delete()

        with pytest.raises(Foo.DoesNotExist):
            await f.reload()

    async def test_reload_of_non_strict_with_special_field_name(self):
        """Ensures reloading works for documents with meta strict is False."""

        class Post(Document):
            meta = {"strict": False}
            title = StringField()
            items = ListField()

        await Post.drop_collection()

        await (await Post._get_collection()).insert_one(
            {"title": "Items eclipse", "items": ["more lorem", "even more ipsum"]}
        )

        post = await Post.objects.first()
        await post.reload()
        assert post.title == "Items eclipse"
        assert post.items == ["more lorem", "even more ipsum"]

    async def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly."""
        person = self.Person(name="Test User", age=30, job=self.Job())
        assert person["name"] == "Test User"

        with pytest.raises(KeyError):
            person.__getitem__("salary")
        with pytest.raises(KeyError):
            person.__setitem__("salary", 50)

        person["name"] = "Another User"
        assert person["name"] == "Another User"

        # Length = length(assigned fields + id)
        assert len(person) == 5

        assert "age" in person
        person.age = None
        assert "age" not in person
        assert "nationality" not in person

    async def test_embedded_document_to_mongo(self):
        class Person(EmbeddedDocument):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        assert sorted(Person(name="Bob", age=35).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
        ]
        assert sorted(Employee(name="Bob", age=35, salary=0).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
            "salary",
        ]

    async def test_embedded_document_to_mongo_id(self):
        class SubDoc(EmbeddedDocument):
            id = StringField(required=True)

        sub_doc = SubDoc(id="abc")
        assert list(sub_doc.to_mongo().keys()) == ["id"]

    async def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly."""

        class Comment(EmbeddedDocument):
            content = StringField()

        assert "content" in Comment._fields
        assert "id" not in Comment._fields

    async def test_embedded_document_instance(self):
        """Ensure that embedded documents can reference parent instance."""

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.drop_collection()

        doc = Doc(embedded_field=Embedded(string="Hi"))
        self._assert_has_instance(doc.embedded_field, doc)

        await doc.save()
        doc = await Doc.objects.get()
        self._assert_has_instance(doc.embedded_field, doc)

    async def test_embedded_document_complex_instance(self):
        """Ensure that embedded documents in complex fields can reference
        parent instance.
        """

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        await Doc.drop_collection()
        doc = Doc(embedded_field=[Embedded(string="Hi")])
        self._assert_has_instance(doc.embedded_field[0], doc)

        await doc.save()
        doc = await Doc.objects.get()
        self._assert_has_instance(doc.embedded_field[0], doc)

    async def test_embedded_document_complex_instance_no_use_db_field(self):
        """Ensure that use_db_field is propagated to list of Emb Docs."""

        class Embedded(EmbeddedDocument):
            string = StringField(db_field="s")

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        d = Doc(embedded_field=[Embedded(string="Hi")]).to_mongo(use_db_field=False).to_dict()
        assert d["embedded_field"] == [{"string": "Hi"}]

    async def test_instance_is_set_on_setattr(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            email = EmbeddedDocumentField(Email)

        await Account.drop_collection()

        acc = Account()
        acc.email = Email(email="test@example.com")
        self._assert_has_instance(acc._data["email"], acc)
        await acc.save()

        acc1 = await Account.objects.first()
        self._assert_has_instance(acc1._data["email"], acc1)

    async def test_instance_is_set_on_setattr_on_embedded_document_list(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            emails = EmbeddedDocumentListField(Email)

        await Account.drop_collection()
        acc = Account()
        acc.emails = [Email(email="test@example.com")]
        self._assert_has_instance(acc._data["emails"][0], acc)
        await acc.save()

        acc1 = await Account.objects.first()
        self._assert_has_instance(acc1._data["emails"][0], acc1)

    async def test_save_checks_that_clean_is_called(self):
        class CustomError(Exception):
            pass

        class TestDocument(Document):
            def clean(self):
                raise CustomError()

        with pytest.raises(CustomError):
            await TestDocument().save()

        await TestDocument().save(clean=False)

    async def test_save_signal_pre_save_post_validation_makes_change_to_doc(self):
        class BlogPost(Document):
            content = StringField()

            @classmethod
            def pre_save_post_validation(cls, sender, document, **kwargs):
                document.content = "checked"

        signals.pre_save_post_validation.connect(BlogPost.pre_save_post_validation, sender=BlogPost)

        await BlogPost.drop_collection()

        post = await BlogPost(content="unchecked").save()
        assert post.content == "checked"
        # Make sure pre_save_post_validation changes makes it to the db
        raw_doc = await get_as_pymongo(post)
        assert raw_doc == {"content": "checked", "_id": post.id}

        # Important to disconnect as it could cause some assertions in test_signals
        # to fail (due to the garbage collection timing of this signal)
        signals.pre_save_post_validation.disconnect(BlogPost.pre_save_post_validation)

    async def test_document_clean(self):
        class TestDocument(Document):
            status = StringField()
            cleaned = BooleanField(default=False)

            def clean(self):
                self.cleaned = True

        await TestDocument.drop_collection()

        t = TestDocument(status="draft")

        # Ensure clean=False prevent call to clean
        t = TestDocument(status="published")
        await t.save(clean=False)
        assert t.status == "published"
        assert t.cleaned is False

        t = TestDocument(status="published")
        assert t.cleaned is False
        await t.save(clean=True)
        assert t.status == "published"
        assert t.cleaned is True
        raw_doc = await get_as_pymongo(t)
        # Make sure clean changes makes it to the db
        assert raw_doc == {"status": "published", "cleaned": True, "_id": t.id}

    async def test_document_embedded_clean(self):
        class TestEmbeddedDocument(EmbeddedDocument):
            x = IntField(required=True)
            y = IntField(required=True)
            z = IntField(required=True)

            meta = {"allow_inheritance": False}

            def clean(self):
                if self.z:
                    if self.z != self.x + self.y:
                        raise ValidationError("Value of z != x + y")
                else:
                    self.z = self.x + self.y

        class TestDocument(Document):
            doc = EmbeddedDocumentField(TestEmbeddedDocument)
            status = StringField()

        await TestDocument.drop_collection()

        t = TestDocument(doc=TestEmbeddedDocument(x=10, y=25, z=15))

        with pytest.raises(ValidationError) as exc_info:
            await t.save()

        expected_msg = "Value of z != x + y"
        assert expected_msg in str(exc_info.value)
        assert exc_info.value.to_dict() == {"doc": {"__all__": expected_msg}}

        t = await TestDocument(doc=TestEmbeddedDocument(x=10, y=25)).save()
        assert t.doc.z == 35

        # Asserts not raises
        t = TestDocument(doc=TestEmbeddedDocument(x=15, y=35, z=5))
        await t.save(clean=False)

    async def test_modify_empty(self):
        doc = await self.Person(name="bob", age=10).save()

        with pytest.raises(InvalidDocumentError):
            await self.Person().modify(set__age=10)

        await self._assert_db_equal([dict(doc.to_mongo())])

    async def test_modify_invalid_query(self):
        doc1 = await self.Person(name="bob", age=10).save()
        doc2 = await self.Person(name="jim", age=20).save()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        with pytest.raises(InvalidQueryError):
            await doc1.modify({"id": doc2.id}, set__value=20)

        await self._assert_db_equal(docs)

    async def test_modify_match_another_document(self):
        doc1 = await self.Person(name="bob", age=10).save()
        doc2 = await self.Person(name="jim", age=20).save()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        n_modified = await doc1.modify({"name": doc2.name}, set__age=100)
        assert n_modified == 0

        await self._assert_db_equal(docs)

    async def test_modify_not_exists(self):
        doc1 = await self.Person(name="bob", age=10).save()
        doc2 = self.Person(id=ObjectId(), name="jim", age=20)
        docs = [dict(doc1.to_mongo())]

        n_modified = await doc2.modify({"name": doc2.name}, set__age=100)
        assert n_modified == 0

        await self._assert_db_equal(docs)

    async def test_modify_update(self):
        other_doc = await self.Person(name="bob", age=10).save()
        doc = await self.Person(name="jim", age=20, job=self.Job(name="10gen", years=3)).save()

        doc_copy = doc._from_son(doc.to_mongo())

        # these changes must go away
        doc.name = "liza"
        doc.job.name = "Google"
        doc.job.years = 3

        n_modified = await doc.modify(set__age=21, set__job__name="MongoDB", unset__job__years=True)
        assert n_modified == 1
        doc_copy.age = 21
        doc_copy.job.name = "MongoDB"
        del doc_copy.job.years

        assert doc.to_json() == doc_copy.to_json()
        assert doc._get_changed_fields() == []

        await self._assert_db_equal([dict(other_doc.to_mongo()), dict(doc.to_mongo())])

    async def test_modify_with_positional_push(self):
        class Content(EmbeddedDocument):
            keywords = ListField(StringField())

        class BlogPost(Document):
            tags = ListField(StringField())
            content = EmbeddedDocumentField(Content)

        post = await BlogPost.objects.create(tags=["python"], content=Content(keywords=["ipsum"]))

        assert post.tags == ["python"]
        await post.modify(push__tags__0=["code", "mongo"])
        assert post.tags == ["code", "mongo", "python"]

        # Assert same order of the list items is maintained in the db
        assert (await (await BlogPost._get_collection()).find_one({"_id": post.pk}))["tags"] == [
            "code",
            "mongo",
            "python",
        ]

        assert post.content.keywords == ["ipsum"]
        await post.modify(push__content__keywords__0=["lorem"])
        assert post.content.keywords == ["lorem", "ipsum"]

        # Assert same order of the list items is maintained in the db
        assert (await (await BlogPost._get_collection()).find_one({"_id": post.pk}))["content"]["keywords"] == [
            "lorem",
            "ipsum",
        ]

    async def test_save(self):
        """Ensure that a document may be saved in the database."""

        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30)
        await person.save()

        # Ensure that the object is in the database
        raw_doc = await get_as_pymongo(person)
        assert raw_doc == {
            "_cls": "Person",
            "name": "Test User",
            "age": 30,
            "_id": person.id,
        }

    async def test_save_write_concern(self):
        class Recipient(Document):
            email = EmailField(required=True)

        rec = Recipient(email="garbage@garbage.com")

        fn = AsyncMock()
        rec._save_create = fn
        await rec.save(write_concern={"w": 0})
        assert fn.call_args[1]["write_concern"] == {"w": 0}

    async def test_save_skip_validation(self):
        class Recipient(Document):
            email = EmailField(required=True)

        recipient = Recipient(email="not-an-email")
        with pytest.raises(ValidationError):
            await recipient.save()

        await recipient.save(validate=False)
        raw_doc = await get_as_pymongo(recipient)
        assert raw_doc == {"email": "not-an-email", "_id": recipient.id}

    async def test_save_with_bad_id(self):
        class Clown(Document):
            id = IntField(primary_key=True)

        with pytest.raises(ValidationError):
            await Clown(id="not_an_int").save()

    async def test_save_to_a_value_that_equates_to_false(self):
        class Thing(EmbeddedDocument):
            count = IntField()

        class User(Document):
            thing = EmbeddedDocumentField(Thing)

        await User.drop_collection()

        user = User(thing=Thing(count=1))
        await user.save()
        await user.reload()

        user.thing.count = 0
        await user.save()

        await user.reload()
        assert user.thing.count == 0

    async def test_save_max_recursion_not_hit(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")
            friend = ReferenceField("self")

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.save()

        p1.friend = p2
        await p1.save()

        # Confirm can save and it resets the changed fields without hitting
        # max recursion error
        p0 = await Person.objects.first()
        p0.name = "wpjunior"
        await p0.save()

    async def test_save_cascades(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.save()

        persons = await Person.objects(name="Wilson Jr").select_related()
        p = persons[0]
        p.parent.name = "Daddy Wilson"
        await p.save(cascade=True)

        await p1.reload()
        assert p1.name == p.parent.name

    async def test_save_cascade_kwargs(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p1.name = "Daddy Wilson"
        await p2.save(force_insert=True, cascade_kwargs={"force_insert": False})

        await p1.reload()
        persons = await Person.objects(name="Wilson Jr").select_related()
        p2_loaded = persons[0]
        assert p1.name == p2_loaded.parent.name

    async def test_save_cascade_meta_false(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

            meta = {"cascade": False}

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.save()

        persons = await Person.objects(name="Wilson Jr").select_related()
        p = persons[0]
        p.parent.name = "Daddy Wilson"
        await p.save()

        await p1.reload()
        assert p1.name != p.parent.name

        await p.save(cascade=True)
        await p1.reload()
        assert p1.name == p.parent.name

    async def test_save_cascade_meta_true(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

            meta = {"cascade": False}

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.save(cascade=True)

        persons = await Person.objects(name="Wilson Jr").select_related()
        p = persons[0]
        p.parent.name = "Daddy Wilson"
        await p.save()

        await p1.reload()
        assert p1.name != p.parent.name

    async def test_save_cascades_generically(self):
        class Person(Document):
            name = StringField()
            parent = GenericReferenceField()

        await Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        await p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.save()

        persons = await Person.objects(name="Wilson Jr").select_related()
        p = persons[0]
        p.parent.name = "Daddy Wilson"
        await p.save()

        await p1.reload()
        assert p1.name != p.parent.name

        await p.save(cascade=True)
        await p1.reload()
        assert p1.name == p.parent.name

    async def test_save_atomicity_condition(self):
        class Widget(Document):
            toggle = BooleanField(default=False)
            count = IntField(default=0)
            save_id = UUIDField()

        def flip(widget):
            widget.toggle = not widget.toggle
            widget.count += 1

        def UUID(i):
            return uuid.UUID(int=i)

        await Widget.drop_collection()

        w1 = Widget(toggle=False, save_id=UUID(1))

        # ignore save_condition on new record creation
        await w1.save(save_condition={"save_id": UUID(42)})
        await w1.reload()
        assert not w1.toggle
        assert w1.save_id == UUID(1)
        assert w1.count == 0

        # mismatch in save_condition prevents save and raise exception
        flip(w1)
        assert w1.toggle
        assert w1.count == 1
        with pytest.raises(SaveConditionError):
            await w1.save(save_condition={"save_id": UUID(42)})
        await w1.reload()
        assert not w1.toggle
        assert w1.count == 0

        # matched save_condition allows save
        flip(w1)
        assert w1.toggle
        assert w1.count == 1
        await w1.save(save_condition={"save_id": UUID(1)})
        await w1.reload()
        assert w1.toggle
        assert w1.count == 1

        # save_condition can be used to ensure atomic read & updates
        # i.e., prevent interleaved reads and writes from separate contexts
        w2 = await Widget.objects.get()
        assert w1 == w2
        old_id = w1.save_id

        flip(w1)
        w1.save_id = UUID(2)
        await w1.save(save_condition={"save_id": old_id})
        await w1.reload()
        assert not w1.toggle
        assert w1.count == 2
        flip(w2)
        flip(w2)
        with pytest.raises(SaveConditionError):
            await w2.save(save_condition={"save_id": old_id})
        await w2.reload()
        assert not w2.toggle
        assert w2.count == 2

        # save_condition uses mongoengine-style operator syntax
        flip(w1)
        await w1.save(save_condition={"count__lt": w1.count})
        await w1.reload()
        assert w1.toggle
        assert w1.count == 3
        flip(w1)
        with pytest.raises(SaveConditionError):
            await w1.save(save_condition={"count__gte": w1.count})
        await w1.reload()
        assert w1.toggle
        assert w1.count == 3

    async def test_save_update_selectively(self):
        class WildBoy(Document):
            age = IntField()
            name = StringField()

        await WildBoy.drop_collection()

        await WildBoy(age=12, name="John").save()

        boy1 = await WildBoy.objects().first()
        boy2 = await WildBoy.objects().first()

        boy1.age = 99
        await boy1.save()
        boy2.name = "Bob"
        await boy2.save()

        fresh_boy = await WildBoy.objects().first()
        assert fresh_boy.age == 99
        assert fresh_boy.name == "Bob"

    async def test_save_update_selectively_with_custom_pk(self):
        # Prevents regression of #2082
        class WildBoy(Document):
            pk_id = StringField(primary_key=True)
            age = IntField()
            name = StringField()

        await WildBoy.drop_collection()

        await WildBoy(pk_id="A", age=12, name="John").save()

        boy1 = await WildBoy.objects().first()
        boy2 = await WildBoy.objects().first()

        boy1.age = 99
        await boy1.save()
        boy2.name = "Bob"
        await boy2.save()

        fresh_boy = await WildBoy.objects().first()
        assert fresh_boy.age == 99
        assert fresh_boy.name == "Bob"

    async def test_update(self):
        """Ensure that an existing document is updated instead of be
        overwritten.
        """
        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30)
        await person.save()

        # Create same person object, with same id, without age
        same_person = self.Person(name="Test")
        same_person.id = person.id
        await same_person.save()

        # Confirm only one object
        assert await self.Person.objects.count() == 1

        # reload
        await person.reload()
        await same_person.reload()

        # Confirm the same
        assert person == same_person
        assert person.name == same_person.name
        assert person.age == same_person.age

        # Confirm the saved values
        assert person.name == "Test"
        assert person.age == 30

        # Test only / exclude only updates included fields
        person = await self.Person.objects.only("name").get()
        person.name = "User"
        await person.save()

        await person.reload()
        assert person.name == "User"
        assert person.age == 30

        # test exclude only updates set fields
        person = await self.Person.objects.exclude("name").get()
        person.age = 21
        await person.save()

        await person.reload()
        assert person.name == "User"
        assert person.age == 21

        # Test only / exclude can set non excluded / included fields
        person = await self.Person.objects.only("name").get()
        person.name = "Test"
        person.age = 30
        await person.save()

        await person.reload()
        assert person.name == "Test"
        assert person.age == 30

        # test exclude only updates set fields
        person = await self.Person.objects.exclude("name").get()
        person.name = "User"
        person.age = 21
        await person.save()

        await person.reload()
        assert person.name == "User"
        assert person.age == 21

        # Confirm does remove unrequired fields
        person = await self.Person.objects.exclude("name").get()
        person.age = None
        await person.save()

        await person.reload()
        assert person.name == "User"
        assert person.age is None

        person = await self.Person.objects.get()
        person.name = None
        person.age = None
        await person.save()

        await person.reload()
        assert person.name is None
        assert person.age is None

    async def test_update_rename_operator(self):
        """Test the $rename operator."""
        coll = await self.Person._get_collection()
        doc = await self.Person(name="John").save()
        raw_doc = await coll.find_one({"_id": doc.pk})
        assert set(raw_doc.keys()) == {"_id", "_cls", "name"}

        await doc.update(rename__name="first_name")
        raw_doc = await coll.find_one({"_id": doc.pk})
        assert set(raw_doc.keys()) == {"_id", "_cls", "first_name"}
        assert raw_doc["first_name"] == "John"

    async def test_inserts_if_you_set_the_pk(self):
        _ = await self.Person(name="p1", id=bson.ObjectId()).save()
        p2 = self.Person(name="p2")
        p2.id = bson.ObjectId()
        await p2.save()

        assert 2 == await self.Person.objects.count()

    async def test_can_save_if_not_included(self):
        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        class Doc(Document):
            string_field = StringField(default="1")
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.now)
            embedded_document_field = EmbeddedDocumentField(EmbeddedDoc, default=lambda: EmbeddedDoc())
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=bson.ObjectId)
            reference_field = ReferenceField(Simple, required=False)
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField(required=False)
            sorted_list_field = SortedListField(IntField(), default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(default=lambda: EmbeddedDoc())

        await Simple.drop_collection()
        await Doc.drop_collection()

        await Doc().save()
        my_doc = await Doc.objects.only("string_field").first()
        my_doc.string_field = "string"
        await my_doc.save()

        my_doc = await Doc.objects.get(string_field="string")
        assert my_doc.string_field == "string"
        assert my_doc.int_field == 1

    async def test_document_update(self):
        # try updating a non-saved document
        with pytest.raises(OperationError):
            person = self.Person(name="dcrosta")
            await person.update(set__name="Dan Crosta")

        author = self.Person(name="dcrosta")
        await author.save()

        await author.update(set__name="Dan Crosta")
        await author.reload()

        p1 = await self.Person.objects.first()
        assert p1.name == author.name

        # try sending an empty update
        with pytest.raises(OperationError):
            person = await self.Person.objects.first()
            await person.update()

        # update that doesn't explicitly specify an operator should default
        # to 'set__'
        person = await self.Person.objects.first()
        await person.update(name="Dan")
        await person.reload()
        assert "Dan" == person.name

    async def test_update_unique_field(self):
        class Doc(Document):
            name = StringField(unique=True)

        doc1 = await Doc(name="first").save()
        doc2 = await Doc(name="second").save()

        with pytest.raises(NotUniqueError):
            await doc2.update(set__name=doc1.name)

    async def test_embedded_update(self):
        """Test update on `EmbeddedDocumentField` fields."""

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.save()

        # Update
        site = await Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        await site.save()

        site = await Site.objects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_update_list_field(self):
        """Test update on `ListField` with $pull + $in."""

        class Doc(Document):
            foo = ListField(StringField())

        await Doc.drop_collection()
        doc = Doc(foo=["a", "b", "c"])
        await doc.save()

        # Update
        doc = await Doc.objects.first()
        await doc.update(pull__foo__in=["a", "c"])

        doc = await Doc.objects.first()
        assert doc.foo == ["b"]

    async def test_embedded_update_db_field(self):
        """Test update on `EmbeddedDocumentField` fields when db_field
        is other than default.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", db_field="page_log_message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.drop_collection()

        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.save()

        # Update
        site = await Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        await site.save()

        site = await Site.objects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_save_only_changed_fields(self):
        """Ensure save only sets / unsets changed fields."""

        class User(self.Person):
            active = BooleanField(default=True)

        await User.drop_collection()

        # Create person object and save it to the database
        user = User(name="Test User", age=30, active=True)
        await user.save()
        await user.reload()

        # Simulated Race condition
        same_person = await self.Person.objects.get()
        same_person.active = False

        user.age = 21
        await user.save()

        same_person.name = "User"
        await same_person.save()

        person = await self.Person.objects.get()
        assert person.name == "User"
        assert person.age == 21
        assert person.active is False

    async def test__get_changed_fields_same_ids_reference_field_does_not_enters_infinite_loop_embedded_doc(
        self,
    ):
        # Refers to Issue #1685
        class EmbeddedChildModel(EmbeddedDocument):
            id = DictField(primary_key=True)

        class ParentModel(Document):
            child = EmbeddedDocumentField(EmbeddedChildModel)

        emb = EmbeddedChildModel(id={"1": [1]})
        changed_fields = ParentModel(child=emb)._get_changed_fields()
        assert changed_fields == []

    async def test__get_changed_fields_same_ids_reference_field_does_not_enters_infinite_loop_different_doc(
        self,
    ):
        # Refers to Issue #1685
        class User(Document):
            id = IntField(primary_key=True)
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            author = ReferenceField(User)

        await Message.drop_collection()

        # All objects share the same id, but each in a different collection
        user = await User(id=1, name="user-name").save()
        message = await Message(id=1, author=user).save()

        message.author.name = "tutu"
        assert message._get_changed_fields() == []
        assert user._get_changed_fields() == ["name"]

    async def test__get_changed_fields_same_ids_embedded(self):
        # Refers to Issue #1768
        class User(EmbeddedDocument):
            id = IntField()
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            author = EmbeddedDocumentField(User)

        await Message.drop_collection()

        # All objects share the same id, but each in a different collection
        user = User(id=1, name="user-name")  # await .save()
        message = await Message(id=1, author=user).save()

        message.author.name = "tutu"
        assert message._get_changed_fields() == ["author.name"]
        await message.save()

        message_fetched = await Message.objects.with_id(message.id)
        assert message_fetched.author.name == "tutu"

    async def test_query_count_when_saving(self):
        """Ensure references don't cause extra fetches when saving"""

        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            orgs = ListField(ReferenceField("Organization"))

        class Feed(Document):
            name = StringField()

        class UserSubscription(Document):
            name = StringField()
            user = ReferenceField(User)
            feed = ReferenceField(Feed)

        await Organization.drop_collection()
        await User.drop_collection()
        await Feed.drop_collection()
        await UserSubscription.drop_collection()

        o1 = await Organization(name="o1").save()
        o2 = await Organization(name="o2").save()

        u1 = await User(name="Ross", orgs=[o1, o2]).save()
        f1 = await Feed(name="MongoEngine").save()

        sub = await UserSubscription(user=u1, feed=f1).save()

        user = await User.objects.first()
        # In async mongoengine, references are stored as DBRefs internally
        # and not auto-dereferenced. Use select_related to dereference.
        assert isinstance(user._data["orgs"][0], DBRef)

        users = await User.objects.select_related()
        user = users[0]
        assert isinstance(user.orgs[0], Organization)

        # Changing a value
        async with query_counter() as q:
            assert await q.get_count() == 0
            sub = await UserSubscription.objects.first()
            assert await q.get_count() == 1
            sub.name = "Test Sub"
            await sub.save()
            assert await q.get_count() == 2

        # Changing a value that will cascade
        subs = await UserSubscription.objects.select_related()
        sub = subs[0]
        sub.user.name = "Test"
        await sub.save(cascade=True)

        # Changing a value and one that will cascade
        subs = await UserSubscription.objects.select_related()
        sub = subs[0]
        sub.name = "Test Sub 2"
        sub.user.name = "Test 2"
        await sub.save(cascade=True)

        # Saving with just the refs
        async with query_counter() as q:
            assert await q.get_count() == 0
            sub = UserSubscription(user=u1.pk, feed=f1.pk)
            assert await q.get_count() == 0
            await sub.save()
            assert await q.get_count() == 1

        # Saving with just the refs on a ListField
        async with query_counter() as q:
            assert await q.get_count() == 0
            await User(name="Bob", orgs=[o1.pk, o2.pk]).save()
            assert await q.get_count() == 1

        # Saving new objects
        async with query_counter() as q:
            assert await q.get_count() == 0
            user = await User.objects.first()
            assert await q.get_count() == 1
            feed = await Feed.objects.first()
            assert await q.get_count() == 2
            sub = UserSubscription(user=user, feed=feed)
            assert await q.get_count() == 2  # Check no change
            await sub.save()
            assert await q.get_count() == 3

    async def test_set_unset_one_operation(self):
        """Ensure that $set and $unset actions are performed in the
        same operation.
        """

        class FooBar(Document):
            foo = StringField(default=None)
            bar = StringField(default=None)

        await FooBar.drop_collection()

        # write an entity with a single prop
        foo = await FooBar(foo="foo").save()

        assert foo.foo == "foo"
        del foo.foo
        foo.bar = "bar"

        async with query_counter() as q:
            assert 0 == await q.get_count()
            await foo.save()
            assert 1 == await q.get_count()

    async def test_save_only_changed_fields_recursive(self):
        """Ensure save only sets / unsets changed fields."""

        class Comment(EmbeddedDocument):
            published = BooleanField(default=True)

        class User(self.Person):
            comments_dict = DictField()
            comments = ListField(EmbeddedDocumentField(Comment))
            active = BooleanField(default=True)

        await User.drop_collection()

        # Create person object and save it to the database
        person = User(name="Test User", age=30, active=True)
        person.comments.append(Comment())
        await person.save()
        await person.reload()

        person = await self.Person.objects.get()
        assert person.comments[0].published

        person.comments[0].published = False
        await person.save()

        person = await self.Person.objects.get()
        assert not person.comments[0].published

        # Simple dict w
        person.comments_dict["first_post"] = Comment()
        await person.save()

        person = await self.Person.objects.get()
        assert person.comments_dict["first_post"]["published"]

        person.comments_dict["first_post"]["published"] = False
        await person.save()

        person = await self.Person.objects.get()
        assert not person.comments_dict["first_post"]["published"]

    async def test_update_propagates_hint_collation_and_comment(self):
        """Make sure adding a hint/comment/collation to the query gets added to the query"""

        base = {"locale": "en", "strength": 2}
        index_name = "name_1"

        class AggPerson(Document):
            name = StringField()
            meta = {"indexes": [{"fields": ["name"], "name": index_name, "collation": base}]}

        await AggPerson.drop_collection()
        _ = await AggPerson.objects.first()

        comment = "test_comment"

        if PYMONGO_VERSION >= (4, 1):
            async with db_ops_tracker() as q:
                _ = await AggPerson.objects.comment(comment).update_one(name="something")
                query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
                CMD_QUERY_KEY = "command"
                assert "hint" not in query_op[CMD_QUERY_KEY]
                assert query_op[CMD_QUERY_KEY]["comment"] == comment
                assert "collation" not in query_op[CMD_QUERY_KEY]

        async with db_ops_tracker() as q:
            _ = await AggPerson.objects.hint(index_name).update_one(name="something")
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
            CMD_QUERY_KEY = "command"

            assert query_op[CMD_QUERY_KEY]["hint"] == {"$hint": index_name}
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with db_ops_tracker() as q:
            _ = await AggPerson.objects.collation(base).update_one(name="something")
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["collation"] == base

    async def test_delete(self):
        """Ensure that document may be deleted using the delete method."""
        person = self.Person(name="Test User", age=30)
        await person.save()
        assert await self.Person.objects.count() == 1
        await person.delete()
        assert await self.Person.objects.count() == 0

    async def test_delete_propagates_hint_collation_and_comment(self):
        """Make sure adding a hint/comment/collation to the query gets added to the query"""

        base = {"locale": "en", "strength": 2}
        index_name = "name_1"

        class AggPerson(Document):
            name = StringField()
            meta = {"indexes": [{"fields": ["name"], "name": index_name, "collation": base}]}

        await AggPerson.drop_collection()
        _ = await AggPerson.objects.first()

        comment = "test_comment"

        if PYMONGO_VERSION >= (4, 1):
            async with db_ops_tracker() as q:
                _ = await AggPerson.objects().comment(comment).delete()
                query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
                CMD_QUERY_KEY = "command"
                assert "hint" not in query_op[CMD_QUERY_KEY]
                assert query_op[CMD_QUERY_KEY]["comment"] == comment
                assert "collation" not in query_op[CMD_QUERY_KEY]

        async with db_ops_tracker() as q:
            _ = await AggPerson.objects.hint(index_name).delete()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
            CMD_QUERY_KEY = "command"

            assert query_op[CMD_QUERY_KEY]["hint"] == {"$hint": index_name}
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with db_ops_tracker() as q:
            _ = await AggPerson.objects.collation(base).delete()
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(None))[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["collation"] == base

    async def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id."""

        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30, id="497ce96f395f2f052a494fd4")
        await person.save()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = await collection.find_one({"name": "Test User"})
        assert str(person_obj["_id"]) == "497ce96f395f2f052a494fd4"

    async def test_save_custom_pk(self):
        """Ensure that a document may be saved with a custom _id using
        pk alias.
        """
        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30, pk="497ce96f395f2f052a494fd4")
        await person.save()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = await collection.find_one({"name": "Test User"})
        assert str(person_obj["_id"]) == "497ce96f395f2f052a494fd4"

    async def test_save_list(self):
        """Ensure that a list field may be properly saved."""

        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = BlogPost(content="Went for a walk today...")
        post.tags = tags = ["fun", "leisure"]
        comments = [Comment(content="Good for you"), Comment(content="Yay.")]
        post.comments = comments
        await post.save()

        collection = self.db[BlogPost._get_collection_name()]
        post_obj = await collection.find_one()
        assert post_obj["tags"] == tags
        for comment_obj, comment in zip(post_obj["comments"], comments):
            assert comment_obj["content"] == comment["content"]

    async def test_list_search_by_embedded(self):
        class User(Document):
            username = StringField(required=True)

            meta = {"allow_inheritance": False}

        class Comment(EmbeddedDocument):
            comment = StringField()
            user = ReferenceField(User, required=True)

            meta = {"allow_inheritance": False}

        class Page(Document):
            comments = ListField(EmbeddedDocumentField(Comment))
            meta = {
                "allow_inheritance": False,
                "indexes": [{"fields": ["comments.user"]}],
            }

        await User.drop_collection()
        await Page.drop_collection()

        u1 = User(username="wilson")
        await u1.save()

        u2 = User(username="rozza")
        await u2.save()

        u3 = User(username="hmarr")
        await u3.save()

        p1 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
                Comment(user=u3, comment="Ping Pong"),
                Comment(user=u1, comment="I like a beer"),
            ]
        )
        await p1.save()

        p2 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
            ]
        )
        await p2.save()

        p3 = Page(comments=[Comment(user=u3, comment="Its very good")])
        await p3.save()

        p4 = Page(comments=[Comment(user=u2, comment="Heavy Metal song")])
        await p4.save()

        assert [p1, p2] == [doc async for doc in Page.objects.filter(comments__user=u1)]
        assert [p1, p2, p4] == [doc async for doc in Page.objects.filter(comments__user=u2)]
        assert [p1, p3] == [doc async for doc in Page.objects.filter(comments__user=u3)]

    async def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.save()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        employee_obj = await collection.find_one({"name": "Test Employee"})
        assert employee_obj["name"] == "Test Employee"
        assert employee_obj["age"] == 50

        # Ensure that the 'details' embedded object saved correctly
        assert employee_obj["details"]["position"] == "Developer"

    async def test_embedded_update_after_save(self):
        """Test update of `EmbeddedDocumentField` attached to a newly
        saved document.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.save()

        # Update
        site.page.log_message = "Error: Dummy message"
        await site.save()

        site = await Site.objects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.save()

        # Test updating an embedded document
        promoted_employee = await Employee.objects.get(name="Test Employee")
        promoted_employee.details.position = "Senior Developer"
        await promoted_employee.save()

        await promoted_employee.reload()
        assert promoted_employee.name == "Test Employee"
        assert promoted_employee.age == 50

        # Ensure that the 'details' embedded object saved correctly
        assert promoted_employee.details.position == "Senior Developer"

        # Test removal
        promoted_employee.details = None
        await promoted_employee.save()

        await promoted_employee.reload()
        assert promoted_employee.details is None

    async def test_object_mixins(self):
        class NameMixin:
            name = StringField()

        class Foo(EmbeddedDocument, NameMixin):
            quantity = IntField()

        assert ["name", "quantity"] == sorted(Foo._fields.keys())

        class Bar(Document, NameMixin):
            widgets = StringField()

        assert ["id", "name", "widgets"] == sorted(Bar._fields.keys())

    async def test_mixin_inheritance(self):
        class BaseMixIn:
            count = IntField()
            data = StringField()

        class DoubleMixIn(BaseMixIn):
            comment = StringField()

        class TestDoc(Document, DoubleMixIn):
            age = IntField()

        await TestDoc.drop_collection()
        t = TestDoc(count=12, data="test", comment="great!", age=19)

        await t.save()

        t = await TestDoc.objects.first()

        assert t.age == 19
        assert t.comment == "great!"
        assert t.data == "test"
        assert t.count == 12

    async def test_save_reference(self):
        """Ensure that a document reference field may be saved in the
        database.
        """

        class BlogPost(Document):
            meta = {"collection": "blogpost_1"}
            content = StringField()
            author = ReferenceField(self.Person)

        await BlogPost.drop_collection()

        author = self.Person(name="Test User")
        await author.save()

        post = BlogPost(content="Watched some TV today... how exciting.")
        # Should only reference author when saving
        post.author = author
        await post.save()

        post_obj = await BlogPost.objects.first()

        # In async mongoengine, references are stored as DBRefs and not auto-dereferenced
        assert isinstance(post_obj._data["author"], bson.DBRef)

        # Use select_related to dereference
        posts = await BlogPost.objects.select_related()
        post_obj = posts[0]
        assert isinstance(post_obj.author, self.Person)
        assert post_obj.author.name == "Test User"

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        await post_obj.author.save()

        author = [doc async for doc in self.Person.objects(name="Test User")][-1]
        assert author.age == 25

    async def test_duplicate_db_fields_raise_invalid_document_error(self):
        """Ensure a InvalidDocumentError is thrown if duplicate fields
        declare the same db_field.
        """
        with pytest.raises(InvalidDocumentError):

            class Foo(Document):
                name = StringField()
                name2 = StringField(db_field="name")

    async def test_invalid_son(self):
        """Raise an error if loading invalid data."""

        class Occurrence(EmbeddedDocument):
            number = IntField()

        class Word(Document):
            stem = StringField()
            count = IntField(default=1)
            forms = ListField(StringField(), default=list)
            occurs = ListField(EmbeddedDocumentField(Occurrence), default=list)

        with pytest.raises(InvalidDocumentError):
            Word._from_son(
                {
                    "stem": [1, 2, 3],
                    "forms": 1,
                    "count": "one",
                    "occurs": {"hello": None},
                }
            )

        # Tests for issue #1438: https://github.com/MongoEngine/mongoengine/issues/1438
        with pytest.raises(ValueError):
            Word._from_son("this is not a valid SON dict")

    async def test_reverse_delete_rule_cascade_and_nullify(self):
        """Ensure that a referenced document is also deleted upon
        deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        await self.Person.drop_collection()
        await BlogPost.drop_collection()

        author = self.Person(name="Test User")
        await author.save()

        reviewer = self.Person(name="Re Viewer")
        await reviewer.save()

        post = BlogPost(content="Watched some TV")
        post.author = author
        post.reviewer = reviewer
        await post.save()

        await reviewer.delete()
        # No effect on the BlogPost
        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.get()).reviewer is None

        # Delete the Person, which should lead to deletion of the BlogPost, too
        await author.delete()
        assert await BlogPost.objects.count() == 0

    async def test_reverse_delete_rule_pull(self):
        """Ensure that a referenced document is also deleted with
        pull.
        """

        class Record(Document):
            name = StringField()
            children = ListField(ReferenceField("self", reverse_delete_rule=PULL))

        await Record.drop_collection()

        parent_record = await Record(name="parent").save()
        child_record = await Record(name="child").save()
        parent_record.children.append(child_record)
        await parent_record.save()

        await child_record.delete()
        assert (await Record.objects(name="parent").get()).children == []

    async def test_reverse_delete_rule_with_custom_id_field(self):
        """Ensure that a referenced document with custom primary key
        is also deleted upon deletion.
        """

        class User(Document):
            name = StringField(primary_key=True)

        class Book(Document):
            author = ReferenceField(User, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(User, reverse_delete_rule=NULLIFY)

        await User.drop_collection()
        await Book.drop_collection()

        user = await User(name="Mike").save()
        reviewer = await User(name="John").save()
        _ = await Book(author=user, reviewer=reviewer).save()

        await reviewer.delete()
        assert await Book.objects.count() == 1
        assert (await Book.objects.get()).reviewer is None

        await user.delete()
        assert await Book.objects.count() == 0

    async def test_reverse_delete_rule_with_shared_id_among_collections(self):
        """Ensure that cascade delete rule doesn't mix id among
        collections.
        """

        class User(Document):
            id = IntField(primary_key=True)

        class Book(Document):
            id = IntField(primary_key=True)
            author = ReferenceField(User, reverse_delete_rule=CASCADE)

        await User.drop_collection()
        await Book.drop_collection()

        user_1 = await User(id=1).save()
        user_2 = await User(id=2).save()
        _ = await Book(id=1, author=user_2).save()
        book_2 = await Book(id=2, author=user_1).save()

        await user_2.delete()
        # Deleting user_2 should also delete book_1 but not book_2
        assert await Book.objects.count() == 1
        assert await Book.objects.get() == book_2

        user_3 = await User(id=3).save()
        _ = await Book(id=3, author=user_3).save()

        await user_3.delete()
        # Deleting user_3 should also delete book_3
        assert await Book.objects.count() == 1
        assert await Book.objects.get() == book_2

    async def test_reverse_delete_rule_with_document_inheritance(self):
        """Ensure that a referenced document is also deleted upon
        deletion of a child document.
        """

        class Writer(self.Person):
            pass

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        await self.Person.drop_collection()
        await BlogPost.drop_collection()

        author = Writer(name="Test User")
        await author.save()

        reviewer = Writer(name="Re Viewer")
        await reviewer.save()

        post = BlogPost(content="Watched some TV")
        post.author = author
        post.reviewer = reviewer
        await post.save()

        await reviewer.delete()
        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.get()).reviewer is None

        # Delete the Writer should lead to deletion of the BlogPost
        await author.delete()
        assert await BlogPost.objects.count() == 0

    async def test_reverse_delete_rule_cascade_and_nullify_complex_field(self):
        """Ensure that a referenced document is also deleted upon
        deletion for complex fields.
        """

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=CASCADE))
            reviewers = ListField(ReferenceField(self.Person, reverse_delete_rule=NULLIFY))

        await self.Person.drop_collection()
        await BlogPost.drop_collection()

        author = self.Person(name="Test User")
        await author.save()

        reviewer = self.Person(name="Re Viewer")
        await reviewer.save()

        post = BlogPost(content="Watched some TV")
        post.authors = [author]
        post.reviewers = [reviewer]
        await post.save()

        # Deleting the reviewer should have no effect on the BlogPost
        await reviewer.delete()
        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.get()).reviewers == []

        # Delete the Person, which should lead to deletion of the BlogPost, too
        await author.delete()
        assert await BlogPost.objects.count() == 0

    async def test_reverse_delete_rule_cascade_triggers_pre_delete_signal(self):
        """Ensure the pre_delete signal is triggered upon a cascading
        deletion setup a blog post with content, an author and editor
        delete the author which triggers deletion of blogpost via
        cascade blog post's pre_delete signal alters an editor attribute.
        """

        class Editor(self.Person):
            review_queue = IntField(default=0)

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            editor = ReferenceField(Editor)

            @classmethod
            async def pre_delete(cls, sender, document, **kwargs):
                # decrement the docs-to-review count (no auto-dereference)
                editor = await Editor.objects.get(pk=document.editor.id)
                await editor.update(dec__review_queue=1)

        signals.pre_delete_async.connect(BlogPost.pre_delete, sender=BlogPost)

        await self.Person.drop_collection()
        await BlogPost.drop_collection()
        await Editor.drop_collection()

        author = await self.Person(name="Will S.").save()
        editor = await Editor(name="Max P.", review_queue=1).save()
        await BlogPost(content="wrote some books", author=author, editor=editor).save()

        # delete the author, the post is also deleted due to the CASCADE rule
        await author.delete()

        # the pre-delete signal should have decremented the editor's queue
        editor = await Editor.objects(name="Max P.").get()
        assert editor.review_queue == 0

    async def test_two_way_reverse_delete_rule(self):
        """Ensure that Bi-Directional relationships work with
        reverse_delete_rule
        """

        class Bar(Document):
            content = StringField()
            foo = ReferenceField("Foo")

        class Foo(Document):
            content = StringField()
            bar = ReferenceField(Bar)

        Bar.register_delete_rule(Foo, "bar", NULLIFY)
        Foo.register_delete_rule(Bar, "foo", NULLIFY)

        await Bar.drop_collection()
        await Foo.drop_collection()

        b = Bar(content="Hello")
        await b.save()

        f = Foo(content="world", bar=b)
        await f.save()

        b.foo = f
        await b.save()

        await f.delete()

        assert await Bar.objects.count() == 1  # No effect on the BlogPost
        assert (await Bar.objects.get()).foo is None

    async def test_invalid_reverse_delete_rule_raise_errors(self):
        with pytest.raises(InvalidDocumentError):

            class Blog(Document):
                content = StringField()
                authors = MapField(ReferenceField(self.Person, reverse_delete_rule=CASCADE))
                reviewers = DictField(field=ReferenceField(self.Person, reverse_delete_rule=NULLIFY))

        with pytest.raises(InvalidDocumentError):

            class Parents(EmbeddedDocument):
                father = ReferenceField("Person", reverse_delete_rule=DENY)
                mother = ReferenceField("Person", reverse_delete_rule=DENY)

    async def test_reverse_delete_rule_cascade_recurs(self):
        """Ensure that a chain of documents is also deleted upon
        cascaded deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class Comment(Document):
            text = StringField()
            post = ReferenceField(BlogPost, reverse_delete_rule=CASCADE)

        await self.Person.drop_collection()
        await BlogPost.drop_collection()
        await Comment.drop_collection()

        author = self.Person(name="Test User")
        await author.save()

        post = BlogPost(content="Watched some TV")
        post.author = author
        await post.save()

        comment = Comment(text="Kudos.")
        comment.post = post
        await comment.save()

        # Delete the Person, which should lead to deletion of the BlogPost,
        # and, recursively to the Comment, too
        await author.delete()
        assert await Comment.objects.count() == 0

    async def test_reverse_delete_rule_deny(self):
        """Ensure that a document cannot be referenced if there are
        still documents referring to it.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        await self.Person.drop_collection()
        await BlogPost.drop_collection()

        author = self.Person(name="Test User")
        await author.save()

        post = BlogPost(content="Watched some TV")
        post.author = author
        await post.save()

        # Delete the Person should be denied
        with pytest.raises(OperationError):
            await author.delete()  # Should raise denied error
        assert await BlogPost.objects.count() == 1  # No objects may have been deleted
        assert await self.Person.objects.count() == 1

        # Other users, that don't have BlogPosts must be removable, like normal
        author = self.Person(name="Another User")
        await author.save()

        assert await self.Person.objects.count() == 2
        await author.delete()
        assert await self.Person.objects.count() == 1

    async def subclasses_and_unique_keys_works(self):
        class A(Document):
            pass

        class B(A):
            foo = BooleanField(unique=True)

        await A.drop_collection()
        await B.drop_collection()

        await A().save()
        await A().save()
        await B(foo=True).save()

        assert await A.objects.count() == 2
        assert await B.objects.count() == 1

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

    async def test_override_method_with_field(self):
        """Test creating a field with a field name that would override
        the "validate" method.
        """
        with pytest.raises(InvalidDocumentError):

            class Blog(Document):
                validate = DictField()

    async def test_mutating_documents(self):
        class B(EmbeddedDocument):
            field1 = StringField(default="field1")

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        await A.drop_collection()

        a = A()
        await a.save()
        await a.reload()
        assert a.b.field1 == "field1"

        class C(EmbeddedDocument):
            c_field = StringField(default="cfield")

        class B(EmbeddedDocument):
            field1 = StringField(default="field1")
            field2 = EmbeddedDocumentField(C, default=lambda: C())

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        a = await A.objects().get_item(0)
        a.b.field2.c_field = "new value"
        await a.save()

        await a.reload()
        assert a.b.field2.c_field == "new value"

    async def test_can_save_false_values(self):
        """Ensures you can save False values on save."""

        class Doc(Document):
            foo = StringField()
            archived = BooleanField(default=False, required=True)

        await Doc.drop_collection()

        d = Doc()
        await d.save()
        d.archived = False
        await d.save()

        assert await Doc.objects(archived=False).count() == 1

    async def test_can_save_false_values_dynamic(self):
        """Ensures you can save False values on dynamic docs."""

        class Doc(DynamicDocument):
            foo = StringField()

        await Doc.drop_collection()

        d = Doc()
        await d.save()
        d.archived = False
        await d.save()

        assert await Doc.objects(archived=False).count() == 1

    async def test_do_not_save_unchanged_references(self):
        """Ensures cascading saves dont auto update"""

        class Job(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = ReferenceField(Job)

        await Job.drop_collection()
        await Person.drop_collection()

        job = Job(name="Job 1")
        # job should not have any changed fields after the save
        await job.save()

        person = Person(name="name", age=10, job=job)

        from pymongo.collection import Collection

        orig_update_one = Collection.update_one
        try:

            def fake_update_one(*args, **kwargs):
                pytest.fail(f"Unexpected update for {args[0].name}")
                return orig_update_one(*args, **kwargs)

            Collection.update_one = fake_update_one
            await person.save()
        finally:
            Collection.update_one = orig_update_one

    async def test_db_alias_tests(self):
        """DB Alias tests."""
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        register_connection("testdb-1", "mongoenginetest2")
        register_connection("testdb-2", "mongoenginetest3")
        register_connection("testdb-3", "mongoenginetest4")

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            meta = {"db_alias": "testdb-2"}

        # Drops
        await User.drop_collection()
        await Book.drop_collection()

        # Create
        bob = await User.objects.create(name="Bob")
        hp = await Book.objects.create(name="Harry Potter")

        # Selects
        assert await User.objects.first() == bob
        assert await Book.objects.first() == hp

        # DeReference
        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)
            meta = {"db_alias": "testdb-3"}

        # Drops
        await AuthorBooks.drop_collection()

        ab = await AuthorBooks.objects.create(author=bob, book=hp)

        # select
        assert await AuthorBooks.objects.first() == ab
        assert (await AuthorBooks.objects.first()).book == hp
        assert (await AuthorBooks.objects.first()).author == bob
        assert await AuthorBooks.objects.filter(author=bob).first() == ab
        assert await AuthorBooks.objects.filter(book=hp).first() == ab

        # DB Alias
        assert User._get_db() == get_db("testdb-1")
        assert Book._get_db() == get_db("testdb-2")
        assert AuthorBooks._get_db() == get_db("testdb-3")

        # Collections
        assert await User._get_collection() == get_db("testdb-1")[User._get_collection_name()]
        assert await Book._get_collection() == get_db("testdb-2")[Book._get_collection_name()]
        assert await AuthorBooks._get_collection() == get_db("testdb-3")[AuthorBooks._get_collection_name()]

    async def test_db_alias_overrides(self):
        """Test db_alias can be overriden."""
        # Register a connection with db_alias testdb-2
        register_connection("testdb-2", "mongoenginetest2")

        class A(Document):
            """Uses default db_alias"""

            name = StringField()
            meta = {"allow_inheritance": True}

        class B(A):
            """Uses testdb-2 db_alias"""

            meta = {"db_alias": "testdb-2"}

        A.objects.all()

        assert "testdb-2" == B._meta.get("db_alias")
        assert "mongoenginetest" == (await A._get_collection()).database.name
        assert "mongoenginetest2" == (await B._get_collection()).database.name

    async def test_db_alias_propagates(self):
        """db_alias propagates?"""
        register_connection("testdb-1", "mongoenginetest2")

        class A(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1", "allow_inheritance": True}

        class B(A):
            pass

        assert "testdb-1" == B._meta.get("db_alias")

    async def test_db_ref_usage(self):
        """DB Ref usage in dict_fields."""

        class User(Document):
            name = StringField()

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)
            extra = DictField()
            meta = {"ordering": ["+name"]}

            def __unicode__(self):
                return self.name

            def __str__(self):
                return self.name

        # Drops
        await User.drop_collection()
        await Book.drop_collection()

        # Authors
        bob = await User.objects.create(name="Bob")
        jon = await User.objects.create(name="Jon")

        # Redactors
        karl = await User.objects.create(name="Karl")
        susan = await User.objects.create(name="Susan")
        peter = await User.objects.create(name="Peter")

        # Bob
        await Book.objects.create(
            name="1",
            author=bob,
            extra={"a": bob.to_dbref(), "b": [karl.to_dbref(), susan.to_dbref()]},
        )
        await Book.objects.create(name="2", author=bob, extra={"a": bob.to_dbref(), "b": karl.to_dbref()})
        await Book.objects.create(
            name="3",
            author=bob,
            extra={"a": bob.to_dbref(), "c": [jon.to_dbref(), peter.to_dbref()]},
        )
        await Book.objects.create(name="4", author=bob)

        # Jon
        await Book.objects.create(name="5", author=jon)
        await Book.objects.create(name="6", author=peter)
        await Book.objects.create(name="7", author=jon)
        await Book.objects.create(name="8", author=jon)
        await Book.objects.create(name="9", author=jon, extra={"a": peter.to_dbref()})

        # Checks
        assert ",".join([str(b) async for b in Book.objects.all()]) == "1,2,3,4,5,6,7,8,9"
        # bob related books
        bob_books_qs = Book.objects.filter(Q(extra__a=bob) | Q(author=bob) | Q(extra__b=bob))
        assert [str(b) async for b in bob_books_qs] == ["1", "2", "3", "4"]
        assert await bob_books_qs.count() == 4

        # Susan & Karl related books
        susan_karl_books_qs = Book.objects.filter(
            Q(extra__a__all=[karl, susan])
            | Q(author__all=[karl, susan])
            | Q(extra__b__all=[karl.to_dbref(), susan.to_dbref()])
        )
        assert [str(b) async for b in susan_karl_books_qs] == ["1"]
        assert await susan_karl_books_qs.count() == 1

        # $Where
        custom_qs = Book.objects.filter(
            __raw__={
                "$where": """
                                            function(){
                                                return this.name == '1' ||
                                                       this.name == '2';}"""
            }
        )
        assert [str(b) async for b in custom_qs] == ["1", "2"]

        # count only will work with this raw query before pymongo 4.x, but
        # the length is also implicitly checked above
        if PYMONGO_VERSION < (4,):
            assert custom_qs.count() == 2

    @pytest.mark.skip(reason="switch_db + update interaction needs investigation")
    async def test_switch_db_instance(self):
        register_connection("testdb-1", "mongoenginetest2")

        class Group(Document):
            name = StringField()

        await Group.drop_collection()
        async with switch_db(Group, "testdb-1") as Group:
            await Group.drop_collection()

        await Group(name="hello - default").save()
        assert 1 == await Group.objects.count()

        group = await Group.objects.first()
        await group.switch_db("testdb-1")
        group.name = "hello - testdb!"
        await group.save()

        async with switch_db(Group, "testdb-1") as Group:
            group = await Group.objects.first()
            assert "hello - testdb!" == group.name

        group = await Group.objects.first()
        assert "hello - default" == group.name

        # Slightly contrived now - perform an update
        # Only works as they have the same object_id
        await group.switch_db("testdb-1")
        await group.update(set__name="hello - update")

        async with switch_db(Group, "testdb-1") as Group:
            group = await Group.objects.first()
            assert "hello - update" == group.name
            await Group.drop_collection()
            assert 0 == await Group.objects.count()

        group = await Group.objects.first()
        assert "hello - default" == group.name

        # Totally contrived now - perform a delete
        # Only works as they have the same object_id
        await group.switch_db("testdb-1")
        await group.delete()

        async with switch_db(Group, "testdb-1") as Group:
            assert 0 == await Group.objects.count()

        group = await Group.objects.first()
        assert "hello - default" == group.name

    async def test_load_undefined_fields(self):
        class User(Document):
            name = StringField()

        await User.drop_collection()

        await (await User._get_collection()).insert_one({"name": "John", "foo": "Bar", "data": [1, 2, 3]})

        with pytest.raises(FieldDoesNotExist):
            await User.objects.first()

    async def test_load_undefined_fields_with_strict_false(self):
        class User(Document):
            name = StringField()

            meta = {"strict": False}

        await User.drop_collection()

        await (await User._get_collection()).insert_one({"name": "John", "foo": "Bar", "data": [1, 2, 3]})

        user = await User.objects.first()
        assert user.name == "John"
        assert not hasattr(user, "foo")
        assert user._data["foo"] == "Bar"
        assert not hasattr(user, "data")
        assert user._data["data"] == [1, 2, 3]

    async def test_load_undefined_fields_on_embedded_document(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        await User.drop_collection()

        await (await User._get_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        with pytest.raises(FieldDoesNotExist):
            await User.objects.first()

    async def test_load_undefined_fields_on_embedded_document_with_strict_false_on_doc(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

            meta = {"strict": False}

        await User.drop_collection()

        await (await User._get_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        with pytest.raises(FieldDoesNotExist):
            await User.objects.first()

    async def test_load_undefined_fields_on_embedded_document_with_strict_false(self):
        class Thing(EmbeddedDocument):
            name = StringField()

            meta = {"strict": False}

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        await User.drop_collection()

        await (await User._get_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        user = await User.objects.first()
        assert user.name == "John"
        assert user.thing.name == "My thing"
        assert not hasattr(user.thing, "foo")
        assert user.thing._data["foo"] == "Bar"
        assert not hasattr(user.thing, "data")
        assert user.thing._data["data"] == [1, 2, 3]

    async def test_spaces_in_keys(self):
        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        await Doc.drop_collection()
        doc = Doc()
        setattr(doc, "hello world", 1)
        await doc.save()

        one = await Doc.objects.filter(**{"hello world": 1}).count()
        assert 1 == one

    async def test_shard_key(self):
        class LogEntry(Document):
            machine = StringField()
            log = StringField()

            meta = {"shard_key": ("machine",)}

        await LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        await log.save()

        assert log.id is not None

        log.log = "Saving"
        await log.save()

        # try to change the shard key
        with pytest.raises(OperationError):
            log.machine = "127.0.0.1"

    async def test_shard_key_in_embedded_document(self):
        class Foo(EmbeddedDocument):
            foo = StringField()

        class Bar(Document):
            meta = {"shard_key": ("foo.foo",)}
            foo = EmbeddedDocumentField(Foo)
            bar = StringField()

        foo_doc = Foo(foo="hello")
        bar_doc = Bar(foo=foo_doc, bar="world")
        await bar_doc.save()

        assert bar_doc.id is not None

        bar_doc.bar = "baz"
        await bar_doc.save()

        # try to change the shard key
        with pytest.raises(OperationError):
            bar_doc.foo.foo = "something"
            await bar_doc.save()

    async def test_shard_key_primary(self):
        class LogEntry(Document):
            machine = StringField(primary_key=True)
            log = StringField()

            meta = {"shard_key": ("machine",)}

        await LogEntry.drop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        await log.save()

        assert log.id is not None

        log.log = "Saving"
        await log.save()

        # try to change the shard key
        with pytest.raises(OperationError):
            log.machine = "127.0.0.1"

    async def test_kwargs_simple(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            doc = EmbeddedDocumentField(Embedded)

            def __eq__(self, other):
                return self.doc_name == other.doc_name and self.doc == other.doc

        classic_doc = Doc(doc_name="my doc", doc=Embedded(name="embedded doc"))
        dict_doc = Doc(**{"doc_name": "my doc", "doc": {"name": "embedded doc"}})

        assert classic_doc == dict_doc
        assert classic_doc._data == dict_doc._data

    async def test_kwargs_complex(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            docs = ListField(EmbeddedDocumentField(Embedded))

            def __eq__(self, other):
                return self.doc_name == other.doc_name and self.docs == other.docs

        classic_doc = Doc(
            doc_name="my doc",
            docs=[Embedded(name="embedded doc1"), Embedded(name="embedded doc2")],
        )
        dict_doc = Doc(
            **{
                "doc_name": "my doc",
                "docs": [{"name": "embedded doc1"}, {"name": "embedded doc2"}],
            }
        )

        assert classic_doc == dict_doc
        assert classic_doc._data == dict_doc._data

    async def test_positional_creation(self):
        """Document cannot be instantiated using positional arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Person("Test User", 42)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    async def test_mixed_creation(self):
        """Document cannot be instantiated using mixed arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Person("Test User", age=42)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    async def test_positional_creation_embedded(self):
        """Embedded document cannot be created using positional arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Job("Test Job", 4)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    async def test_mixed_creation_embedded(self):
        """Embedded document cannot be created using mixed arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Job("Test Job", years=4)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    async def test_data_contains_id_field(self):
        """Ensure that asking for _data returns 'id'."""

        class Person(Document):
            name = StringField()

        await Person.drop_collection()
        await Person(name="Harry Potter").save()

        person = await Person.objects.first()
        assert "id" in person._data.keys()
        assert person._data.get("id") == person.id

    @pytest.mark.skip(reason="Requires auto-dereference which is removed in async-mongoengine")
    async def test_complex_nesting_document_and_embedded_document(self):
        class Macro(EmbeddedDocument):
            value = DynamicField(default="UNDEFINED")

        class Parameter(EmbeddedDocument):
            macros = MapField(EmbeddedDocumentField(Macro))

            def expand(self):
                self.macros["test"] = Macro()

        class Node(Document):
            parameters = MapField(EmbeddedDocumentField(Parameter))

            def expand(self):
                self.flattened_parameter = {}
                for parameter_name, parameter in self.parameters.items():
                    parameter.expand()

        class NodesSystem(Document):
            name = StringField(required=True)
            nodes = MapField(ReferenceField(Node, dbref=False))

            async def save(self, *args, **kwargs):
                for node_name, node in self.nodes.items():
                    node.expand()
                    await node.save(*args, **kwargs)
                await super().save(*args, **kwargs)

        await NodesSystem.drop_collection()
        await Node.drop_collection()

        system = NodesSystem(name="system")
        system.nodes["node"] = Node()
        await system.save()
        system.nodes["node"].parameters["param"] = Parameter()
        await system.save()

        system = await NodesSystem.objects.first()
        assert "UNDEFINED" == system.nodes["node"].parameters["param"].macros["test"].value

    async def test_embedded_document_equality(self):
        class Test(Document):
            field = StringField(required=True)

        class Embedded(EmbeddedDocument):
            ref = ReferenceField(Test)

        await Test.drop_collection()
        test = await Test(field="123").save()  # has id

        e = Embedded(ref=test)
        f1 = Embedded._from_son(e.to_mongo())
        f2 = Embedded._from_son(e.to_mongo())

        assert f1 == f2
        f1.ref  # Dereferences lazily
        assert f1 == f2

    async def test_embedded_document_equality_with_lazy_ref(self):
        class Job(EmbeddedDocument):
            boss = LazyReferenceField("Person")
            boss_dbref = LazyReferenceField("Person", dbref=True)

        class Person(Document):
            job = EmbeddedDocumentField(Job)

        await Person.drop_collection()

        boss = Person()
        worker = Person(job=Job(boss=boss, boss_dbref=boss))
        await boss.save()
        await worker.save()

        worker1 = await Person.objects.get(id=worker.id)

        # worker1.job should be equal to the job used originally to create the
        # document.
        assert worker1.job == worker.job

        # worker1.job should be equal to a newly created Job EmbeddedDocument
        # using either the Boss object or his ID.
        assert worker1.job == Job(boss=boss, boss_dbref=boss)
        assert worker1.job == Job(boss=boss.id, boss_dbref=boss.id)

        # The above equalities should also hold after worker1.job.boss has been
        # fetch()ed.
        await worker1.job.boss.fetch()
        assert worker1.job == worker.job
        assert worker1.job == Job(boss=boss, boss_dbref=boss)
        assert worker1.job == Job(boss=boss.id, boss_dbref=boss.id)

    async def test_dbref_equality(self):
        class Test2(Document):
            name = StringField()

        class Test3(Document):
            name = StringField()

        class Test(Document):
            name = StringField()
            test2 = ReferenceField("Test2")
            test3 = ReferenceField("Test3")

        await Test.drop_collection()
        await Test2.drop_collection()
        await Test3.drop_collection()

        t2 = Test2(name="a")
        await t2.save()

        t3 = Test3(name="x")
        t3.id = t2.id
        await t3.save()

        t = Test(name="b", test2=t2, test3=t3)

        f = Test._from_son(t.to_mongo())

        dbref2 = f._data["test2"]
        ref2 = f.test2
        assert isinstance(dbref2, DBRef)
        # No auto-dereference in async: f.test2 returns DBRef
        assert isinstance(ref2, DBRef)
        assert ref2.id == dbref2.id

        dbref3 = f._data["test3"]
        ref3 = f.test3
        assert isinstance(dbref3, DBRef)
        assert isinstance(ref3, DBRef)
        assert ref3.id == dbref3.id
        assert dbref3 == ref3

        assert ref2.id == ref3.id
        assert dbref2.id == dbref3.id
        assert dbref2 != dbref3
        assert dbref3 != dbref2
        assert dbref2 != dbref3
        assert dbref3 != dbref2

        assert ref2 != dbref3
        assert dbref3 != ref2
        assert ref2 != dbref3
        assert dbref3 != ref2

        assert ref3 != dbref2
        assert dbref2 != ref3
        assert ref3 != dbref2
        assert dbref2 != ref3

    async def test_default_values_dont_get_override_upon_save_when_only_is_used(self):
        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()

        p = Person(name="alon")
        await p.save()
        orig_created_on = (await Person.objects().only("created_on").get_item(0)).created_on

        p2 = await Person.objects().only("name").get_item(0)
        p2.name = "alon2"
        await p2.save()
        p3 = await Person.objects().only("created_on").get_item(0)
        assert orig_created_on == p3.created_on

        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()
            height = IntField(default=189)

        p4 = await Person.objects().get_item(0)
        await p4.save()
        assert p4.height == 189

        # However the default will not be fixed in DB
        assert await Person.objects(height=189).count() == 0

        # alter DB for the new default
        coll = await Person._get_collection()
        async for person in Person.objects.as_pymongo():
            if "height" not in person:
                await coll.update_one({"_id": person["_id"]}, {"$set": {"height": 189}})

        assert await Person.objects(height=189).count() == 1

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

    async def test_null_field(self):
        # 734
        class User(Document):
            name = StringField()
            height = IntField(default=184, null=True)
            str_fld = StringField(null=True)
            int_fld = IntField(null=True)
            flt_fld = FloatField(null=True)
            dt_fld = DateTimeField(null=True)
            cdt_fld = ComplexDateTimeField(null=True)

        await User.objects.delete()
        u = await User(name="user").save()
        u_from_db = await User.objects.get(name="user")
        u_from_db.height = None
        await u_from_db.save()
        assert u_from_db.height is None
        # 864
        assert u_from_db.str_fld is None
        assert u_from_db.int_fld is None
        assert u_from_db.flt_fld is None
        assert u_from_db.dt_fld is None
        assert u_from_db.cdt_fld is None

        # 735
        await User.objects.delete()
        u = User(name="user")
        await u.save()
        await User.objects(name="user").update_one(set__height=None, upsert=True)
        u_from_db = await User.objects.get(name="user")
        assert u_from_db.height is None

    async def test_not_saved_eq(self):
        """Ensure we can compare documents not saved."""

        class Person(Document):
            pass

        p = Person()
        p1 = Person()
        assert p != p1
        assert p == p

    async def test_list_iter(self):
        # 914
        class B(EmbeddedDocument):
            v = StringField()

        class A(Document):
            array = ListField(EmbeddedDocumentField(B))

        await A.objects.delete()
        await A(array=[B(v="1"), B(v="2"), B(v="3")]).save()
        a = await A.objects.get()
        assert a.array._instance == a
        for idx, b in enumerate(a.array):
            assert b._instance == a
        assert idx == 2

    async def test_updating_listfield_manipulate_list(self):
        class Company(Document):
            name = StringField()
            employees = ListField(field=DictField())

        await Company.drop_collection()

        comp = Company(name="BigBank", employees=[{"name": "John"}])
        await comp.save()
        comp.employees.append({"name": "Bill"})
        await comp.save()

        stored_comp = await get_as_pymongo(comp)
        assert stored_comp == {
            "_id": comp.id,
            "employees": [{"name": "John"}, {"name": "Bill"}],
            "name": "BigBank",
        }

        comp = await comp.reload()
        comp.employees[0]["color"] = "red"
        comp.employees[-1]["color"] = "blue"
        comp.employees[-1].update({"size": "xl"})
        await comp.save()

        assert len(comp.employees) == 2
        assert comp.employees[0] == {"name": "John", "color": "red"}
        assert comp.employees[1] == {"name": "Bill", "size": "xl", "color": "blue"}

        stored_comp = await get_as_pymongo(comp)
        assert stored_comp == {
            "_id": comp.id,
            "employees": [
                {"name": "John", "color": "red"},
                {"size": "xl", "color": "blue", "name": "Bill"},
            ],
            "name": "BigBank",
        }

    async def test_falsey_pk(self):
        """Ensure that we can create and update a document with Falsey PK."""

        class Person(Document):
            age = IntField(primary_key=True)
            height = FloatField()

        person = Person()
        person.age = 0
        person.height = 1.89
        await person.save()

        await person.update(set__height=2.0)

    async def test_push_with_position(self):
        """Ensure that push with position works properly for an instance."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        blog = BlogPost()
        blog.slug = "ABC"
        blog.tags = ["python"]
        await blog.save()

        await blog.update(push__tags__0=["mongodb", "code"])
        await blog.reload()
        assert blog.tags == ["mongodb", "code", "python"]

    async def test_push_nested_list(self):
        """Ensure that push update works in nested list"""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        blog = await BlogPost(slug="test").save()
        await blog.update(push__tags=["value1", 123])
        await blog.reload()
        assert blog.tags == [["value1", 123]]

    async def test_accessing_objects_with_indexes_error(self):
        insert_result = await self.db.company.insert_many(
            [{"name": "Foo"}, {"name": "Foo"}]
        )  # Force 2 doc with same name
        REF_OID = insert_result.inserted_ids[0]
        await self.db.user.insert_one({"company": REF_OID})  # Force 2 doc with same name

        class Company(Document):
            name = StringField(unique=True)

        class User(Document):
            company = ReferenceField(Company)

        # Ensure index creation exception aren't swallowed (#1688)
        with pytest.raises(DuplicateKeyError):
            await User.objects().select_related()

    async def test_deepcopy(self):
        regex_field = StringField(regex=r"(^ABC\d\d\d\d$)")
        no_regex_field = StringField()
        # Copy copied field object
        copy.deepcopy(copy.deepcopy(regex_field))
        copy.deepcopy(copy.deepcopy(no_regex_field))
        # Copy same field object multiple times to make sure we restore __deepcopy__ correctly
        copy.deepcopy(regex_field)
        copy.deepcopy(regex_field)
        copy.deepcopy(no_regex_field)
        copy.deepcopy(no_regex_field)

    async def test_deepcopy_with_reference_itself(self):
        class User(Document):
            name = StringField(regex=r"(.*)")
            other_user = ReferenceField("self")

        user1 = await User(name="John").save()
        await User(name="Bob", other_user=user1).save()

        user1.other_user = user1
        await user1.save()
        async for u in User.objects:
            copied_u = copy.deepcopy(u)
            assert copied_u is not u
            assert copied_u._fields["name"] is u._fields["name"]
            assert copied_u._fields["name"].regex is u._fields["name"].regex  # Compiled regex objects are atomic

    async def test_from_son_with_auto_dereference_disabled(self):
        class User(Document):
            name = StringField(regex=r"(^ABC\d\d\d\d$)")

        data = {"name": "ABC0000"}
        user_obj = User._from_son(son=data, _auto_dereference=False)

        assert user_obj._fields["name"] is not User.name
        assert user_obj._fields["name"].regex is User.name.regex  # Compiled regex are atomic
        copied_user = copy.deepcopy(user_obj)
        assert user_obj._fields["name"] is not copied_user._fields["name"]
        assert user_obj._fields["name"].regex is copied_user._fields["name"].regex  # Compiled regex are atomic

    async def test_embedded_document_failed_while_loading_instance_when_it_is_not_a_dict(
        self,
    ):
        class LightSaber(EmbeddedDocument):
            color = StringField()

        class Jedi(Document):
            light_saber = EmbeddedDocumentField(LightSaber)

        coll = await Jedi._get_collection()
        await Jedi(light_saber=LightSaber(color="red")).save()
        _ = [doc async for doc in Jedi.objects]  # Ensure a proper document loads without errors

        # Forces a document with a wrong shape (may occur in case of migration)
        value = "I_should_be_a_dict"
        await coll.insert_one({"light_saber": value})

        with pytest.raises(InvalidDocumentError) as exc_info:
            [doc async for doc in Jedi.objects]

        assert (
            str(exc_info.value)
            == f"Invalid data to create a `Jedi` instance.\nField 'light_saber' - The source SON object needs to be of type 'dict' but a '{type(value)}' was found"
        )


class ObjectKeyTestCase(MongoDBTestCase):
    async def test_object_key_simple_document(self):
        class Book(Document):
            title = StringField()

        book = Book(title="Whatever")
        assert book._object_key == {"pk": None}

        book.pk = ObjectId()
        assert book._object_key == {"pk": book.pk}

    async def test_object_key_with_custom_primary_key(self):
        class Book(Document):
            isbn = StringField(primary_key=True)
            title = StringField()

        book = Book(title="Sapiens")
        assert book._object_key == {"pk": None}

        book = Book(pk="0062316117")
        assert book._object_key == {"pk": "0062316117"}

    async def test_object_key_in_a_sharded_collection(self):
        class Book(Document):
            title = StringField()
            meta = {"shard_key": ("pk", "title")}

        book = Book()
        assert book._object_key == {"pk": None, "title": None}
        book = Book(pk=ObjectId(), title="Sapiens")
        assert book._object_key == {"pk": book.pk, "title": "Sapiens"}

    async def test_object_key_with_custom_db_field(self):
        class Book(Document):
            author = StringField(db_field="creator")
            meta = {"shard_key": ("pk", "author")}

        book = Book(pk=ObjectId(), author="Author")
        assert book._object_key == {"pk": book.pk, "author": "Author"}

    async def test_object_key_with_nested_shard_key(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Book(Document):
            author = EmbeddedDocumentField(Author)
            meta = {"shard_key": ("pk", "author.name")}

        book = Book(pk=ObjectId(), author=Author(name="Author"))
        assert book._object_key == {"pk": book.pk, "author__name": "Author"}


class DBFieldMappingTest(MongoDBTestCase):
    def setup_method(self, method=None):
        class Fields:
            w1 = BooleanField(db_field="w2")

            x1 = BooleanField(db_field="x2")
            x2 = BooleanField(db_field="x3")

            y1 = BooleanField(db_field="y0")
            y2 = BooleanField(db_field="y1")

            z1 = BooleanField(db_field="z2")
            z2 = BooleanField(db_field="z1")

        class Doc(Fields, Document):
            pass

        class DynDoc(Fields, DynamicDocument):
            pass

        self.Doc = Doc
        self.DynDoc = DynDoc

    async def test_setting_fields_in_constructor_of_strict_doc_uses_model_names(self):
        doc = self.Doc(z1=True, z2=False)
        assert doc.z1 is True
        assert doc.z2 is False

    async def test_setting_fields_in_constructor_of_dyn_doc_uses_model_names(self):
        doc = self.DynDoc(z1=True, z2=False)
        assert doc.z1 is True
        assert doc.z2 is False

    async def test_setting_unknown_field_in_constructor_of_dyn_doc_does_not_overwrite_model_fields(
        self,
    ):
        doc = self.DynDoc(w2=True)
        assert doc.w1 is None
        assert doc.w2 is True

    async def test_unknown_fields_of_strict_doc_do_not_overwrite_dbfields_1(self):
        doc = self.Doc()
        doc.w2 = True
        doc.x3 = True
        doc.y0 = True
        await doc.save()
        reloaded = await self.Doc.objects.get(id=doc.id)
        assert reloaded.w1 is None
        assert reloaded.x1 is None
        assert reloaded.x2 is None
        assert reloaded.y1 is None
        assert reloaded.y2 is None

    async def test_dbfields_are_loaded_to_the_right_modelfield_for_strict_doc_2(self):
        doc = self.Doc()
        doc.x2 = True
        doc.y2 = True
        doc.z2 = True
        await doc.save()
        reloaded = await self.Doc.objects.get(id=doc.id)
        assert (
            reloaded.x1,
            reloaded.x2,
            reloaded.y1,
            reloaded.y2,
            reloaded.z1,
            reloaded.z2,
        ) == (doc.x1, doc.x2, doc.y1, doc.y2, doc.z1, doc.z2)

    async def test_dbfields_are_loaded_to_the_right_modelfield_for_dyn_doc_2(self):
        doc = self.DynDoc()
        doc.x2 = True
        doc.y2 = True
        doc.z2 = True
        await doc.save()
        reloaded = await self.DynDoc.objects.get(id=doc.id)
        assert (
            reloaded.x1,
            reloaded.x2,
            reloaded.y1,
            reloaded.y2,
            reloaded.z1,
            reloaded.z2,
        ) == (doc.x1, doc.x2, doc.y1, doc.y2, doc.z1, doc.z2)
