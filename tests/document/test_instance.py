import copy
from datetime import datetime

import bson
import pytest
from bson import DBRef, ObjectId
from pymongo.errors import DuplicateKeyError

from mongoengine import *
from mongoengine import signals
from mongoengine.base import _DocumentRegistry
from mongoengine.connection import get_db
from mongoengine.context_managers import switch_db
from mongoengine.errors import (
    FieldDoesNotExist,
    InvalidDocumentError,
    NotRegistered,
)
from mongoengine.pymongo_support import PYMONGO_VERSION
from mongoengine.queryset import Q
from tests.utils import (
    MongoDBTestCase,
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
