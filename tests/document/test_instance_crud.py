import uuid
from datetime import datetime

import bson
import pytest
from bson import ObjectId

from mongoengine import *
from mongoengine.context_managers import query_counter
from mongoengine.errors import (
    InvalidDocumentError,
    InvalidQueryError,
    NotUniqueError,
)
from mongoengine.pymongo_support import PYMONGO_VERSION
from tests.utils import MongoDBTestCase, db_ops_tracker


class TestInstanceCrud(MongoDBTestCase):
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

    # ---- reload operations ----

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

    # ---- modify operations ----

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

    # ---- update operations ----

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

    # ---- delete operations ----

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
