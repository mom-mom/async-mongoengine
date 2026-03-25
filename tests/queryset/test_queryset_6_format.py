import datetime
import uuid
from decimal import Decimal

import pytest
from bson import ObjectId

from mongoengine import *
from mongoengine.context_managers import query_counter
from mongoengine.queryset import (
    QuerySet,
)
from tests.utils import (
    MongoDBTestCase,
)


class TestQueryset6(MongoDBTestCase):
    def setup_method(self, method=None):
        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {"allow_inheritance": True}

        self.PersonMeta = PersonMeta
        self.Person = Person

    async def assertSequence(self, qs, expected):
        qs = [d async for d in qs]
        expected = list(expected)
        assert len(qs) == len(expected)
        for i in range(len(qs)):
            assert qs[i] == expected[i]

    async def tearDown(self):
        await self.Person.drop_collection()

    async def test_json_simple(self):
        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            string = StringField()
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.drop_collection()
        await Doc(string="Hi", embedded_field=Embedded(string="Hi")).save()
        await Doc(string="Bye", embedded_field=Embedded(string="Bye")).save()

        await Doc().save()
        json_data = await Doc.objects.to_json(sort_keys=True, separators=(",", ":"))
        doc_objects = [d async for d in Doc.objects]

        assert doc_objects == Doc.objects.from_json(json_data)

    async def test_json_complex(self):
        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        class Doc(Document):
            string_field = StringField(default="1")
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.datetime.now)
            embedded_document_field = EmbeddedDocumentField(EmbeddedDoc, default=lambda: EmbeddedDoc())
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=ObjectId)
            reference_field = ReferenceField(Simple)
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField()
            sorted_list_field = SortedListField(IntField(), default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(binary=False, default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(default=lambda: EmbeddedDoc())

        await Simple.drop_collection()
        await Doc.drop_collection()

        simple_ref = await Simple().save()
        doc = Doc()
        doc.reference_field = simple_ref
        doc.generic_reference_field = simple_ref
        await doc.save()
        json_data = await Doc.objects.to_json()
        doc_objects = [d async for d in Doc.objects]

        assert doc_objects == Doc.objects.from_json(json_data)

    async def test_as_pymongo(self):
        class LastLogin(EmbeddedDocument):
            location = StringField()
            ip = StringField()

        class User(Document):
            id = StringField(primary_key=True)
            name = StringField()
            age = IntField()
            price = DecimalField()
            last_login = EmbeddedDocumentField(LastLogin)

        await User.drop_collection()

        await User.objects.create(id="Bob", name="Bob Dole", age=89, price=Decimal("1.11"))
        await User.objects.create(
            id="Barak",
            name="Barak Obama",
            age=51,
            price=Decimal("2.22"),
            last_login=LastLogin(location="White House", ip="104.107.108.116"),
        )

        results = [d async for d in User.objects.as_pymongo()]
        assert set(results[0].keys()) == {"_id", "name", "age", "price"}
        assert set(results[1].keys()) == {"_id", "name", "age", "price", "last_login"}

        results = [d async for d in User.objects.only("id", "name").as_pymongo()]
        assert set(results[0].keys()) == {"_id", "name"}

        users = User.objects.only("name", "price").as_pymongo()
        results = [d async for d in users]
        assert isinstance(results[0], dict)
        assert isinstance(results[1], dict)
        assert results[0]["name"] == "Bob Dole"
        assert results[0]["price"] == 1.11
        assert results[1]["name"] == "Barak Obama"
        assert results[1]["price"] == 2.22

        users = User.objects.only("name", "last_login").as_pymongo()
        results = [d async for d in users]
        assert isinstance(results[0], dict)
        assert isinstance(results[1], dict)
        assert results[0] == {"_id": "Bob", "name": "Bob Dole"}
        assert results[1] == {
            "_id": "Barak",
            "name": "Barak Obama",
            "last_login": {"location": "White House", "ip": "104.107.108.116"},
        }

    async def test_as_pymongo_returns_cls_attribute_when_using_inheritance(self):
        class User(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        await User.drop_collection()

        user = await User(name="Bob Dole").save()
        result = await User.objects.as_pymongo().first()
        assert result == {"_cls": "User", "_id": user.id, "name": "Bob Dole"}

    async def test_as_pymongo_json_limit_fields(self):
        class User(Document):
            email = EmailField(unique=True, required=True)
            password_hash = StringField(db_field="password_hash", required=True)
            password_salt = StringField(db_field="password_salt", required=True)

        await User.drop_collection()
        await User(email="ross@example.com", password_salt="SomeSalt", password_hash="SomeHash").save()

        serialized_user = await User.objects.exclude("password_salt", "password_hash").as_pymongo().get_item(0)
        assert {"_id", "email"} == set(serialized_user.keys())

        serialized_user = await User.objects.exclude("id", "password_salt", "password_hash").to_json()
        assert '[{"email": "ross@example.com"}]' == serialized_user

        serialized_user = await User.objects.only("email").as_pymongo().get_item(0)
        assert {"_id", "email"} == set(serialized_user.keys())

        serialized_user = await User.objects.exclude("password_salt").only("email").as_pymongo().get_item(0)
        assert {"_id", "email"} == set(serialized_user.keys())

        serialized_user = await User.objects.exclude("password_salt", "id").only("email").as_pymongo().get_item(0)
        assert {"email"} == set(serialized_user.keys())

        serialized_user = await User.objects.exclude("password_salt", "id").only("email").to_json()
        assert '[{"email": "ross@example.com"}]' == serialized_user

    async def test_only_after_count(self):
        """Test that only() works after count()"""

        class User(Document):
            name = StringField()
            age = IntField()
            address = StringField()

        await User.drop_collection()
        user = await User(name="User", age=50, address="Moscow, Russia").save()

        user_queryset = User.objects(age=50)

        result = await user_queryset.only("name", "age").as_pymongo().first()
        assert result == {"_id": user.id, "name": "User", "age": 50}

        result = await user_queryset.count()
        assert result == 1

        result = await user_queryset.only("name", "age").as_pymongo().first()
        assert result == {"_id": user.id, "name": "User", "age": 50}

    async def test_cached_queryset(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        persons = [Person(name=f"No: {i}") for i in range(100)]
        await Person.objects.insert(persons, load_bulk=True)

        async with query_counter() as q:
            assert await q.get_count() == 0
            people = Person.objects

            [x async for x in people]
            assert 100 == len(people._result_cache)

            import platform

            if platform.python_implementation() != "PyPy":
                # PyPy evaluates __len__ when iterating with list comprehensions while CPython does not.
                # This may be a bug in PyPy (PyPy/#1802) but it does not affect
                # the behavior of MongoEngine.
                assert people._len is None
            assert await q.get_count() == 1

            [d async for d in people]
            # In async, __len__ is not called implicitly by async for,
            # so _len stays None until count(with_limit_and_skip=True) is called
            assert people._len is None
            assert await q.get_count() == 1

            await people.count(with_limit_and_skip=True)  # sets _len
            assert 100 == people._len
            assert await q.get_count() == 2  # count() caused an extra query

            await people.count(with_limit_and_skip=True)  # now cached
            assert await q.get_count() == 2

    async def test_no_cached_queryset(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        persons = [Person(name=f"No: {i}") for i in range(100)]
        await Person.objects.insert(persons, load_bulk=True)

        async with query_counter() as q:
            assert await q.get_count() == 0
            people = Person.objects.no_cache()

            [x async for x in people]
            assert await q.get_count() == 1

            [d async for d in people]
            assert await q.get_count() == 2

            await people.count()
            assert await q.get_count() == 3

    async def test_no_cached_queryset__repr__(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()
        qs = Person.objects.no_cache()
        assert repr(qs) == "Person async queryset (no cache)"

    async def test_no_cached_on_a_cached_queryset_raise_error(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()
        await Person(name="a").save()
        qs = Person.objects()
        _ = [d async for d in qs]
        with pytest.raises(OperationError, match="QuerySet already cached"):
            qs.no_cache()

    async def test_no_cached_queryset_no_cache_back_to_cache(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()
        qs = Person.objects()
        assert isinstance(qs, QuerySet)
        qs = qs.no_cache()
        assert isinstance(qs, QuerySetNoCache)
        qs = qs.cache()
        assert isinstance(qs, QuerySet)

    async def test_cache_not_cloned(self):
        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        await User.drop_collection()

        await User(name="Alice").save()
        await User(name="Bob").save()

        users = User.objects.all().order_by("name")
        result = [d async for d in users]
        assert len(result) == 2
        assert 2 == len(users._result_cache)

        users = users.filter(name="Bob")
        result = [d async for d in users]
        assert len(result) == 1
        assert 1 == len(users._result_cache)

    async def test_no_cache(self):
        """Ensure you can add metadata to file"""

        class Noddy(Document):
            fields = DictField()

        await Noddy.drop_collection()

        noddies = []
        for i in range(100):
            noddy = Noddy()
            for j in range(20):
                noddy.fields["key" + str(j)] = "value " + str(j)
            noddies.append(noddy)
        await Noddy.objects.insert(noddies, load_bulk=True)

        docs = Noddy.objects.no_cache()

        counter = len([1 async for i in docs])
        assert counter == 100

        assert len([d async for d in docs]) == 100

        # Can't directly get a length of a no-cache queryset.
        with pytest.raises(TypeError):
            len(docs)

        # Another iteration over the queryset should result in another db op.
        async with query_counter() as q:
            [d async for d in docs]
            assert await q.get_count() == 1

        # ... and another one to double-check.
        async with query_counter() as q:
            [d async for d in docs]
            assert await q.get_count() == 1

    async def test_nested_queryset_iterator(self):
        # Try iterating the same queryset twice, nested.
        names = ["Alice", "Bob", "Chuck", "David", "Eric", "Francis", "George"]

        class User(Document):
            name = StringField()

            def __unicode__(self):
                return self.name

        await User.drop_collection()

        for name in names:
            await User(name=name).save()

        users = User.objects.all().order_by("name")
        outer_count = 0
        inner_count = 0
        inner_total_count = 0

        async with query_counter() as q:
            assert await q.get_count() == 0

            assert await users.count(with_limit_and_skip=True) == 7

            i = 0
            async for outer_user in users:
                assert outer_user.name == names[i]
                outer_count += 1
                inner_count = 0

                # Calling len might disrupt the inner loop if there are bugs
                assert await users.count(with_limit_and_skip=True) == 7

                j = 0
                async for inner_user in users:
                    assert inner_user.name == names[j]
                    inner_count += 1
                    inner_total_count += 1
                    j += 1

                # inner loop should always be executed seven times
                assert inner_count == 7
                i += 1

            # outer loop should be executed seven times total
            assert outer_count == 7
            # inner loop should be executed fourtynine times total
            assert inner_total_count == 7 * 7

            assert await q.get_count() == 2

    async def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            y = IntField()

            meta = {"allow_inheritance": True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        await A.drop_collection()

        await A(x=10, y=20).save()
        await A(x=15, y=30).save()
        await B(x=20, y=40).save()
        await B(x=30, y=50).save()
        await C(x=40, y=60).save()

        assert await A.objects.no_sub_classes().count() == 2
        assert await A.objects.count() == 5

        assert await B.objects.no_sub_classes().count() == 2
        assert await B.objects.count() == 3

        assert await C.objects.no_sub_classes().count() == 1
        assert await C.objects.count() == 1

        async for obj in A.objects.no_sub_classes():
            assert obj.__class__ == A

        async for obj in B.objects.no_sub_classes():
            assert obj.__class__ == B

        async for obj in C.objects.no_sub_classes():
            assert obj.__class__ == C

    async def test_query_generic_embedded_document(self):
        """Ensure that querying sub field on generic_embedded_field works"""

        class A(EmbeddedDocument):
            a_name = StringField()

        class B(EmbeddedDocument):
            b_name = StringField()

        class Doc(Document):
            document = GenericEmbeddedDocumentField(choices=(A, B))

        await Doc.drop_collection()
        await Doc(document=A(a_name="A doc")).save()
        await Doc(document=B(b_name="B doc")).save()

        # Using raw in filter working fine
        assert await Doc.objects(__raw__={"document.a_name": "A doc"}).count() == 1
        assert await Doc.objects(__raw__={"document.b_name": "B doc"}).count() == 1
        assert await Doc.objects(document__a_name="A doc").count() == 1
        assert await Doc.objects(document__b_name="B doc").count() == 1

    async def test_query_reference_to_custom_pk_doc(self):
        class A(Document):
            id = StringField(primary_key=True)

        class B(Document):
            a = ReferenceField(A)

        await A.drop_collection()
        await B.drop_collection()

        a = await A.objects.create(id="custom_id")
        await B.objects.create(a=a)

        assert await B.objects.count() == 1
        assert (await B.objects.get(a=a)).a == a
        assert (await B.objects.get(a=a.id)).a == a

    async def test_cls_query_in_subclassed_docs(self):
        class Animal(Document):
            name = StringField()

            meta = {"allow_inheritance": True}

        class Dog(Animal):
            pass

        class Cat(Animal):
            pass

        assert Animal.objects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": {"$in": ("Animal", "Animal.Dog", "Animal.Cat")},
        }
        assert Dog.objects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": "Animal.Dog",
        }
        assert Cat.objects(name="Charlie")._query == {
            "name": "Charlie",
            "_cls": "Animal.Cat",
        }

    async def test_can_have_field_same_name_as_query_operator(self):
        class Size(Document):
            name = StringField()

        class Example(Document):
            size = ReferenceField(Size)

        await Size.drop_collection()
        await Example.drop_collection()

        instance_size = await Size(name="Large").save()
        await Example(size=instance_size).save()

        assert await Example.objects(size=instance_size).count() == 1
        assert await Example.objects(size__in=[instance_size]).count() == 1

    async def test_cursor_in_an_if_stmt(self):
        class Test(Document):
            test_field = StringField()

        await Test.drop_collection()
        queryset = Test.objects

        # In async mode, bool(queryset) is not supported; use is_empty()
        if not await queryset.is_empty():
            raise AssertionError("Empty cursor returns True")

        test = Test()
        test.test_field = "test"
        await test.save()

        queryset = Test.objects
        if await queryset.is_empty():
            raise AssertionError("Cursor has data and returned False")

        # Verify bool() raises TypeError in async mode
        with pytest.raises(TypeError):
            bool(queryset)

    async def test_bool_performance(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        persons = [Person(name=f"No: {i}") for i in range(100)]
        await Person.objects.insert(persons, load_bulk=True)

        async with query_counter() as q:
            # In async mode, use _has_data() instead of bool()
            assert await Person.objects._has_data()

            assert await q.get_count() == 1
            ops = await q.db.system.profile.find({"ns": {"$ne": f"{q.db.name}.system.indexes"}}).to_list()
            op = ops[0]

            assert op["nreturned"] == 1

    async def test_bool_with_ordering(self):
        ORDER_BY_KEY, CMD_QUERY_KEY = "sort", "command"

        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        await Person(name="Test").save()

        # Check that _has_data() does not use the orderby
        qs = Person.objects.order_by("name")
        async with query_counter() as q:
            assert await qs._has_data()

            ops = await q.db.system.profile.find({"ns": {"$ne": f"{q.db.name}.system.indexes"}}).to_list()
            op = ops[0]

            assert ORDER_BY_KEY not in op[CMD_QUERY_KEY]

        # Check that normal query uses orderby
        qs2 = Person.objects.order_by("name")
        async with query_counter() as q:
            async for x in qs2:
                pass

            ops = await q.db.system.profile.find({"ns": {"$ne": f"{q.db.name}.system.indexes"}}).to_list()
            op = ops[0]

            assert ORDER_BY_KEY in op[CMD_QUERY_KEY]

    async def test_bool_with_ordering_from_meta_dict(self):
        _ORDER_BY_KEY, CMD_QUERY_KEY = "sort", "command"

        class Person(Document):
            name = StringField()
            meta = {"ordering": ["name"]}

        await Person.drop_collection()

        await Person(name="B").save()
        await Person(name="C").save()
        await Person(name="A").save()

        async with query_counter() as q:
            # In async mode, use _has_data() instead of bool()
            assert await Person.objects._has_data()

            ops = await q.db.system.profile.find({"ns": {"$ne": f"{q.db.name}.system.indexes"}}).to_list()
            op = ops[0]

            assert "$orderby" not in op[CMD_QUERY_KEY], "BaseQuerySet must remove orderby from meta in boolen test"

            assert (await Person.objects.first()).name == "A"
            assert await Person.objects._has_data(), "Cursor has data and returned False"

    async def test_delete_count(self):
        for i in range(1, 4):
            await self.Person(name=f"User {i}", age=i * 10).save()
        assert await self.Person.objects().delete() == 3  # test ordinary QuerySey delete count

        for i in range(1, 4):
            await self.Person(name=f"User {i}", age=i * 10).save()

        assert await self.Person.objects().skip(1).delete() == 2  # test Document delete with existing documents

        await self.Person.objects().delete()
        assert await self.Person.objects().skip(1).delete() == 0  # test Document delete without existing documents

    async def test_max_time_ms(self):
        # 778: max_time_ms accepts int or None
        # In the async version, _chainable_method defers validation,
        # so we just verify it works with valid input
        qs = self.Person.objects(name="name").max_time_ms(100)
        assert qs._max_time_ms == 100

        qs = self.Person.objects(name="name").max_time_ms(None)
        assert qs._max_time_ms is None

    async def test_subclass_field_query(self):
        class Animal(Document):
            is_mamal = BooleanField()
            meta = {"allow_inheritance": True}

        class Cat(Animal):
            whiskers_length = FloatField()

        class ScottishCat(Cat):
            folded_ears = BooleanField()

        await Animal.drop_collection()

        await Animal(is_mamal=False).save()
        await Cat(is_mamal=True, whiskers_length=5.1).save()
        await ScottishCat(is_mamal=True, folded_ears=True).save()
        assert await Animal.objects(folded_ears=True).count() == 1
        assert await Animal.objects(whiskers_length=5.1).count() == 1

    async def test_loop_over_invalid_id_does_not_crash(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        await (await Person._get_collection()).insert_one({"name": "a", "id": ""})
        async for p in Person.objects():
            assert p.name == "a"

    async def test_len_during_iteration(self):
        """Tests that iterating over a limited queryset returns the
        correct number of documents.
        """

        class Data(Document):
            pass

        await Data.drop_collection()

        for i in range(300):
            await Data().save()

        records = Data.objects.limit(250)

        # Verify that count works before iteration
        assert await records.count(with_limit_and_skip=True) == 250

        # Assert that iterating touches exactly 250 documents
        i = -1
        async for r in records:
            i += 1
        assert i == 249

        # Assert the same behavior with a fresh queryset
        records = Data.objects.limit(250)
        i = -1
        async for r in records:
            i += 1
        assert i == 249

    async def test_iteration_within_iteration(self):
        """You should be able to reliably iterate over all the documents
        in a given queryset even if there are multiple iterations of it
        happening at the same time.
        """

        class Data(Document):
            pass

        for i in range(300):
            await Data().save()

        qs = Data.objects.limit(250)
        i = -1
        async for doc in qs:
            i += 1
            j = -1
            async for doc2 in qs:
                j += 1

        assert i == 249
        assert j == 249

    async def test_in_operator_on_non_iterable(self):
        """Ensure that using the `__in` operator on a non-iterable raises an
        error.
        """

        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(User))

        await User.drop_collection()
        await BlogPost.drop_collection()

        author = await User.objects.create(name="Test User")
        post = await BlogPost.objects.create(content="Had a good coffee today...", authors=[author])

        # Make sure using `__in` with a list works
        blog_posts = BlogPost.objects(authors__in=[author])
        assert [d async for d in blog_posts] == [post]

        # Using `__in` with a non-iterable should raise a TypeError
        with pytest.raises(TypeError):
            await BlogPost.objects(authors__in=author.pk).count()

        # Using `__in` with a `Document` (which is seemingly iterable but not
        # in a way we'd expect) should raise a TypeError, too
        with pytest.raises(TypeError):
            await BlogPost.objects(authors__in=author).count()

    async def test_create_count(self):
        await self.Person.drop_collection()
        await self.Person.objects.create(name="Foo")
        await self.Person.objects.create(name="Bar")
        await self.Person.objects.create(name="Baz")
        assert await self.Person.objects.count(with_limit_and_skip=True) == 3

        await self.Person.objects.create(name="Foo_1")
        assert await self.Person.objects.count(with_limit_and_skip=True) == 4

    async def test_no_cursor_timeout(self):
        qs = self.Person.objects()
        assert qs._cursor_args == {}  # ensure no regression of  #2148

        qs = self.Person.objects().timeout(True)
        assert qs._cursor_args == {}

        qs = self.Person.objects().timeout(False)
        assert qs._cursor_args == {"no_cursor_timeout": True}

    async def test_allow_disk_use(self):
        qs = self.Person.objects()
        assert qs._cursor_args == {}

        qs = self.Person.objects().allow_disk_use(False)
        assert qs._cursor_args == {}

        qs = self.Person.objects().allow_disk_use(True)
        assert qs._cursor_args == {"allow_disk_use": True}

        # Test if allow_disk_use changes the results
        await self.Person.drop_collection()
        await self.Person.objects.create(name="Foo", age=12)
        await self.Person.objects.create(name="Baz", age=17)
        await self.Person.objects.create(name="Bar", age=13)

        qs_disk = self.Person.objects().order_by("age").allow_disk_use(True)
        qs = self.Person.objects().order_by("age")

        assert await qs_disk.count() == await qs.count()

        for index in range(await qs_disk.count()):
            assert await qs_disk.get_item(index) == await qs.get_item(index)
