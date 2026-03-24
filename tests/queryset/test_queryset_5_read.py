import datetime
import uuid
from decimal import Decimal

import pymongo
import pytest
from bson import DBRef, ObjectId
from pymongo.read_preferences import ReadPreference
from pymongo.results import UpdateResult

from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.context_managers import query_counter, switch_db
from mongoengine.errors import InvalidQueryError
from mongoengine.mongodb_support import MONGODB_36
from mongoengine.pymongo_support import PYMONGO_VERSION
from mongoengine.queryset import (
    DoesNotExist,
    MultipleObjectsReturned,
    QuerySet,
    QuerySetManager,
    queryset_manager,
)
from mongoengine.queryset.base import BaseQuerySet
from tests.utils import (
    db_ops_tracker,
    get_as_pymongo,
    requires_mongodb_gte_42,
    requires_mongodb_gte_44,
    requires_mongodb_lt_42,
)

from tests.utils import MongoDBTestCase



def get_key_compat(mongo_ver):
    ORDER_BY_KEY = "sort"
    CMD_QUERY_KEY = "command" if mongo_ver >= MONGODB_36 else "query"
    return ORDER_BY_KEY, CMD_QUERY_KEY


class TestQueryset5(MongoDBTestCase):
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


    async def test_custom_querysets_managers_directly(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySetManager(QuerySetManager):
            @staticmethod
            def get_queryset(doc_cls, queryset):
                return queryset(is_published=True)

        class Post(Document):
            is_published = BooleanField(default=False)
            published = CustomQuerySetManager()

        await Post.drop_collection()

        await Post().save()
        await Post(is_published=True).save()
        assert await Post.objects.count() == 2
        assert await Post.published.count() == 1

        await Post.drop_collection()


    async def test_custom_querysets_inherited(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(QuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class Base(Document):
            meta = {"abstract": True, "queryset_class": CustomQuerySet}

        class Post(Base):
            pass

        await Post.drop_collection()
        assert isinstance(Post.objects, CustomQuerySet)
        assert not await Post.objects.not_empty()

        await Post().save()
        assert await Post.objects.not_empty()

        await Post.drop_collection()


    async def test_custom_querysets_inherited_direct(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(QuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Base(Document):
            meta = {"abstract": True}
            objects = CustomQuerySetManager()

        class Post(Base):
            pass

        await Post.drop_collection()
        assert isinstance(Post.objects, CustomQuerySet)
        assert not await Post.objects.not_empty()

        await Post().save()
        assert await Post.objects.not_empty()

        await Post.drop_collection()


    async def test_count_limit_and_skip(self):
        class Post(Document):
            title = StringField()

        await Post.drop_collection()

        for i in range(10):
            await Post(title="Post %s" % i).save()

        assert 5 == await Post.objects.limit(5).skip(5).count(with_limit_and_skip=True)

        assert 10 == await Post.objects.limit(5).skip(5).count(with_limit_and_skip=False)


    async def test_count_and_none(self):
        """Test count works with None()"""

        class MyDoc(Document):
            pass

        await MyDoc.drop_collection()
        for i in range(0, 10):
            await MyDoc().save()

        assert await MyDoc.objects.count() == 10
        assert await MyDoc.objects.none().count() == 0


    async def test_count_list_embedded(self):
        class B(EmbeddedDocument):
            c = StringField()

        class A(Document):
            b = ListField(EmbeddedDocumentField(B))

        assert await A.objects(b=[{"c": "c"}]).count() == 0


    async def test_call_after_limits_set(self):
        """Ensure that re-filtering after slicing works"""

        class Post(Document):
            title = StringField()

        await Post.drop_collection()

        await Post(title="Post 1").save()
        await Post(title="Post 2").save()

        posts = Post.objects.all()[0:1]
        assert len([d async for d in posts()]) == 1

        await Post.drop_collection()


    async def test_order_then_filter(self):
        """Ensure that ordering still works after filtering."""

        class Number(Document):
            n = IntField()

        await Number.drop_collection()

        n2 = await Number.objects.create(n=2)
        n1 = await Number.objects.create(n=1)

        assert [d async for d in Number.objects] == [n2, n1]
        assert [d async for d in Number.objects.order_by("n")] == [n1, n2]
        assert [d async for d in Number.objects.order_by("n").filter()] == [n1, n2]

        await Number.drop_collection()


    async def test_clone(self):
        """Ensure that cloning clones complex querysets"""

        class Number(Document):
            n = IntField()

        await Number.drop_collection()

        for i in range(1, 101):
            t = Number(n=i)
            await t.save()

        test = Number.objects
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        test = test.filter(n__gt=11)
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        test = test.limit(10)
        test2 = test.clone()
        assert test != test2
        assert await test.count() == await test2.count()

        await Number.drop_collection()


    async def test_clone_retains_settings(self):
        """Ensure that cloning retains the read_preference and read_concern"""

        class Number(Document):
            n = IntField()

        await Number.drop_collection()

        qs = Number.objects
        qs_clone = qs.clone()
        assert qs._read_preference == qs_clone._read_preference
        assert qs._read_concern == qs_clone._read_concern

        qs = Number.objects.read_preference(ReadPreference.PRIMARY_PREFERRED)
        qs_clone = qs.clone()
        assert qs._read_preference == ReadPreference.PRIMARY_PREFERRED
        assert qs._read_preference == qs_clone._read_preference

        qs = Number.objects.read_concern({"level": "majority"})
        qs_clone = qs.clone()
        assert qs._read_concern.document == {"level": "majority"}
        assert qs._read_concern == qs_clone._read_concern

        await Number.drop_collection()


    async def test_using(self):
        """Ensure that switching databases for a queryset is possible"""

        class Number2(Document):
            n = IntField()

        await Number2.drop_collection()
        async with switch_db(Number2, "test2") as Number2:
            await Number2.drop_collection()

        for i in range(1, 10):
            t = Number2(n=i)
            t.switch_db("test2")
            await t.save()

        assert await Number2.objects.using("test2").count() == 9


    async def test_unset_reference(self):
        class Comment(Document):
            text = StringField()

        class Post(Document):
            comment = ReferenceField(Comment)

        await Comment.drop_collection()
        await Post.drop_collection()

        comment = await Comment.objects.create(text="test")
        post = await Post.objects.create(comment=comment)

        assert post.comment == comment
        await Post.objects.update(unset__comment=1)
        await post.reload()
        assert post.comment is None

        await Comment.drop_collection()
        await Post.drop_collection()


    async def test_order_works_with_custom_db_field_names(self):
        class Number(Document):
            n = IntField(db_field="number")

        await Number.drop_collection()

        n2 = await Number.objects.create(n=2)
        n1 = await Number.objects.create(n=1)

        assert [d async for d in Number.objects] == [n2, n1]
        assert [d async for d in Number.objects.order_by("n")] == [n1, n2]

        await Number.drop_collection()


    async def test_order_works_with_primary(self):
        """Ensure that order_by and primary work."""

        class Number(Document):
            n = IntField(primary_key=True)

        await Number.drop_collection()

        await Number(n=1).save()
        await Number(n=2).save()
        await Number(n=3).save()

        numbers = [n.n async for n in Number.objects.order_by("-n")]
        assert [3, 2, 1] == numbers

        numbers = [n.n async for n in Number.objects.order_by("+n")]
        assert [1, 2, 3] == numbers
        await Number.drop_collection()


    async def test_create_index(self):
        """Ensure that manual creation of indexes works."""

        class Comment(Document):
            message = StringField()
            meta = {"allow_inheritance": True}

        Comment.create_index("message")

        info = Comment.objects._collection.index_information()
        info = [
            (value["key"], value.get("unique", False), value.get("sparse", False))
            for key, value in info.items()
        ]
        assert ([("_cls", 1), ("message", 1)], False, False) in info


    async def test_where_query(self):
        """Ensure that where clauses work."""

        class IntPair(Document):
            fielda = IntField()
            fieldb = IntField()

        await IntPair.drop_collection()

        a = IntPair(fielda=1, fieldb=1)
        b = IntPair(fielda=1, fieldb=2)
        c = IntPair(fielda=2, fieldb=1)
        await a.save()
        await b.save()
        await c.save()

        query = IntPair.objects.where("this[~fielda] >= this[~fieldb]")
        assert 'this["fielda"] >= this["fieldb"]' == query._where_clause
        results = [d async for d in query]
        assert 2 == len(results)
        assert a in results
        assert c in results

        query = IntPair.objects.where("this[~fielda] == this[~fieldb]")
        results = [d async for d in query]
        assert 1 == len(results)
        assert a in results

        query = IntPair.objects.where(
            "function() { return this[~fielda] >= this[~fieldb] }"
        )
        assert (
            'function() { return this["fielda"] >= this["fieldb"] }'
            == query._where_clause
        )
        results = [d async for d in query]
        assert 2 == len(results)
        assert a in results
        assert c in results

        with pytest.raises(TypeError):
            [d async for d in IntPair.objects.where(fielda__gte=3)]


    async def test_where_query_field_name_subs(self):
        class DomainObj(Document):
            field_1 = StringField(db_field="field_2")

        await DomainObj.drop_collection()

        await DomainObj(field_1="test").save()

        obj = DomainObj.objects.where("this[~field_1] == 'NOTMATCHING'")
        assert not [d async for d in obj]

        obj = DomainObj.objects.where("this[~field_1] == 'test'")
        assert [d async for d in obj]


    async def test_where_modify(self):
        class DomainObj(Document):
            field = StringField()

        await DomainObj.drop_collection()

        await DomainObj(field="test").save()

        obj = DomainObj.objects.where("this[~field] == 'NOTMATCHING'")
        assert not [d async for d in obj]

        obj = DomainObj.objects.where("this[~field] == 'test'")
        assert [d async for d in obj]

        qs = await DomainObj.objects.where("this[~field] == 'NOTMATCHING'").modify(
            field="new"
        )
        assert not qs

        qs = await DomainObj.objects.where("this[~field] == 'test'").modify(field="new")
        assert qs


    async def test_where_modify_field_name_subs(self):
        class DomainObj(Document):
            field_1 = StringField(db_field="field_2")

        await DomainObj.drop_collection()

        await DomainObj(field_1="test").save()

        obj = await DomainObj.objects.where("this[~field_1] == 'NOTMATCHING'").modify(
            field_1="new"
        )
        assert not obj

        obj = await DomainObj.objects.where("this[~field_1] == 'test'").modify(field_1="new")
        assert obj

        assert await get_as_pymongo(obj) == {"_id": obj.id, "field_2": "new"}


    async def test_scalar(self):
        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            organization = ObjectIdField()

        await User.drop_collection()
        await Organization.drop_collection()

        whitehouse = Organization(name="White House")
        await whitehouse.save()
        await User(name="Bob Dole", organization=whitehouse.id).save()

        # Efficient way to get all unique organization names for a given
        # set of users (Pretend this has additional filtering.)
        user_orgs = set([d async for d in User.objects.scalar("organization")])
        orgs = Organization.objects(id__in=user_orgs).scalar("name")
        assert [d async for d in orgs] == ["White House"]

        # Efficient for generating listings, too.
        orgs = await Organization.objects.scalar("name").in_bulk(list(user_orgs))
        user_map = User.objects.scalar("name", "organization")
        user_listing = [(user, orgs[org]) async for user, org in user_map]
        assert [("Bob Dole", "White House")] == user_listing


    async def test_scalar_simple(self):
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.drop_collection()

        await TestDoc(x=10, y=True).save()
        await TestDoc(x=20, y=False).save()
        await TestDoc(x=30, y=True).save()

        plist = [d async for d in TestDoc.objects.scalar("x", "y")]

        assert len(plist) == 3
        assert plist[0] == (10, True)
        assert plist[1] == (20, False)
        assert plist[2] == (30, True)

        class UserDoc(Document):
            name = StringField()
            age = IntField()

        await UserDoc.drop_collection()

        await UserDoc(name="Wilson Jr", age=19).save()
        await UserDoc(name="Wilson", age=43).save()
        await UserDoc(name="Eliana", age=37).save()
        await UserDoc(name="Tayza", age=15).save()

        ulist = [d async for d in UserDoc.objects.scalar("name", "age")]

        assert ulist == [
            ("Wilson Jr", 19),
            ("Wilson", 43),
            ("Eliana", 37),
            ("Tayza", 15),
        ]

        ulist = [d async for d in UserDoc.objects.scalar("name").order_by("age")]

        assert ulist == [("Tayza"), ("Wilson Jr"), ("Eliana"), ("Wilson")]


    async def test_scalar_embedded(self):
        class Profile(EmbeddedDocument):
            name = StringField()
            age = IntField()

        class Locale(EmbeddedDocument):
            city = StringField()
            country = StringField()

        class Person(Document):
            profile = EmbeddedDocumentField(Profile)
            locale = EmbeddedDocumentField(Locale)

        await Person.drop_collection()

        await Person(
            profile=Profile(name="Wilson Jr", age=19),
            locale=Locale(city="Corumba-GO", country="Brazil"),
        ).save()

        await Person(
            profile=Profile(name="Gabriel Falcao", age=23),
            locale=Locale(city="New York", country="USA"),
        ).save()

        await Person(
            profile=Profile(name="Lincoln de souza", age=28),
            locale=Locale(city="Belo Horizonte", country="Brazil"),
        ).save()

        await Person(
            profile=Profile(name="Walter cruz", age=30),
            locale=Locale(city="Brasilia", country="Brazil"),
        ).save()

        assert [d async for d in
            Person.objects.order_by("profile__age").scalar("profile__name")
        ] == ["Wilson Jr", "Gabriel Falcao", "Lincoln de souza", "Walter cruz"]

        ulist = [d async for d in
            Person.objects.order_by("locale.city").scalar(
                "profile__name", "profile__age", "locale__city"
            )
        ]
        assert ulist == [
            ("Lincoln de souza", 28, "Belo Horizonte"),
            ("Walter cruz", 30, "Brasilia"),
            ("Wilson Jr", 19, "Corumba-GO"),
            ("Gabriel Falcao", 23, "New York"),
        ]


    async def test_scalar_decimal(self):
        from decimal import Decimal

        class Person(Document):
            name = StringField()
            rating = DecimalField()

        await Person.drop_collection()
        await Person(name="Wilson Jr", rating=Decimal("1.0")).save()

        ulist = [d async for d in Person.objects.scalar("name", "rating")]
        assert ulist == [("Wilson Jr", Decimal("1.0"))]


    async def test_scalar_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = ReferenceField(State)

        await State.drop_collection()
        await Person.drop_collection()

        s1 = State(name="Goias")
        await s1.save()

        await Person(name="Wilson JR", state=s1).save()

        plist = [d async for d in Person.objects.scalar("name", "state")]
        assert plist == [("Wilson JR", s1)]


    async def test_scalar_generic_reference_field(self):
        class State(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            state = GenericReferenceField()

        await State.drop_collection()
        await Person.drop_collection()

        s1 = State(name="Goias")
        await s1.save()

        await Person(name="Wilson JR", state=s1).save()

        plist = [d async for d in Person.objects.scalar("name", "state")]
        assert plist == [("Wilson JR", s1)]


    async def test_generic_reference_field_with_only_and_as_pymongo(self):
        class TestPerson(Document):
            name = StringField()

        class TestActivity(Document):
            name = StringField()
            owner = GenericReferenceField()

        await TestPerson.drop_collection()
        await TestActivity.drop_collection()

        person = TestPerson(name="owner")
        await person.save()

        a1 = TestActivity(name="a1", owner=person)
        await a1.save()

        activity = await (
            TestActivity.objects(owner=person)
            .scalar("id", "owner")
            .no_dereference()
            .first()
        )
        assert activity[0] == a1.pk
        assert activity[1]["_ref"] == DBRef("test_person", person.pk)

        activity = await TestActivity.objects(owner=person).only("id", "owner").get_item(0)
        assert activity.pk == a1.pk
        assert activity.owner == person

        activity = await (
            TestActivity.objects(owner=person).only("id", "owner").as_pymongo().first()
        )
        assert activity["_id"] == a1.pk
        assert activity["owner"]["_ref"], DBRef("test_person", person.pk)


    async def test_scalar_db_field(self):
        class TestDoc(Document):
            x = IntField()
            y = BooleanField()

        await TestDoc.drop_collection()

        await TestDoc(x=10, y=True).save()
        await TestDoc(x=20, y=False).save()
        await TestDoc(x=30, y=True).save()

        plist = [d async for d in TestDoc.objects.scalar("x", "y")]
        assert len(plist) == 3
        assert plist[0] == (10, True)
        assert plist[1] == (20, False)
        assert plist[2] == (30, True)


    async def test_scalar_primary_key(self):
        class SettingValue(Document):
            key = StringField(primary_key=True)
            value = StringField()

        await SettingValue.drop_collection()
        s = SettingValue(key="test", value="test value")
        await s.save()

        val = SettingValue.objects.scalar("key", "value")
        assert [d async for d in val] == [("test", "test value")]


    async def test_scalar_cursor_behaviour(self):
        """Ensure that a query returns a valid set of results."""
        person1 = self.Person(name="User A", age=20)
        await person1.save()
        person2 = self.Person(name="User B", age=30)
        await person2.save()

        # Find all people in the collection
        people = self.Person.objects.scalar("name")
        assert await people.count() == 2
        results = [d async for d in people]
        assert results[0] == "User A"
        assert results[1] == "User B"

        # Use a query to filter the people found to just person1
        people = self.Person.objects(age=20).scalar("name")
        assert await people.count() == 1
        person = await people.__anext__()
        assert person == "User A"

        # Test limit
        people = [d async for d in self.Person.objects.limit(1).scalar("name")]
        assert len(people) == 1
        assert people[0] == "User A"

        # Test skip
        people = [d async for d in self.Person.objects.skip(1).scalar("name")]
        assert len(people) == 1
        assert people[0] == "User B"

        person3 = self.Person(name="User C", age=40)
        await person3.save()

        # Test slice limit
        people = [d async for d in self.Person.objects[:2].scalar("name")]
        assert len(people) == 2
        assert people[0] == "User A"
        assert people[1] == "User B"

        # Test slice skip
        people = [d async for d in self.Person.objects[1:].scalar("name")]
        assert len(people) == 2
        assert people[0] == "User B"
        assert people[1] == "User C"

        # Test slice limit and skip
        people = [d async for d in self.Person.objects[1:2].scalar("name")]
        assert len(people) == 1
        assert people[0] == "User B"

        # people = list(self.Person.objects[1:1].scalar("name"))
        people = self.Person.objects[1:1]
        people = people.scalar("name")
        assert await people.count() == 0

        # Test slice out of range
        people = [d async for d in self.Person.objects.scalar("name")[80000:80001]]
        assert len(people) == 0

        # Test larger slice __repr__
        await self.Person.objects.delete()
        for i in range(55):
            await self.Person(name="A%s" % i, age=i).save()

        assert await self.Person.objects.scalar("name").count() == 55
        assert (
            "A0" == "%s" % await self.Person.objects.order_by("name").scalar("name").first()
        )
        assert "A0" == "%s" % await self.Person.objects.scalar("name").order_by("name").get_item(0)
        assert (
            "['A1', 'A2']"
            == "%s" % self.Person.objects.order_by("age").scalar("name")[1:3]
        )
        assert (
            "['A51', 'A52']"
            == "%s" % self.Person.objects.order_by("age").scalar("name")[51:53]
        )

        # with_id and in_bulk
        person = await self.Person.objects.order_by("name").first()
        assert "A0" == "%s" % await self.Person.objects.scalar("name").with_id(person.id)

        pks = self.Person.objects.order_by("age").scalar("pk")[1:3]
        names = (await self.Person.objects.scalar("name").in_bulk([d async for d in pks])).values()
        expected = "['A1', 'A2']"
        assert expected == "%s" % sorted(names)


    async def test_fields(self):
        class Bar(EmbeddedDocument):
            v = StringField()
            z = StringField()

        class Foo(Document):
            x = StringField()
            y = IntField()
            items = EmbeddedDocumentListField(Bar)

        await Foo.drop_collection()

        await Foo(x="foo1", y=1).save()
        await Foo(x="foo2", y=2, items=[]).save()
        await Foo(x="foo3", y=3, items=[Bar(z="a", v="V")]).save()
        await Foo(
            x="foo4",
            y=4,
            items=[
                Bar(z="a", v="V"),
                Bar(z="b", v="W"),
                Bar(z="b", v="X"),
                Bar(z="c", v="V"),
            ],
        ).save()
        await Foo(
            x="foo5",
            y=5,
            items=[
                Bar(z="b", v="X"),
                Bar(z="c", v="V"),
                Bar(z="d", v="V"),
                Bar(z="e", v="V"),
            ],
        ).save()

        foos_with_x = [d async for d in Foo.objects.order_by("y").fields(x=1)]

        assert all(o.x is not None for o in foos_with_x)

        foos_without_y = [d async for d in Foo.objects.order_by("y").fields(y=0)]

        assert all(o.y is None for o in foos_without_y)

        foos_with_sliced_items = [d async for d in Foo.objects.order_by("y").fields(slice__items=1)]

        assert foos_with_sliced_items[0].items == []
        assert foos_with_sliced_items[1].items == []
        assert len(foos_with_sliced_items[2].items) == 1
        assert foos_with_sliced_items[2].items[0].z == "a"
        assert len(foos_with_sliced_items[3].items) == 1
        assert foos_with_sliced_items[3].items[0].z == "a"
        assert len(foos_with_sliced_items[4].items) == 1
        assert foos_with_sliced_items[4].items[0].z == "b"

        foos_with_elem_match_items = [d async for d in
            Foo.objects.order_by("y").fields(elemMatch__items={"z": "b"})
        ]

        assert foos_with_elem_match_items[0].items == []
        assert foos_with_elem_match_items[1].items == []
        assert foos_with_elem_match_items[2].items == []
        assert len(foos_with_elem_match_items[3].items) == 1
        assert foos_with_elem_match_items[3].items[0].z == "b"
        assert foos_with_elem_match_items[3].items[0].v == "W"
        assert len(foos_with_elem_match_items[4].items) == 1
        assert foos_with_elem_match_items[4].items[0].z == "b"


    async def test_elem_match(self):
        class Foo(EmbeddedDocument):
            shape = StringField()
            color = StringField()
            thick = BooleanField()
            meta = {"allow_inheritance": False}

        class Bar(Document):
            foo = ListField(EmbeddedDocumentField(Foo))
            meta = {"allow_inheritance": False}

        await Bar.drop_collection()

        b1 = Bar(
            foo=[
                Foo(shape="square", color="purple", thick=False),
                Foo(shape="circle", color="red", thick=True),
            ]
        )
        await b1.save()

        b2 = Bar(
            foo=[
                Foo(shape="square", color="red", thick=True),
                Foo(shape="circle", color="purple", thick=False),
            ]
        )
        await b2.save()

        b3 = Bar(
            foo=[
                Foo(shape="square", thick=True),
                Foo(shape="circle", color="purple", thick=False),
            ]
        )
        await b3.save()

        ak = [d async for d in Bar.objects(foo__match={"shape": "square", "color": "purple"})]
        assert [b1] == ak

        ak = [d async for d in Bar.objects(foo__elemMatch={"shape": "square", "color": "purple"})]
        assert [b1] == ak

        ak = [d async for d in Bar.objects(foo__match=Foo(shape="square", color="purple"))]
        assert [b1] == ak

        ak = [d async for d in
            Bar.objects(foo__elemMatch={"shape": "square", "color__exists": True})
        ]
        assert [b1, b2] == ak

        ak = [d async for d in Bar.objects(foo__match={"shape": "square", "color__exists": True})]
        assert [b1, b2] == ak

        ak = [d async for d in
            Bar.objects(foo__elemMatch={"shape": "square", "color__exists": False})
        ]
        assert [b3] == ak

        ak = [d async for d in Bar.objects(foo__match={"shape": "square", "color__exists": False})]
        assert [b3] == ak


    async def test_upsert_includes_cls(self):
        """Upserts should include _cls information for inheritable classes"""

        class Test(Document):
            test = StringField()

        await Test.drop_collection()
        await Test.objects(test="foo").update_one(upsert=True, set__test="foo")
        assert "_cls" not in await Test._collection.find_one()

        class Test(Document):
            meta = {"allow_inheritance": True}
            test = StringField()

        await Test.drop_collection()

        await Test.objects(test="foo").update_one(upsert=True, set__test="foo")
        assert "_cls" in await Test._collection.find_one()


    async def test_update_upsert_looks_like_a_digit(self):
        class MyDoc(DynamicDocument):
            pass

        await MyDoc.drop_collection()
        assert 1 == await MyDoc.objects.update_one(upsert=True, inc__47=1)
        assert (await MyDoc.objects.get())["47"] == 1


    async def test_dictfield_key_looks_like_a_digit(self):
        """Only should work with DictField even if they have numeric keys."""

        class MyDoc(Document):
            test = DictField()

        await MyDoc.drop_collection()
        doc = MyDoc(test={"47": 1})
        await doc.save()
        assert (await MyDoc.objects.only("test__47").get()).test["47"] == 1


    async def test_clear_cls_query(self):
        class Parent(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Child(Parent):
            age = IntField()

        await Parent.drop_collection()

        # Default query includes the "_cls" check.
        assert Parent.objects._query == {"_cls": {"$in": ("Parent", "Parent.Child")}}

        # Clearing the "_cls" query should work.
        assert Parent.objects.clear_cls_query()._query == {}

        # Clearing the "_cls" query should not persist across queryset instances.
        assert Parent.objects._query == {"_cls": {"$in": ("Parent", "Parent.Child")}}

        # The rest of the query should not be cleared.
        assert Parent.objects.filter(name="xyz").clear_cls_query()._query == {
            "name": "xyz"
        }

        await Parent.objects.create(name="foo")
        await Child.objects.create(name="bar", age=1)
        assert await Parent.objects.clear_cls_query().count() == 2
        assert await Parent.objects.count() == 2
        assert await Child.objects().count() == 1

        # XXX This isn't really how you'd want to use `clear_cls_query()`, but
        # it's a decent test to validate its behavior nonetheless.
        assert await Child.objects.clear_cls_query().count() == 2


    async def test_read_preference(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        await Bar.drop_collection()
        bar = await Bar.objects.create(txt="xyz")

        bars = [d async for d in Bar.objects.read_preference(ReadPreference.PRIMARY)]
        assert bars == [bar]

        bars = Bar.objects.read_preference(ReadPreference.SECONDARY_PREFERRED)
        assert bars._read_preference == ReadPreference.SECONDARY_PREFERRED
        assert (
            bars._cursor.collection.read_preference
            == ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure that `.read_preference(...)` does accept string values.
        with pytest.raises(TypeError):
            Bar.objects.read_preference("Primary")

        def assert_read_pref(qs, expected_read_pref):
            assert qs._read_preference == expected_read_pref
            assert qs._cursor.collection.read_preference == expected_read_pref

        # Make sure read preference is respected after a `.skip(...)`.
        bars = Bar.objects.skip(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after a `.limit(...)`.
        bars = Bar.objects.limit(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after an `.order_by(...)`.
        bars = Bar.objects.order_by("txt").read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)

        # Make sure read preference is respected after a `.hint(...)`.
        bars = Bar.objects.hint([("txt", 1)]).read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        assert_read_pref(bars, ReadPreference.SECONDARY_PREFERRED)


    async def test_read_concern(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        await Bar.drop_collection()
        bar = await Bar.objects.create(txt="xyz")

        bars = [d async for d in Bar.objects.read_concern(None)]
        assert bars == [bar]

        bars = Bar.objects.read_concern({"level": "local"})
        assert bars._read_concern.document == {"level": "local"}
        assert bars._cursor.collection.read_concern.document == {"level": "local"}

        # Make sure that `.read_concern(...)` does not accept string values.
        with pytest.raises(TypeError):
            Bar.objects.read_concern("local")

        def assert_read_concern(qs, expected_read_concern):
            assert qs._read_concern.document == expected_read_concern
            assert qs._cursor.collection.read_concern.document == expected_read_concern

        # Make sure read concern is respected after a `.skip(...)`.
        bars = Bar.objects.skip(1).read_concern({"level": "local"})
        assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after a `.limit(...)`.
        bars = Bar.objects.limit(1).read_concern({"level": "local"})
        assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after an `.order_by(...)`.
        bars = Bar.objects.order_by("txt").read_concern({"level": "local"})
        assert_read_concern(bars, {"level": "local"})

        # Make sure read concern is respected after a `.hint(...)`.
        bars = Bar.objects.hint([("txt", 1)]).read_concern({"level": "majority"})
        assert_read_concern(bars, {"level": "majority"})

