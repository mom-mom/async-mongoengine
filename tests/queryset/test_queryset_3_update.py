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
from mongoengine.mongodb_support import (
    MONGODB_36,
    get_mongodb_version,
)
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


class TestQueryset3(MongoDBTestCase):
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

        self.mongodb_version = get_mongodb_version()


    async def assertSequence(self, qs, expected):
        qs = [d async for d in qs]
        expected = list(expected)
        assert len(qs) == len(expected)
        for i in range(len(qs)):
            assert qs[i] == expected[i]



    async def test_reference_field_find(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person)

        await BlogPost.drop_collection()
        await self.Person.drop_collection()

        me = await self.Person(name="Test User").save()
        await BlogPost(content="test 123", author=me).save()

        assert 1 == await BlogPost.objects(author=me).count()
        assert 1 == await BlogPost.objects(author=me.pk).count()
        assert 1 == await BlogPost.objects(author="%s" % me.pk).count()

        assert 1 == await BlogPost.objects(author__in=[me]).count()
        assert 1 == await BlogPost.objects(author__in=[me.pk]).count()
        assert 1 == await BlogPost.objects(author__in=["%s" % me.pk]).count()


    async def test_reference_field_find_dbref(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, dbref=True)

        await BlogPost.drop_collection()
        await self.Person.drop_collection()

        me = await self.Person(name="Test User").save()
        await BlogPost(content="test 123", author=me).save()

        assert 1 == await BlogPost.objects(author=me).count()
        assert 1 == await BlogPost.objects(author=me.pk).count()
        assert 1 == await BlogPost.objects(author="%s" % me.pk).count()

        assert 1 == await BlogPost.objects(author__in=[me]).count()
        assert 1 == await BlogPost.objects(author__in=[me.pk]).count()
        assert 1 == await BlogPost.objects(author__in=["%s" % me.pk]).count()


    async def test_update_intfield_operator(self):
        class BlogPost(Document):
            hits = IntField()

        await BlogPost.drop_collection()

        post = BlogPost(hits=5)
        await post.save()

        await BlogPost.objects.update_one(set__hits=10)
        await post.reload()
        assert post.hits == 10

        await BlogPost.objects.update_one(inc__hits=1)
        await post.reload()
        assert post.hits == 11

        await BlogPost.objects.update_one(dec__hits=1)
        await post.reload()
        assert post.hits == 10

        # Negative dec operator is equal to a positive inc operator
        await BlogPost.objects.update_one(dec__hits=-1)
        await post.reload()
        assert post.hits == 11


    async def test_update_decimalfield_operator(self):
        class BlogPost(Document):
            review = DecimalField()

        await BlogPost.drop_collection()

        post = BlogPost(review=3.5)
        await post.save()

        await BlogPost.objects.update_one(inc__review=0.1)  # test with floats
        await post.reload()
        assert float(post.review) == 3.6

        await BlogPost.objects.update_one(dec__review=0.1)
        await post.reload()
        assert float(post.review) == 3.5

        await BlogPost.objects.update_one(inc__review=Decimal(0.12))  # test with Decimal
        await post.reload()
        assert float(post.review) == 3.62

        await BlogPost.objects.update_one(dec__review=Decimal(0.12))
        await post.reload()
        assert float(post.review) == 3.5


    async def test_update_decimalfield_operator_not_working_with_force_string(self):
        class BlogPost(Document):
            review = DecimalField(force_string=True)

        await BlogPost.drop_collection()

        post = BlogPost(review=3.5)
        await post.save()

        with pytest.raises(OperationError):
            await BlogPost.objects.update_one(inc__review=0.1)  # test with floats


    async def test_update_listfield_operator(self):
        """Ensure that atomic updates work properly."""

        class BlogPost(Document):
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = BlogPost(tags=["test"])
        await post.save()

        # ListField operator
        await BlogPost.objects.update(push__tags="mongo")
        await post.reload()
        assert "mongo" in post.tags

        await BlogPost.objects.update_one(push_all__tags=["db", "nosql"])
        await post.reload()
        assert "db" in post.tags
        assert "nosql" in post.tags

        tags = post.tags[:-1]
        await BlogPost.objects.update(pop__tags=1)
        await post.reload()
        assert post.tags == tags

        await BlogPost.objects.update_one(add_to_set__tags="unique")
        await BlogPost.objects.update_one(add_to_set__tags="unique")
        await post.reload()
        assert post.tags.count("unique") == 1

        await BlogPost.drop_collection()


    async def test_update_unset(self):
        class BlogPost(Document):
            title = StringField()

        await BlogPost.drop_collection()

        post = await BlogPost(title="garbage").save()

        assert post.title is not None
        await BlogPost.objects.update_one(unset__title=1)
        await post.reload()
        assert post.title is None
        pymongo_doc = await BlogPost.objects.as_pymongo().first()
        assert "title" not in pymongo_doc


    async def test_update_push_with_position(self):
        """Ensure that the 'push' update with position works properly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = await BlogPost.objects.create(slug="test")

        await BlogPost.objects.filter(id=post.id).update(push__tags="code")
        await BlogPost.objects.filter(id=post.id).update(push__tags__0=["mongodb", "python"])
        await post.reload()
        assert post.tags == ["mongodb", "python", "code"]

        await BlogPost.objects.filter(id=post.id).update(set__tags__2="java")
        await post.reload()
        assert post.tags == ["mongodb", "python", "java"]

        # test push with singular value
        await BlogPost.objects.filter(id=post.id).update(push__tags__0="scala")
        await post.reload()
        assert post.tags == ["scala", "mongodb", "python", "java"]


    async def test_update_push_list_of_list(self):
        """Ensure that the 'push' update operation works in the list of list"""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        await BlogPost.drop_collection()

        post = await BlogPost(slug="test").save()

        await BlogPost.objects.filter(slug="test").update(push__tags=["value1", 123])
        await post.reload()
        assert post.tags == [["value1", 123]]


    async def test_update_push_and_pull_add_to_set(self):
        """Ensure that the 'pull' update operation works correctly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = BlogPost(slug="test")
        await post.save()

        await BlogPost.objects.filter(id=post.id).update(push__tags="code")
        await post.reload()
        assert post.tags == ["code"]

        await BlogPost.objects.filter(id=post.id).update(push_all__tags=["mongodb", "code"])
        await post.reload()
        assert post.tags == ["code", "mongodb", "code"]

        await BlogPost.objects(slug="test").update(pull__tags="code")
        await post.reload()
        assert post.tags == ["mongodb"]

        await BlogPost.objects(slug="test").update(pull_all__tags=["mongodb", "code"])
        await post.reload()
        assert post.tags == []

        await BlogPost.objects(slug="test").update(
            __raw__={"$addToSet": {"tags": {"$each": ["code", "mongodb", "code"]}}}
        )
        await post.reload()
        assert post.tags == ["code", "mongodb"]


    @requires_mongodb_gte_42
    async def test_aggregation_update(self):
        """Ensure that the 'aggregation_update' update works correctly."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = BlogPost(slug="test")
        await post.save()

        await BlogPost.objects(slug="test").update(
            __raw__=[{"$set": {"slug": {"$concat": ["$slug", " ", "$slug"]}}}],
        )
        await post.reload()
        assert post.slug == "test test"

        await BlogPost.objects(slug="test test").update(
            __raw__=[
                {"$set": {"slug": {"$concat": ["$slug", " ", "it"]}}},  # test test it
                {
                    "$set": {"slug": {"$concat": ["When", " ", "$slug"]}}
                },  # When test test it
            ],
        )
        await post.reload()
        assert post.slug == "When test test it"


    async def test_combination_of_mongoengine_and__raw__(self):
        """Ensure that the '__raw__' update/query works in combination with mongoengine syntax correctly."""

        class BlogPost(Document):
            slug = StringField()
            foo = StringField()
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post = BlogPost(slug="test", foo="bar")
        await post.save()

        await BlogPost.objects(slug="test").update(
            foo="baz",
            __raw__={"$set": {"slug": "test test"}},
        )
        await post.reload()
        assert post.slug == "test test"
        assert post.foo == "baz"

        assert await BlogPost.objects(foo="baz", __raw__={"slug": "test test"}).count() == 1
        assert (
            await BlogPost.objects(foo__ne="bar", __raw__={"slug": {"$ne": "test"}}).count()
            == 1
        )
        assert (
            await BlogPost.objects(foo="baz", __raw__={"slug": {"$ne": "test test"}}).count()
            == 0
        )
        assert (
            await BlogPost.objects(foo__ne="baz", __raw__={"slug": "test test"}).count() == 0
        )
        assert (
            await BlogPost.objects(
                foo__ne="baz", __raw__={"slug": {"$ne": "test test"}}
            ).count()
            == 0
        )


    async def test_add_to_set_each(self):
        class Item(Document):
            name = StringField(required=True)
            description = StringField(max_length=50)
            parents = ListField(ReferenceField("self"))

        await Item.drop_collection()

        item = await Item(name="test item").save()
        parent_1 = await Item(name="parent 1").save()
        parent_2 = await Item(name="parent 2").save()

        await item.update(add_to_set__parents=[parent_1, parent_2, parent_1])
        await item.reload()

        assert [parent_1, parent_2] == item.parents


    async def test_pull_nested(self):
        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return "%s" % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = ListField(EmbeddedDocumentField(Collaborator))

        await Site.drop_collection()

        c = Collaborator(user="Esteban")
        s = await Site(name="test", collaborators=[c]).save()

        await Site.objects(id=s.id).update_one(pull__collaborators__user="Esteban")
        assert (await Site.objects.first()).collaborators == []

        with pytest.raises(InvalidQueryError):
            await Site.objects(id=s.id).update_one(pull_all__collaborators__user=["Ross"])


    async def test_pull_from_nested_embedded(self):
        class User(EmbeddedDocument):
            name = StringField()

            def __unicode__(self):
                return "%s" % self.name

        class Collaborator(EmbeddedDocument):
            helpful = ListField(EmbeddedDocumentField(User))
            unhelpful = ListField(EmbeddedDocumentField(User))

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = EmbeddedDocumentField(Collaborator)

        await Site.drop_collection()

        c = User(name="Esteban")
        f = User(name="Frank")
        s = await Site(
            name="test", collaborators=Collaborator(helpful=[c], unhelpful=[f])
        ).save()

        await Site.objects(id=s.id).update_one(pull__collaborators__helpful=c)
        assert (await Site.objects.first()).collaborators["helpful"] == []

        await Site.objects(id=s.id).update_one(
            pull__collaborators__unhelpful={"name": "Frank"}
        )
        assert (await Site.objects.first()).collaborators["unhelpful"] == []

        with pytest.raises(InvalidQueryError):
            await Site.objects(id=s.id).update_one(
                pull_all__collaborators__helpful__name=["Ross"]
            )


    async def test_pull_from_nested_embedded_using_in_nin(self):
        """Ensure that the 'pull' update operation works on embedded documents using 'in' and 'nin' operators."""

        class User(EmbeddedDocument):
            name = StringField()

            def __unicode__(self):
                return "%s" % self.name

        class Collaborator(EmbeddedDocument):
            helpful = ListField(EmbeddedDocumentField(User))
            unhelpful = ListField(EmbeddedDocumentField(User))

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = EmbeddedDocumentField(Collaborator)

        await Site.drop_collection()

        a = User(name="Esteban")
        b = User(name="Frank")
        x = User(name="Harry")
        y = User(name="John")

        s = await Site(
            name="test", collaborators=Collaborator(helpful=[a, b], unhelpful=[x, y])
        ).save()

        await Site.objects(id=s.id).update_one(
            pull__collaborators__helpful__name__in=["Esteban"]
        )  # Pull a
        assert (await Site.objects.first()).collaborators["helpful"] == [b]

        await Site.objects(id=s.id).update_one(
            pull__collaborators__unhelpful__name__nin=["John"]
        )  # Pull x
        assert (await Site.objects.first()).collaborators["unhelpful"] == [y]


    async def test_pull_from_nested_mapfield(self):
        class Collaborator(EmbeddedDocument):
            user = StringField()

            def __unicode__(self):
                return "%s" % self.user

        class Site(Document):
            name = StringField(max_length=75, unique=True, required=True)
            collaborators = MapField(ListField(EmbeddedDocumentField(Collaborator)))

        await Site.drop_collection()

        c = Collaborator(user="Esteban")
        f = Collaborator(user="Frank")
        s = Site(name="test", collaborators={"helpful": [c], "unhelpful": [f]})
        await s.save()

        await Site.objects(id=s.id).update_one(pull__collaborators__helpful__user="Esteban")
        assert (await Site.objects.first()).collaborators["helpful"] == []

        await Site.objects(id=s.id).update_one(
            pull__collaborators__unhelpful={"user": "Frank"}
        )
        assert (await Site.objects.first()).collaborators["unhelpful"] == []

        with pytest.raises(InvalidQueryError):
            await Site.objects(id=s.id).update_one(
                pull_all__collaborators__helpful__user=["Ross"]
            )


    async def test_pull_in_genericembedded_field(self):
        class Foo(EmbeddedDocument):
            name = StringField()

        class Bar(Document):
            foos = ListField(GenericEmbeddedDocumentField(choices=[Foo]))

        await Bar.drop_collection()

        foo = Foo(name="bar")
        bar = await Bar(foos=[foo]).save()
        await Bar.objects(id=bar.id).update(pull__foos=foo)
        await bar.reload()
        assert len(bar.foos) == 0


    async def test_update_one_check_return_with_full_result(self):
        class BlogTag(Document):
            name = StringField(required=True)

        await BlogTag.drop_collection()

        await BlogTag(name="garbage").save()
        default_update = await BlogTag.objects.update_one(name="new")
        assert default_update == 1

        full_result_update = await BlogTag.objects.update_one(name="new", full_result=True)
        assert isinstance(full_result_update, UpdateResult)


    async def test_update_one_pop_generic_reference(self):
        class BlogTag(Document):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(ReferenceField(BlogTag), required=True)

        await BlogPost.drop_collection()
        await BlogTag.drop_collection()

        tag_1 = BlogTag(name="code")
        await tag_1.save()
        tag_2 = BlogTag(name="mongodb")
        await tag_2.save()

        post = BlogPost(slug="test", tags=[tag_1])
        await post.save()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        await post.save()
        assert len(post.tags) == 2

        await BlogPost.objects(slug="test-2").update_one(pop__tags=-1)

        await post.reload()
        assert len(post.tags) == 1

        await BlogPost.drop_collection()
        await BlogTag.drop_collection()


    async def test_editting_embedded_objects(self):
        class BlogTag(EmbeddedDocument):
            name = StringField(required=True)

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(EmbeddedDocumentField(BlogTag), required=True)

        await BlogPost.drop_collection()

        tag_1 = BlogTag(name="code")
        tag_2 = BlogTag(name="mongodb")

        post = BlogPost(slug="test", tags=[tag_1])
        await post.save()

        post = BlogPost(slug="test-2", tags=[tag_1, tag_2])
        await post.save()
        assert len(post.tags) == 2

        await BlogPost.objects(slug="test-2").update_one(set__tags__0__name="python")
        await post.reload()
        assert post.tags[0].name == "python"

        await BlogPost.objects(slug="test-2").update_one(pop__tags=-1)
        await post.reload()
        assert len(post.tags) == 1

        await BlogPost.drop_collection()


    async def test_set_list_embedded_documents(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Message(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField("Author"))

        await Message.drop_collection()

        message = Message(title="hello", authors=[Author(name="Harry")])
        await message.save()

        await Message.objects(authors__name="Harry").update_one(
            set__authors__S=Author(name="Ross")
        )

        message = await message.reload()
        assert message.authors[0].name == "Ross"

        await Message.objects(authors__name="Ross").update_one(
            set__authors=[
                Author(name="Harry"),
                Author(name="Ross"),
                Author(name="Adam"),
            ]
        )

        message = await message.reload()
        assert message.authors[0].name == "Harry"
        assert message.authors[1].name == "Ross"
        assert message.authors[2].name == "Adam"


    async def test_set_generic_embedded_documents(self):
        class Bar(EmbeddedDocument):
            name = StringField()

        class User(Document):
            username = StringField()
            bar = GenericEmbeddedDocumentField(choices=[Bar])

        await User.drop_collection()

        await User(username="abc").save()
        await User.objects(username="abc").update(set__bar=Bar(name="test"), upsert=True)

        user = await User.objects(username="abc").first()
        assert user.bar.name == "test"


    async def test_reload_embedded_docs_instance(self):
        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = EmbeddedDocumentField(SubDoc)

        doc = await Doc(embedded=SubDoc(val=0)).save()
        await doc.reload()

        assert doc.pk == doc.embedded._instance.pk


    async def test_reload_list_embedded_docs_instance(self):
        class SubDoc(EmbeddedDocument):
            val = IntField()

        class Doc(Document):
            embedded = ListField(EmbeddedDocumentField(SubDoc))

        doc = await Doc(embedded=[SubDoc(val=0)]).save()
        await doc.reload()

        assert doc.pk == doc.embedded[0]._instance.pk


    async def test_order_by(self):
        """Ensure that QuerySets may be ordered."""
        await self.Person(name="User B", age=40).save()
        await self.Person(name="User A", age=20).save()
        await self.Person(name="User C", age=30).save()

        names = [p.name async for p in self.Person.objects.order_by("-age")]
        assert names == ["User B", "User C", "User A"]

        names = [p.name async for p in self.Person.objects.order_by("+age")]
        assert names == ["User A", "User C", "User B"]

        names = [p.name async for p in self.Person.objects.order_by("age")]
        assert names == ["User A", "User C", "User B"]

        ages = [p.age async for p in self.Person.objects.order_by("-name")]
        assert ages == [30, 40, 20]

        ages = [p.age async for p in self.Person.objects.order_by()]
        assert ages == [40, 20, 30]

        ages = [p.age async for p in self.Person.objects.order_by("")]
        assert ages == [40, 20, 30]


    async def test_order_by_optional(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        await BlogPost.drop_collection()

        blog_post_3 = await BlogPost.objects.create(
            title="Blog Post #3", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.objects.create(
            title="Blog Post #2", published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_4 = await BlogPost.objects.create(
            title="Blog Post #4", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )
        blog_post_1 = await BlogPost.objects.create(title="Blog Post #1", published_date=None)

        expected = [blog_post_1, blog_post_2, blog_post_3, blog_post_4]
        await self.assertSequence(BlogPost.objects.order_by("published_date"), expected)
        await self.assertSequence(BlogPost.objects.order_by("+published_date"), expected)

        expected.reverse()
        await self.assertSequence(BlogPost.objects.order_by("-published_date"), expected)


    async def test_order_by_list(self):
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField(required=False)

        await BlogPost.drop_collection()

        blog_post_1 = await BlogPost.objects.create(
            title="A", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.objects.create(
            title="B", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = await BlogPost.objects.create(
            title="C", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        qs = BlogPost.objects.order_by("published_date", "title")
        expected = [blog_post_1, blog_post_2, blog_post_3]
        await self.assertSequence(qs, expected)

        qs = BlogPost.objects.order_by("-published_date", "-title")
        expected.reverse()
        await self.assertSequence(qs, expected)


    async def test_order_by_chaining(self):
        """Ensure that an order_by query chains properly and allows .only()"""
        await self.Person(name="User B", age=40).save()
        await self.Person(name="User A", age=20).save()
        await self.Person(name="User C", age=30).save()

        only_age = self.Person.objects.order_by("-age").only("age")

        names = [p.name async for p in only_age]
        ages = [p.age async for p in only_age]

        # The .only('age') clause should mean that all names are None
        assert names == [None, None, None]
        assert ages == [40, 30, 20]

        qs = self.Person.objects.all().order_by("-age")
        qs = qs.limit(10)
        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]

        qs = self.Person.objects.all().limit(10)
        qs = qs.order_by("-age")

        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]

        qs = self.Person.objects.all().skip(0)
        qs = qs.order_by("-age")
        ages = [p.age async for p in qs]
        assert ages == [40, 30, 20]


    async def test_order_by_using_raw(self):
        person_a = self.Person(name="User A", age=20)
        await person_a.save()
        person_b = self.Person(name="User B", age=30)
        await person_b.save()
        person_c = self.Person(name="User B", age=25)
        await person_c.save()
        person_d = self.Person(name="User C", age=40)
        await person_d.save()

        qs = self.Person.objects.order_by(__raw__=[("name", pymongo.DESCENDING)])
        assert qs._ordering == [("name", pymongo.DESCENDING)]
        names = [p.name async for p in qs]
        assert names == ["User C", "User B", "User B", "User A"]

        names = [
            (p.name, p.age)
            async for p in self.Person.objects.order_by(__raw__=[("name", pymongo.ASCENDING)])
        ]
        assert names == [("User A", 20), ("User B", 30), ("User B", 25), ("User C", 40)]

        if PYMONGO_VERSION >= (4, 4):
            # Pymongo >= 4.4 allow to mix single key with tuples inside the list
            qs = self.Person.objects.order_by(
                __raw__=["name", ("age", pymongo.ASCENDING)]
            )
            names = [(p.name, p.age) async for p in qs]
            assert names == [
                ("User A", 20),
                ("User B", 25),
                ("User B", 30),
                ("User C", 40),
            ]


    async def test_order_by_using_raw_and_keys_raises_exception(self):
        with pytest.raises(OperationError):
            self.Person.objects.order_by("-name", __raw__=[("age", pymongo.ASCENDING)])


    async def test_confirm_order_by_reference_wont_work(self):
        """Ordering by reference is not possible.  Use map / reduce.. or
            denormalise"""

        class Author(Document):
            author = ReferenceField(self.Person)

        await Author.drop_collection()

        person_a = self.Person(name="User A", age=20)
        await person_a.save()
        person_b = self.Person(name="User B", age=40)
        await person_b.save()
        person_c = self.Person(name="User C", age=30)
        await person_c.save()

        await Author(author=person_a).save()
        await Author(author=person_b).save()
        await Author(author=person_c).save()

        names = [a.author.name async for a in Author.objects.order_by("-author__age")]
        assert names == ["User A", "User B", "User C"]


    async def test_comment(self):
        """Make sure adding a comment to the query gets added to the query"""
        MONGO_VER = self.mongodb_version
        _, CMD_QUERY_KEY = get_key_compat(MONGO_VER)
        QUERY_KEY = "filter"
        COMMENT_KEY = "comment"

        class User(Document):
            age = IntField()

        with db_ops_tracker() as q:
            await User.objects.filter(age__gte=18).comment("looking for an adult").first()
            await User.objects.comment("looking for an adult").filter(age__gte=18).first()

            ops = q.get_ops()
            assert len(ops) == 2
            for op in ops:
                assert op[CMD_QUERY_KEY][QUERY_KEY] == {"age": {"$gte": 18}}
                assert op[CMD_QUERY_KEY][COMMENT_KEY] == "looking for an adult"


    async def test_map_reduce(self):
        """Ensure map/reduce is both mapping and reducing."""

        class BlogPost(Document):
            title = StringField()
            tags = ListField(StringField(), db_field="post-tag-list")

        await BlogPost.drop_collection()

        await BlogPost(title="Post #1", tags=["music", "film", "print"]).save()
        await BlogPost(title="Post #2", tags=["music", "film"]).save()
        await BlogPost(title="Post #3", tags=["film", "photography"]).save()

        map_f = """
                function() {
                    this[~tags].forEach(function(tag) {
                        emit(tag, 1);
                    });
                }
            """

        reduce_f = """
                function(key, values) {
                    var total = 0;
                    for(var i=0; i<values.length; i++) {
                        total += values[i];
                    }
                    return total;
                }
            """

        # run a map/reduce operation spanning all posts
        results = await BlogPost.objects.map_reduce(map_f, reduce_f, "myresults")
        results = [d async for d in results]
        assert len(results) == 4

        music = list(filter(lambda r: r.key == "music", results))[0]
        assert music.value == 2

        film = list(filter(lambda r: r.key == "film", results))[0]
        assert film.value == 3

        await BlogPost.drop_collection()


    async def test_map_reduce_with_custom_object_ids(self):
        """Ensure that QuerySet.map_reduce works properly with custom
            primary keys.
            """

        class BlogPost(Document):
            title = StringField(primary_key=True)
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        post1 = BlogPost(title="Post #1", tags=["mongodb", "mongoengine"])
        post2 = BlogPost(title="Post #2", tags=["django", "mongodb"])
        post3 = BlogPost(title="Post #3", tags=["hitchcock films"])

        await post1.save()
        await post2.save()
        await post3.save()

        assert BlogPost._fields["title"].db_field == "_id"
        assert BlogPost._meta["id_field"] == "title"

        map_f = """
                function() {
                    emit(this._id, 1);
                }
            """

        # reduce to a list of tag ids and counts
        reduce_f = """
                function(key, values) {
                    var total = 0;
                    for(var i=0; i<values.length; i++) {
                        total += values[i];
                    }
                    return total;
                }
            """

        results = await BlogPost.objects.order_by("_id").map_reduce(
            map_f, reduce_f, "myresults2"
        )
        results = [d async for d in results]

        assert len(results) == 3
        assert results[0].object.id == post1.id
        assert results[1].object.id == post2.id
        assert results[2].object.id == post3.id

        await BlogPost.drop_collection()


    async def test_map_reduce_custom_output(self):
        """
            Test map/reduce custom output
            """

        class Family(Document):
            id = IntField(primary_key=True)
            log = StringField()

        class Person(Document):
            id = IntField(primary_key=True)
            name = StringField()
            age = IntField()
            family = ReferenceField(Family)

        await Family.drop_collection()
        await Person.drop_collection()

        # creating first family
        f1 = Family(id=1, log="Trav 02 de Julho")
        await f1.save()

        # persons of first family
        await Person(id=1, family=f1, name="Wilson Jr", age=21).save()
        await Person(id=2, family=f1, name="Wilson Father", age=45).save()
        await Person(id=3, family=f1, name="Eliana Costa", age=40).save()
        await Person(id=4, family=f1, name="Tayza Mariana", age=17).save()

        # creating second family
        f2 = Family(id=2, log="Av prof frasc brunno")
        await f2.save()

        # persons of second family
        await Person(id=5, family=f2, name="Isabella Luanna", age=16).save()
        await Person(id=6, family=f2, name="Sandra Mara", age=36).save()
        await Person(id=7, family=f2, name="Igor Gabriel", age=10).save()

        # creating third family
        f3 = Family(id=3, log="Av brazil")
        await f3.save()

        # persons of thrird family
        await Person(id=8, family=f3, name="Arthur WA", age=30).save()
        await Person(id=9, family=f3, name="Paula Leonel", age=25).save()

        # executing join map/reduce
        map_person = """
                function () {
                    emit(this.family, {
                         totalAge: this.age,
                         persons: [{
                            name: this.name,
                            age: this.age
                    }]});
                }
            """

        map_family = """
                function () {
                    emit(this._id, {
                       totalAge: 0,
                       persons: []
                    });
                }
            """

        reduce_f = """
                function (key, values) {
                    var family = {persons: [], totalAge: 0};

                    values.forEach(function(value) {
                        if (value.persons) {
                            value.persons.forEach(function (person) {
                                family.persons.push(person);
                                family.totalAge += person.age;
                            });
                            family.persons.sort((a, b) => (a.age > b.age))
                        }
                    });

                    return family;
                }
            """
        cursor = await Family.objects.map_reduce(
            map_f=map_family,
            reduce_f=reduce_f,
            output={"replace": "family_map", "db_alias": "test2"},
        )

        # start a map/reduce
        await cursor.__anext__()

        results = await Person.objects.map_reduce(
            map_f=map_person,
            reduce_f=reduce_f,
            output={"reduce": "family_map", "db_alias": "test2"},
        )

        results = [d async for d in results]
        collection = get_db("test2").family_map

        assert await collection.find_one({"_id": 1}) == {
            "_id": 1,
            "value": {
                "persons": [
                    {"age": 17, "name": "Tayza Mariana"},
                    {"age": 21, "name": "Wilson Jr"},
                    {"age": 40, "name": "Eliana Costa"},
                    {"age": 45, "name": "Wilson Father"},
                ],
                "totalAge": 123,
            },
        }

        assert await collection.find_one({"_id": 2}) == {
            "_id": 2,
            "value": {
                "persons": [
                    {"age": 10, "name": "Igor Gabriel"},
                    {"age": 16, "name": "Isabella Luanna"},
                    {"age": 36, "name": "Sandra Mara"},
                ],
                "totalAge": 62,
            },
        }

        assert await collection.find_one({"_id": 3}) == {
            "_id": 3,
            "value": {
                "persons": [
                    {"age": 25, "name": "Paula Leonel"},
                    {"age": 30, "name": "Arthur WA"},
                ],
                "totalAge": 55,
            },
        }


    async def test_map_reduce_finalize(self):
        """Ensure that map, reduce, and finalize run and introduce "scope"
            by simulating "hotness" ranking with Reddit algorithm.
            """
        from time import mktime

        class Link(Document):
            title = StringField(db_field="bpTitle")
            up_votes = IntField()
            down_votes = IntField()
            submitted = DateTimeField(db_field="sTime")

        await Link.drop_collection()

        now = datetime.datetime.utcnow()

        # Note: Test data taken from a custom Reddit homepage on
        # Fri, 12 Feb 2010 14:36:00 -0600. Link ordering should
        # reflect order of insertion below, but is not influenced
        # by insertion order.
        await Link(
            title="Google Buzz auto-followed a woman's abusive ex ...",
            up_votes=1079,
            down_votes=553,
            submitted=now - datetime.timedelta(hours=4),
        ).save()
        await Link(
            title="We did it! Barbie is a computer engineer.",
            up_votes=481,
            down_votes=124,
            submitted=now - datetime.timedelta(hours=2),
        ).save()
        await Link(
            title="This Is A Mosquito Getting Killed By A Laser",
            up_votes=1446,
            down_votes=530,
            submitted=now - datetime.timedelta(hours=13),
        ).save()
        await Link(
            title="Arabic flashcards land physics student in jail.",
            up_votes=215,
            down_votes=105,
            submitted=now - datetime.timedelta(hours=6),
        ).save()
        await Link(
            title="The Burger Lab: Presenting, the Flood Burger",
            up_votes=48,
            down_votes=17,
            submitted=now - datetime.timedelta(hours=5),
        ).save()
        await Link(
            title="How to see polarization with the naked eye",
            up_votes=74,
            down_votes=13,
            submitted=now - datetime.timedelta(hours=10),
        ).save()

        map_f = """
                function() {
                    emit(this[~id], {up_delta: this[~up_votes] - this[~down_votes],
                                    sub_date: this[~submitted].getTime() / 1000})
                }
            """

        reduce_f = """
                function(key, values) {
                    data = values[0];

                    x = data.up_delta;

                    // calculate time diff between reddit epoch and submission
                    sec_since_epoch = data.sub_date - reddit_epoch;

                    // calculate 'Y'
                    if(x > 0) {
                        y = 1;
                    } else if (x = 0) {
                        y = 0;
                    } else {
                        y = -1;
                    }

                    // calculate 'Z', the maximal value
                    if(Math.abs(x) >= 1) {
                        z = Math.abs(x);
                    } else {
                        z = 1;
                    }

                    return {x: x, y: y, z: z, t_s: sec_since_epoch};
                }
            """

        finalize_f = """
                function(key, value) {
                    // f(sec_since_epoch,y,z) =
                    //                    log10(z) + ((y*sec_since_epoch) / 45000)
                    z_10 = Math.log(value.z) / Math.log(10);
                    weight = z_10 + ((value.y * value.t_s) / 45000);
                    return weight;
                }
            """

        # provide the reddit epoch (used for ranking) as a variable available
        # to all phases of the map/reduce operation: map, reduce, and finalize.
        reddit_epoch = mktime(datetime.datetime(2005, 12, 8, 7, 46, 43).timetuple())
        scope = {"reddit_epoch": reddit_epoch}

        # run a map/reduce operation across all links. ordering is set
        # to "-value", which orders the "weight" value returned from
        # "finalize_f" in descending order.
        results = Link.objects.order_by("-value")
        results = await results.map_reduce(
            map_f, reduce_f, "myresults", finalize_f=finalize_f, scope=scope
        )
        results = [d async for d in results]

        # assert troublesome Buzz article is ranked 1st
        assert results[0].object.title.startswith("Google Buzz")

        # assert laser vision is ranked last
        assert results[-1].object.title.startswith("How to see")

        await Link.drop_collection()

