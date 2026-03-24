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


class TestQueryset1(MongoDBTestCase):
    async def setup_method(self, method=None):
        connect(db="mongoenginetest")
        connect(db="mongoenginetest2", alias="test2")

        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {"allow_inheritance": True}

        await Person.drop_collection()
        self.PersonMeta = PersonMeta
        self.Person = Person

        self.mongodb_version = get_mongodb_version()


    async def assertSequence(self, qs, expected):
        qs = [d async for d in qs]
        expected = list(expected)
        assert len(qs) == len(expected)
        for i in range(len(qs)):
            assert qs[i] == expected[i]


    async def teardown_method(self, method=None):
        await self.Person.drop_collection()


    async def test_initialisation(self):
        """Ensure that a QuerySet is correctly initialised by QuerySetManager."""
        assert isinstance(self.Person.objects, QuerySet)
        assert (
            self.Person.objects._collection.name == self.Person._get_collection_name()
        )
        assert isinstance(
            self.Person.objects._collection, pymongo.collection.Collection
        )


    async def test_cannot_perform_joins_references(self):
        class BlogPost(Document):
            author = ReferenceField(self.Person)
            author2 = GenericReferenceField()

        # test addressing a field from a reference
        with pytest.raises(InvalidQueryError):
            [d async for d in BlogPost.objects(author__name="test")]

        # should fail for a generic reference as well
        with pytest.raises(InvalidQueryError):
            [d async for d in BlogPost.objects(author2__name="test")]


    async def test_find(self):
        """Ensure that a query returns a valid set of results."""
        user_a = await self.Person.objects.create(name="User A", age=20)
        user_b = await self.Person.objects.create(name="User B", age=30)

        # Find all people in the collection
        people = self.Person.objects
        assert await people.count() == 2
        results = [d async for d in people]

        assert isinstance(results[0], self.Person)
        assert isinstance(results[0].id, ObjectId)

        assert results[0] == user_a
        assert results[0].name == "User A"
        assert results[0].age == 20

        assert results[1] == user_b
        assert results[1].name == "User B"
        assert results[1].age == 30

        # Filter people by age
        people = self.Person.objects(age=20)
        assert await people.count() == 1
        person = await people.__anext__()
        assert person == user_a
        assert person.name == "User A"
        assert person.age == 20


    async def test_slicing_sets_empty_limit_skip(self):
        await self.Person.objects.insert(
            [self.Person(name=f"User {i}", age=i) for i in range(5)],
            load_bulk=False,
        )

        await self.Person.objects.create(name="User B", age=30)
        await self.Person.objects.create(name="User C", age=40)

        qs = self.Person.objects()[1:2]
        assert (qs._empty, qs._skip, qs._limit) == (False, 1, 1)
        assert len([d async for d in qs]) == 1

        # Test edge case of [1:1] which should return nothing
        # and require a hack so that it doesn't clash with limit(0)
        qs = self.Person.objects()[1:1]
        assert (qs._empty, qs._skip, qs._limit) == (True, 1, 0)
        assert len([d async for d in qs]) == 0

        qs2 = qs[1:5]  # Make sure that further slicing resets _empty
        assert (qs2._empty, qs2._skip, qs2._limit) == (False, 1, 4)
        assert len([d async for d in qs2]) == 4


    async def test_limit_0_returns_all_documents(self):
        await self.Person.objects.create(name="User A", age=20)
        await self.Person.objects.create(name="User B", age=30)

        n_docs = await self.Person.objects().count()

        persons = [d async for d in self.Person.objects().limit(0)]
        assert len(persons) == 2 == n_docs


    async def test_limit_0(self):
        """Ensure that QuerySet.limit works as expected."""
        await self.Person.objects.create(name="User A", age=20)

        # Test limit with 0 as parameter
        qs = self.Person.objects.limit(0)
        assert await qs.count() == 0


    async def test_limit(self):
        """Ensure that QuerySet.limit works as expected."""
        user_a = await self.Person.objects.create(name="User A", age=20)
        _ = await self.Person.objects.create(name="User B", age=30)

        # Test limit on a new queryset
        people = [d async for d in self.Person.objects.limit(1)]
        assert len(people) == 1
        assert people[0] == user_a

        # Test limit on an existing queryset
        people = self.Person.objects
        assert await people.count() == 2
        people2 = people.limit(1)
        assert await people.count() == 2
        assert await people2.count() == 1
        assert await people2.get_item(0) == user_a

        # Test limit with 0 as parameter
        people = self.Person.objects.limit(0)
        assert await people.count(with_limit_and_skip=True) == 2
        assert await people.count() == 2

        # Test chaining of only after limit
        person = await self.Person.objects().limit(1).only("name").first()
        assert person == user_a
        assert person.name == "User A"
        assert person.age is None


    async def test_skip(self):
        """Ensure that QuerySet.skip works as expected."""
        user_a = await self.Person.objects.create(name="User A", age=20)
        user_b = await self.Person.objects.create(name="User B", age=30)

        # Test skip on a new queryset
        people = [d async for d in self.Person.objects.skip(0)]
        assert len(people) == 2
        assert people[0] == user_a
        assert people[1] == user_b

        people = [d async for d in self.Person.objects.skip(1)]
        assert len(people) == 1
        assert people[0] == user_b

        # Test skip on an existing queryset
        people = self.Person.objects
        assert await people.count() == 2
        people2 = people.skip(1)
        assert await people.count() == 2
        assert await people2.count() == 1
        assert await people2.get_item(0) == user_b

        # Test chaining of only after skip
        person = await self.Person.objects().skip(1).only("name").first()
        assert person == user_b
        assert person.name == "User B"
        assert person.age is None


    async def test___getitem___invalid_index(self):
        """Ensure slicing a queryset works as expected."""
        with pytest.raises(TypeError):
            self.Person.objects()["a"]


    async def test_slice(self):
        """Ensure slicing a queryset works as expected."""
        user_a = await self.Person.objects.create(name="User A", age=20)
        user_b = await self.Person.objects.create(name="User B", age=30)
        user_c = await self.Person.objects.create(name="User C", age=40)

        # Test slice limit
        people = [d async for d in self.Person.objects[:2]]
        assert len(people) == 2
        assert people[0] == user_a
        assert people[1] == user_b

        # Test slice skip
        people = [d async for d in self.Person.objects[1:]]
        assert len(people) == 2
        assert people[0] == user_b
        assert people[1] == user_c

        # Test slice limit and skip
        people = [d async for d in self.Person.objects[1:2]]
        assert len(people) == 1
        assert people[0] == user_b

        # Test slice limit and skip on an existing queryset
        people = self.Person.objects
        assert await people.count() == 3
        people2 = people[1:2]
        assert await people2.count() == 1
        assert await people2.get_item(0) == user_b

        # Test slice limit and skip cursor reset
        qs = self.Person.objects[1:2]
        # fetch then delete the cursor
        qs._cursor
        qs._cursor_obj = None
        people = [d async for d in qs]
        assert len(people) == 1
        assert people[0].name == "User B"

        # Test empty slice
        people = [d async for d in self.Person.objects[1:1]]
        assert len(people) == 0

        # Test slice out of range
        people = [d async for d in self.Person.objects[80000:80001]]
        assert len(people) == 0

        # Test larger slice __repr__
        await self.Person.objects.delete()
        for i in range(55):
            await self.Person(name="A%s" % i, age=i).save()

        assert await self.Person.objects.count() == 55
        assert "Person object" == "%s" % await self.Person.objects.get_item(0)
        assert (
            "[<Person: Person object>, <Person: Person object>]"
            == "%s" % self.Person.objects[1:3]
        )
        assert (
            "[<Person: Person object>, <Person: Person object>]"
            == "%s" % self.Person.objects[51:53]
        )


    async def test_find_one(self):
        """Ensure that a query using find_one returns a valid result."""
        person1 = self.Person(name="User A", age=20)
        await person1.save()
        person2 = self.Person(name="User B", age=30)
        await person2.save()

        # Retrieve the first person from the database
        person = await self.Person.objects.first()
        assert isinstance(person, self.Person)
        assert person.name == "User A"
        assert person.age == 20

        # Use a query to filter the people found to just person2
        person = await self.Person.objects(age=30).first()
        assert person.name == "User B"

        person = await self.Person.objects(age__lt=30).first()
        assert person.name == "User A"

        # Use array syntax
        person = await self.Person.objects.get_item(0)
        assert person.name == "User A"

        person = await self.Person.objects.get_item(1)
        assert person.name == "User B"

        with pytest.raises(IndexError):
            await self.Person.objects.get_item(2)

        # Find a document using just the object id
        person = await self.Person.objects.with_id(person1.id)
        assert person.name == "User A"

        with pytest.raises(InvalidQueryError):
            await self.Person.objects(name="User A").with_id(person1.id)


    async def test_get_no_document_exists_raises_doesnotexist(self):
        assert await self.Person.objects.count() == 0
        # Try retrieving when no objects exists
        with pytest.raises(DoesNotExist):
            await self.Person.objects.get()
        with pytest.raises(self.Person.DoesNotExist):
            await self.Person.objects.get()


    async def test_get_multiple_match_raises_multipleobjectsreturned(self):
        """Ensure that a query using ``get`` returns at most one result."""
        assert await self.Person.objects().count() == 0

        person1 = self.Person(name="User A", age=20)
        await person1.save()

        p = await self.Person.objects.get()
        assert p == person1

        person2 = self.Person(name="User B", age=20)
        await person2.save()

        person3 = self.Person(name="User C", age=30)
        await person3.save()

        # .get called without argument
        with pytest.raises(MultipleObjectsReturned):
            await self.Person.objects.get()
        with pytest.raises(self.Person.MultipleObjectsReturned):
            await self.Person.objects.get()

        # check filtering
        with pytest.raises(MultipleObjectsReturned):
            await self.Person.objects.get(age__lt=30)
        with pytest.raises(MultipleObjectsReturned) as exc_info:
            await self.Person.objects(age__lt=30).get()
        assert "2 or more items returned, instead of 1" == str(exc_info.value)

        # Use a query to filter the people found to just person2
        person = await self.Person.objects.get(age=30)
        assert person == person3


    async def test_find_array_position(self):
        """Ensure that query by array position works."""

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        await Blog.drop_collection()

        await Blog.objects.create(tags=["a", "b"])
        assert await Blog.objects(tags__0="a").count() == 1
        assert await Blog.objects(tags__0="b").count() == 0
        assert await Blog.objects(tags__1="a").count() == 0
        assert await Blog.objects(tags__1="b").count() == 1

        await Blog.drop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = await Blog.objects.create(posts=[post1, post2])
        blog2 = await Blog.objects.create(posts=[post2, post1])

        blog = await Blog.objects(posts__0__comments__0__name="testa").get()
        assert blog == blog1

        blog = await Blog.objects(posts__0__comments__0__name="testb").get()
        assert blog == blog2

        query = Blog.objects(posts__1__comments__1__name="testb")
        assert await query.count() == 2

        query = Blog.objects(posts__1__comments__1__name="testa")
        assert await query.count() == 0

        query = Blog.objects(posts__0__comments__1__name="testa")
        assert await query.count() == 0

        await Blog.drop_collection()


    async def test_none(self):
        class A(Document):
            s = StringField()

        await A.drop_collection()
        await A().save()

        # validate collection not empty
        assert await A.objects.count() == 1

        # update operations
        assert await A.objects.none().update(s="1") == 0
        assert await A.objects.none().update_one(s="1") == 0
        assert await A.objects.none().modify(s="1") is None

        # validate noting change by update operations
        assert await A.objects(s="1").count() == 0

        # fetch queries
        assert await A.objects.none().first() is None
        assert [d async for d in A.objects.none()] == []
        assert [d async for d in A.objects.none().all()] == []
        assert [d async for d in A.objects.none().limit(1)] == []
        assert [d async for d in A.objects.none().skip(1)] == []
        assert [d async for d in A.objects.none()[:5]] == []


    async def test_chaining(self):
        class A(Document):
            s = StringField()

        class B(Document):
            ref = ReferenceField(A)
            boolfield = BooleanField(default=False)

        await A.drop_collection()
        await B.drop_collection()

        a1 = await A(s="test1").save()
        a2 = await A(s="test2").save()

        await B(ref=a1, boolfield=True).save()

        # Works
        q1 = B.objects.filter(ref__in=[a1, a2], ref=a1)._query

        # Doesn't work
        q2 = B.objects.filter(ref__in=[a1, a2])
        q2 = q2.filter(ref=a1)._query
        assert q1 == q2

        a_objects = A.objects(s="test1")
        query = B.objects(ref__in=a_objects)
        query = query.filter(boolfield=True)
        assert await query.count() == 1


    async def test_batch_size(self):
        """Ensure that batch_size works."""

        class A(Document):
            s = StringField()

        await A.drop_collection()

        await A.objects.insert([A(s=str(i)) for i in range(100)], load_bulk=True)

        # test iterating over the result set
        cnt = 0
        async for _ in A.objects.batch_size(10):
            cnt += 1
        assert cnt == 100

        # test chaining
        qs = A.objects.all()
        qs = qs.limit(10).batch_size(20).skip(91)
        cnt = 0
        async for _ in qs:
            cnt += 1
        assert cnt == 9

        # test invalid batch size
        qs = A.objects.batch_size(-1)
        with pytest.raises(ValueError):
            [d async for d in qs]


    async def test_batch_size_cloned(self):
        class A(Document):
            s = StringField()

        # test that batch size gets cloned
        qs = A.objects.batch_size(5)
        assert qs._batch_size == 5
        qs_clone = qs.clone()
        assert qs_clone._batch_size == 5


    async def test_update_write_concern(self):
        """Test that passing write_concern works"""
        await self.Person.drop_collection()

        write_concern = {"fsync": True}
        author = await self.Person.objects.create(name="Test User")
        await author.save(write_concern=write_concern)

        # Ensure no regression of #1958
        author = self.Person(name="Test User2")
        await author.save(write_concern=None)  # will default to {w: 1}

        result = await self.Person.objects.update(set__name="Ross", write_concern={"w": 1})

        assert result == 2
        result = await self.Person.objects.update(set__name="Ross", write_concern={"w": 0})
        assert result is None

        result = await self.Person.objects.update_one(
            set__name="Test User", write_concern={"w": 1}
        )
        assert result == 1
        result = await self.Person.objects.update_one(
            set__name="Test User", write_concern={"w": 0}
        )
        assert result is None


    async def test_update_update_has_a_value(self):
        """Test to ensure that update is passed a value to update to"""
        await self.Person.drop_collection()

        author = await self.Person.objects.create(name="Test User")

        with pytest.raises(OperationError):
            await self.Person.objects(pk=author.pk).update({})

        with pytest.raises(OperationError):
            await self.Person.objects(pk=author.pk).update_one({})


    async def test_update_array_position(self):
        """Ensure that updating by array position works.

            Check update() and update_one() can take syntax like:
                set__posts__1__comments__1__name="testc"
            Check that it only works for ListFields.
            """

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        await Blog.drop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        await Blog.objects.create(posts=[post1, post2])
        await Blog.objects.create(posts=[post2, post1])

        # Update all of the first comments of second posts of all blogs
        await Blog.objects().update(set__posts__1__comments__0__name="testc")
        testc_blogs = Blog.objects(posts__1__comments__0__name="testc")
        assert await testc_blogs.count() == 2

        await Blog.drop_collection()
        await Blog.objects.create(posts=[post1, post2])
        await Blog.objects.create(posts=[post2, post1])

        # Update only the first blog returned by the query
        await Blog.objects().update_one(set__posts__1__comments__1__name="testc")
        testc_blogs = Blog.objects(posts__1__comments__1__name="testc")
        assert await testc_blogs.count() == 1

        # Check that using this indexing syntax on a non-list fails
        with pytest.raises(InvalidQueryError):
            await Blog.objects().update(set__posts__1__comments__0__name__1="asdf")

        await Blog.drop_collection()


    async def test_update_array_filters(self):
        """Ensure that updating by array_filters works."""

        class Comment(EmbeddedDocument):
            comment_tags = ListField(StringField())

        class Blog(Document):
            tags = ListField(StringField())
            comments = EmbeddedDocumentField(Comment)

        await Blog.drop_collection()

        # update one
        await Blog.objects.create(tags=["test1", "test2", "test3"])

        await Blog.objects().update_one(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.objects(tags="test11111")

        assert await testc_blogs.count() == 1

        # modify
        await Blog.drop_collection()

        # update one
        await Blog.objects.create(tags=["test1", "test2", "test3"])

        new_blog = await Blog.objects().modify(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
            new=True,
        )
        testc_blogs = Blog.objects(tags="test11111")
        assert await new_blog == testc_blogs.first()

        assert await testc_blogs.count() == 1

        await Blog.drop_collection()

        # update one inner list
        comments = Comment(comment_tags=["test1", "test2", "test3"])
        await Blog.objects.create(comments=comments)

        await Blog.objects().update_one(
            __raw__={"$set": {"comments.comment_tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.objects(comments__comment_tags="test11111")

        assert await testc_blogs.count() == 1

        # update many
        await Blog.drop_collection()

        await Blog.objects.create(tags=["test1", "test2", "test3", "test_all"])
        await Blog.objects.create(tags=["test4", "test5", "test6", "test_all"])

        await Blog.objects().update(
            __raw__={"$set": {"tags.$[element]": "test11111"}},
            array_filters=[{"element": {"$eq": "test2"}}],
        )
        testc_blogs = Blog.objects(tags="test11111")

        assert await testc_blogs.count() == 1

        await Blog.objects().update(
            __raw__={"$set": {"tags.$[element]": "test_all1234577"}},
            array_filters=[{"element": {"$eq": "test_all"}}],
        )
        testc_blogs = Blog.objects(tags="test_all1234577")

        assert await testc_blogs.count() == 2


    async def test_update_using_positional_operator(self):
        """Ensure that the list fields can be updated using the positional
            operator."""

        class Comment(EmbeddedDocument):
            by = StringField()
            votes = IntField()

        class BlogPost(Document):
            title = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))

        await BlogPost.drop_collection()

        c1 = Comment(by="joe", votes=3)
        c2 = Comment(by="jane", votes=7)

        await BlogPost(title="ABC", comments=[c1, c2]).save()

        await BlogPost.objects(comments__by="jane").update(inc__comments__S__votes=1)

        post = await BlogPost.objects.first()
        assert post.comments[1].by == "jane"
        assert post.comments[1].votes == 8


    async def test_update_using_positional_operator_matches_first(self):
        # Currently the $ operator only applies to the first matched item in
        # the query

        class Simple(Document):
            x = ListField()

        await Simple.drop_collection()
        await Simple(x=[1, 2, 3, 2]).save()
        await Simple.objects(x=2).update(inc__x__S=1)

        simple = await Simple.objects.first()
        assert simple.x == [1, 3, 3, 2]
        await Simple.drop_collection()

        # You can set multiples
        await Simple.drop_collection()
        await Simple(x=[1, 2, 3, 4]).save()
        await Simple(x=[2, 3, 4, 5]).save()
        await Simple(x=[3, 4, 5, 6]).save()
        await Simple(x=[4, 5, 6, 7]).save()
        await Simple.objects(x=3).update(set__x__S=0)

        s = Simple.objects()
        assert (await s.get_item(0)).x == [1, 2, 0, 4]
        assert (await s.get_item(1)).x == [2, 0, 4, 5]
        assert (await s.get_item(2)).x == [0, 4, 5, 6]
        assert (await s.get_item(3)).x == [4, 5, 6, 7]

        # Using "$unset" with an expression like this "array.$" will result in
        # the array item becoming None, not being removed.
        await Simple.drop_collection()
        await Simple(x=[1, 2, 3, 4, 3, 2, 3, 4]).save()
        await Simple.objects(x=3).update(unset__x__S=1)
        simple = await Simple.objects.first()
        assert simple.x == [1, 2, None, 4, 3, 2, 3, 4]

        # Nested updates arent supported yet..
        with pytest.raises(OperationError):
            await Simple.drop_collection()
            await Simple(x=[{"test": [1, 2, 3, 4]}]).save()
            await Simple.objects(x__test=2).update(set__x__S__test__S=3)
            assert simple.x == [1, 2, 3, 4]


    async def test_update_using_positional_operator_embedded_document(self):
        """Ensure that the embedded documents can be updated using the positional
            operator."""

        class Vote(EmbeddedDocument):
            score = IntField()

        class Comment(EmbeddedDocument):
            by = StringField()
            votes = EmbeddedDocumentField(Vote)

        class BlogPost(Document):
            title = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))

        await BlogPost.drop_collection()

        c1 = Comment(by="joe", votes=Vote(score=3))
        c2 = Comment(by="jane", votes=Vote(score=7))

        await BlogPost(title="ABC", comments=[c1, c2]).save()

        await BlogPost.objects(comments__by="joe").update(
            set__comments__S__votes=Vote(score=4)
        )

        post = await BlogPost.objects.first()
        assert post.comments[0].by == "joe"
        assert post.comments[0].votes.score == 4


    async def test_update_min_max(self):
        class Scores(Document):
            high_score = IntField()
            low_score = IntField()

        scores = await Scores.objects.create(high_score=800, low_score=200)

        await Scores.objects(id=scores.id).update(min__low_score=150)
        assert (await Scores.objects.get(id=scores.id)).low_score == 150
        await Scores.objects(id=scores.id).update(min__low_score=250)
        assert (await Scores.objects.get(id=scores.id)).low_score == 150

        await Scores.objects(id=scores.id).update(max__high_score=1000)
        assert (await Scores.objects.get(id=scores.id)).high_score == 1000
        await Scores.objects(id=scores.id).update(max__high_score=500)
        assert (await Scores.objects.get(id=scores.id)).high_score == 1000


    async def test_update_multiple(self):
        class Product(Document):
            item = StringField()
            price = FloatField()

        product = await Product.objects.create(item="ABC", price=10.99)
        product = await Product.objects.create(item="ABC", price=10.99)
        await Product.objects(id=product.id).update(mul__price=1.25)
        assert (await Product.objects.get(id=product.id)).price == 13.7375
        unknown_product = await Product.objects.create(item="Unknown")
        await Product.objects(id=unknown_product.id).update(mul__price=100)
        assert (await Product.objects.get(id=unknown_product.id)).price == 0


    async def test_updates_can_have_match_operators(self):
        class Comment(EmbeddedDocument):
            content = StringField()
            name = StringField(max_length=120)
            vote = IntField()

        class Post(Document):
            title = StringField(required=True)
            tags = ListField(StringField())
            comments = ListField(EmbeddedDocumentField("Comment"))

        await Post.drop_collection()

        comm1 = Comment(content="very funny indeed", name="John S", vote=1)
        comm2 = Comment(content="kind of funny", name="Mark P", vote=0)

        await Post(
            title="Fun with MongoEngine",
            tags=["mongodb", "mongoengine"],
            comments=[comm1, comm2],
        ).save()

        await Post.objects().update_one(pull__comments__vote__lt=1)

        assert 1 == len((await Post.objects.first()).comments)


    async def test_mapfield_update(self):
        """Ensure that the MapField can be updated."""

        class Member(EmbeddedDocument):
            gender = StringField()
            age = IntField()

        class Club(Document):
            members = MapField(EmbeddedDocumentField(Member))

        await Club.drop_collection()

        club = Club()
        club.members["John"] = Member(gender="M", age=13)
        await club.save()

        await Club.objects().update(set__members={"John": Member(gender="F", age=14)})

        club = await Club.objects().first()
        assert club.members["John"].gender == "F"
        assert club.members["John"].age == 14


    async def test_dictfield_update(self):
        """Ensure that the DictField can be updated."""

        class Club(Document):
            members = DictField()

        club = Club()
        club.members["John"] = {"gender": "M", "age": 13}
        await club.save()

        await Club.objects().update(set__members={"John": {"gender": "F", "age": 14}})

        club = await Club.objects().first()
        assert club.members["John"]["gender"] == "F"
        assert club.members["John"]["age"] == 14


    async def test_update_results(self):
        await self.Person.drop_collection()

        result = await self.Person(name="Bob", age=25).update(upsert=True, full_result=True)
        assert isinstance(result, UpdateResult)
        assert "upserted" in result.raw_result
        assert not result.raw_result["updatedExisting"]

        bob = await self.Person.objects.first()
        result = await bob.update(set__age=30, full_result=True)
        assert isinstance(result, UpdateResult)
        assert result.raw_result["updatedExisting"]

        await self.Person(name="Bob", age=20).save()
        result = await self.Person.objects(name="Bob").update(set__name="bobby", multi=True)
        assert result == 2


    async def test_update_validate(self):
        class EmDoc(EmbeddedDocument):
            str_f = StringField()

        class Doc(Document):
            str_f = StringField()
            dt_f = DateTimeField()
            cdt_f = ComplexDateTimeField()
            ed_f = EmbeddedDocumentField(EmDoc)

        with pytest.raises(ValidationError):
            await Doc.objects().update(str_f=1, upsert=True)
        with pytest.raises(ValidationError):
            await Doc.objects().update(dt_f="datetime", upsert=True)
        with pytest.raises(ValidationError):
            await Doc.objects().update(ed_f__str_f=1, upsert=True)


    async def test_update_related_models(self):
        class TestPerson(Document):
            name = StringField()

        class TestOrganization(Document):
            name = StringField()
            owner = ReferenceField(TestPerson)

        await TestPerson.drop_collection()
        await TestOrganization.drop_collection()

        p = TestPerson(name="p1")
        await p.save()
        o = TestOrganization(name="o1")
        await o.save()

        o.owner = p
        p.name = "p2"

        assert o._get_changed_fields() == ["owner"]
        assert p._get_changed_fields() == ["name"]

        await o.save()

        assert o._get_changed_fields() == []
        assert p._get_changed_fields() == ["name"]  # Fails; it's empty

        # This will do NOTHING at all, even though we changed the name
        await p.save()

        await p.reload()

        assert p.name == "p2"  # Fails; it's still `p1`


    async def test_upsert(self):
        await self.Person.drop_collection()

        await self.Person.objects(pk=ObjectId(), name="Bob", age=30).update(upsert=True)

        bob = await self.Person.objects.first()
        assert "Bob" == bob.name
        assert 30 == bob.age


    async def test_upsert_one(self):
        await self.Person.drop_collection()

        bob = await self.Person.objects(name="Bob", age=30).upsert_one()

        assert "Bob" == bob.name
        assert 30 == bob.age

        bob.name = "Bobby"
        await bob.save()

        bobby = await self.Person.objects(name="Bobby", age=30).upsert_one()

        assert "Bobby" == bobby.name
        assert 30 == bobby.age
        assert bob.id == bobby.id


    async def test_set_on_insert(self):
        await self.Person.drop_collection()

        await self.Person.objects(pk=ObjectId()).update(
            set__name="Bob", set_on_insert__age=30, upsert=True
        )

        bob = await self.Person.objects.first()
        assert "Bob" == bob.name
        assert 30 == bob.age


    async def test_rename(self):
        await self.Person.drop_collection()
        await self.Person.objects.create(name="Foo", age=11)

        bob = await self.Person.objects.as_pymongo().first()
        assert "age" in bob
        assert bob["age"] == 11

        await self.Person.objects(name="Foo").update(rename__age="person_age")

        bob = await self.Person.objects.as_pymongo().first()
        assert "age" not in bob
        assert "person_age" in bob
        assert bob["person_age"] == 11

