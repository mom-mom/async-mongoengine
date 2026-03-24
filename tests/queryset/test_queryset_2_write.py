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


class TestQueryset2(MongoDBTestCase):
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



    async def test_save_and_only_on_fields_with_default(self):
        class Embed(EmbeddedDocument):
            field = IntField()

        class B(Document):
            meta = {"collection": "b"}

            field = IntField(default=1)
            embed = EmbeddedDocumentField(Embed, default=Embed)
            embed_no_default = EmbeddedDocumentField(Embed)

        # Creating {field : 2, embed : {field: 2}, embed_no_default: {field: 2}}
        val = 2
        embed = Embed()
        embed.field = val
        record = B()
        record.field = val
        record.embed = embed
        record.embed_no_default = embed
        await record.save()

        # Checking it was saved correctly
        await record.reload()
        assert record.field == 2
        assert record.embed_no_default.field == 2
        assert record.embed.field == 2

        # Request only the _id field and save
        clone = await B.objects().only("id").first()
        await clone.save()

        # Reload the record and see that the embed data is not lost
        await record.reload()
        assert record.field == 2
        assert record.embed_no_default.field == 2
        assert record.embed.field == 2


    async def test_bulk_insert(self):
        """Ensure that bulk insert works"""

        class Comment(EmbeddedDocument):
            name = StringField()

        class Post(EmbeddedDocument):
            comments = ListField(EmbeddedDocumentField(Comment))

        class Blog(Document):
            title = StringField(unique=True)
            tags = ListField(StringField())
            posts = ListField(EmbeddedDocumentField(Post))

        await Blog.drop_collection()

        # Recreates the collection
        assert 0 == await Blog.objects.count()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])

        # Check bulk insert using load_bulk=False
        blogs = [Blog(title="%s" % i, posts=[post1, post2]) for i in range(99)]
        await Blog.objects.insert(blogs, load_bulk=False)

        assert await Blog.objects.count() == len(blogs)

        await Blog.drop_collection()
        await Blog.ensure_indexes()

        # Check bulk insert using load_bulk=True
        blogs = [Blog(title="%s" % i, posts=[post1, post2]) for i in range(99)]
        result = await Blog.objects.insert(blogs)
        assert len(result) == len(blogs)

        await Blog.drop_collection()

        comment1 = Comment(name="testa")
        comment2 = Comment(name="testb")
        post1 = Post(comments=[comment1, comment2])
        post2 = Post(comments=[comment2, comment2])
        blog1 = Blog(title="code", posts=[post1, post2])
        blog2 = Blog(title="mongodb", posts=[post2, post1])
        blog1, blog2 = await Blog.objects.insert([blog1, blog2])
        assert blog1.title == "code"
        assert blog2.title == "mongodb"

        assert await Blog.objects.count() == 2

        # test inserting an existing document (shouldn't be allowed)
        with pytest.raises(OperationError) as exc_info:
            blog = await Blog.objects.first()
            await Blog.objects.insert(blog)
        assert (
            str(exc_info.value)
            == "Some documents have ObjectIds, use doc.update() instead"
        )

        # test inserting a query set
        with pytest.raises(OperationError) as exc_info:
            blogs_qs = [b async for b in Blog.objects]
            await Blog.objects.insert(blogs_qs)
        assert (
            str(exc_info.value)
            == "Some documents have ObjectIds, use doc.update() instead"
        )

        # insert 1 new doc
        new_post = Blog(title="code123", id=ObjectId())
        await Blog.objects.insert(new_post)

        await Blog.drop_collection()

        blog1 = Blog(title="code", posts=[post1, post2])
        blog1 = await Blog.objects.insert(blog1)
        assert blog1.title == "code"
        assert await Blog.objects.count() == 1

        await Blog.drop_collection()
        blog1 = Blog(title="code", posts=[post1, post2])
        obj_id = await Blog.objects.insert(blog1, load_bulk=False)
        assert isinstance(obj_id, ObjectId)

        await Blog.drop_collection()
        await Blog.ensure_indexes()
        post3 = Post(comments=[comment1, comment1])
        blog1 = Blog(title="foo", posts=[post1, post2])
        blog2 = Blog(title="bar", posts=[post2, post3])
        await Blog.objects.insert([blog1, blog2])

        with pytest.raises(NotUniqueError):
            await Blog.objects.insert(Blog(title=blog2.title))

        assert await Blog.objects.count() == 2


    async def test_bulk_insert_different_class_fails(self):
        class Blog(Document):
            pass

        class Author(Document):
            pass

        # try inserting a different document class
        with pytest.raises(OperationError):
            await Blog.objects.insert(Author())


    async def test_bulk_insert_with_wrong_type(self):
        class Blog(Document):
            name = StringField()

        await Blog.drop_collection()
        await Blog(name="test").save()

        with pytest.raises(OperationError):
            await Blog.objects.insert("HELLO WORLD")

        with pytest.raises(OperationError):
            await Blog.objects.insert({"name": "garbage"})


    async def test_bulk_insert_update_input_document_ids(self):
        class Comment(Document):
            idx = IntField()

        await Comment.drop_collection()

        # Test with bulk
        comments = [Comment(idx=idx) for idx in range(20)]
        for com in comments:
            assert com.id is None

        returned_comments = await Comment.objects.insert(comments, load_bulk=True)

        for com in comments:
            assert isinstance(com.id, ObjectId)

        input_mapping = {com.id: com.idx for com in comments}
        saved_mapping = {com.id: com.idx for com in returned_comments}
        assert input_mapping == saved_mapping

        await Comment.drop_collection()

        # Test with just one
        comment = Comment(idx=0)
        inserted_comment_id = await Comment.objects.insert(comment, load_bulk=False)
        assert comment.id == inserted_comment_id


    async def test_bulk_insert_accepts_doc_with_ids(self):
        class Comment(Document):
            id = IntField(primary_key=True)

        await Comment.drop_collection()

        com1 = Comment(id=0)
        com2 = Comment(id=1)
        await Comment.objects.insert([com1, com2])


    async def test_insert_raise_if_duplicate_in_constraint(self):
        class Comment(Document):
            id = IntField(primary_key=True)

        await Comment.drop_collection()

        com1 = Comment(id=0)

        await Comment.objects.insert(com1)

        with pytest.raises(NotUniqueError):
            await Comment.objects.insert(com1)


    async def test_get_changed_fields_query_count(self):
        """Make sure we don't perform unnecessary db operations when
            none of document's fields were updated.
            """

        class Person(Document):
            name = StringField()
            owns = ListField(ReferenceField("Organization"))
            projects = ListField(ReferenceField("Project"))

        class Organization(Document):
            name = StringField()
            owner = ReferenceField(Person)
            employees = ListField(ReferenceField(Person))

        class Project(Document):
            name = StringField()

        await Person.drop_collection()
        await Organization.drop_collection()
        await Project.drop_collection()

        r1 = await Project(name="r1").save()
        r2 = await Project(name="r2").save()
        r3 = await Project(name="r3").save()
        p1 = await Person(name="p1", projects=[r1, r2]).save()
        p2 = await Person(name="p2", projects=[r2, r3]).save()
        o1 = await Organization(name="o1", employees=[p1]).save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            # Fetching a document should result in a query.
            org = await Organization.objects.get(id=o1.id)
            assert await q.get_count() == 1

            # Checking changed fields of a newly fetched document should not
            # result in a query.
            org._get_changed_fields()
            assert await q.get_count() == 1

        # Saving a doc without changing any of its fields should not result
        # in a query (with or without cascade=False).
        org = await Organization.objects.get(id=o1.id)
        async with query_counter() as q:
            await org.save()
            assert await q.get_count() == 0

        org = await Organization.objects.get(id=o1.id)
        async with query_counter() as q:
            await org.save(cascade=False)
            assert await q.get_count() == 0

        # Saving a doc after you append a reference to it should result in
        # two db operations (a query for the reference and an update).
        # TODO dereferencing of p2 shouldn't be necessary.
        org = await Organization.objects.get(id=o1.id)
        async with query_counter() as q:
            org.employees.append(p2)  # add p2 reference
            await org.save()  # saves the org
            assert await q.get_count() == 1


    async def test_repeated_iteration(self):
        """Ensure that QuerySet rewinds itself one iteration finishes."""
        await self.Person(name="Person 1").save()
        await self.Person(name="Person 2").save()

        queryset = self.Person.objects
        people1 = [person async for person in queryset]
        people2 = [person async for person in queryset]

        # Check that it still works even if iteration is interrupted.
        async for _person in queryset:
            break
        people3 = [person async for person in queryset]

        assert people1 == people2
        assert people1 == people3


    async def test_repr(self):
        """Test repr behavior isnt destructive"""

        class Doc(Document):
            number = IntField()

            def __repr__(self):
                return "<Doc: %s>" % self.number

        await Doc.drop_collection()

        inserted = await Doc.objects.insert([Doc(number=i) for i in range(1000)], load_bulk=True)
        assert len(inserted) == 1000

        docs = Doc.objects.order_by("number")

        assert await docs.count() == 1000

        # In async, repr can't lazily evaluate; before iteration it shows class name
        docs_string = "%s" % docs
        assert "Doc async queryset" in docs_string

        # Trigger iteration to populate cache
        _ = [d async for d in docs]
        docs.rewind()
        docs_string = "%s" % docs
        assert "Doc: 0" in docs_string

        assert await docs.count() == 1000
        assert "(remaining elements truncated)" in "%s" % docs

        # Limit and skip
        docs = Doc.objects.order_by("number")[1:4]
        _ = [d async for d in docs]
        docs.rewind()
        assert "[<Doc: 1>, <Doc: 2>, <Doc: 3>]" == "%s" % docs

        assert await docs.count(with_limit_and_skip=True) == 3
        async for _ in docs:
            assert ".. queryset mid-iteration .." == repr(docs)


    async def test_regex_query_shortcuts(self):
        """Ensure that contains, startswith, endswith, etc work."""
        person = self.Person(name="Guido van Rossum")
        await person.save()

        # Test contains
        obj = await self.Person.objects(name__contains="van").first()
        assert obj == person
        obj = await self.Person.objects(name__contains="Van").first()
        assert obj is None

        # Test icontains
        obj = await self.Person.objects(name__icontains="Van").first()
        assert obj == person

        # Test startswith
        obj = await self.Person.objects(name__startswith="Guido").first()
        assert obj == person
        obj = await self.Person.objects(name__startswith="guido").first()
        assert obj is None

        # Test istartswith
        obj = await self.Person.objects(name__istartswith="guido").first()
        assert obj == person

        # Test endswith
        obj = await self.Person.objects(name__endswith="Rossum").first()
        assert obj == person
        obj = await self.Person.objects(name__endswith="rossuM").first()
        assert obj is None

        # Test iendswith
        obj = await self.Person.objects(name__iendswith="rossuM").first()
        assert obj == person

        # Test exact
        obj = await self.Person.objects(name__exact="Guido van Rossum").first()
        assert obj == person
        obj = await self.Person.objects(name__exact="Guido van rossum").first()
        assert obj is None
        obj = await self.Person.objects(name__exact="Guido van Rossu").first()
        assert obj is None

        # Test iexact
        obj = await self.Person.objects(name__iexact="gUIDO VAN rOSSUM").first()
        assert obj == person
        obj = await self.Person.objects(name__iexact="gUIDO VAN rOSSU").first()
        assert obj is None

        # Test wholeword
        obj = await self.Person.objects(name__wholeword="Guido").first()
        assert obj == person
        obj = await self.Person.objects(name__wholeword="rossum").first()
        assert obj is None
        obj = await self.Person.objects(name__wholeword="Rossu").first()
        assert obj is None

        # Test iwholeword
        obj = await self.Person.objects(name__iwholeword="rOSSUM").first()
        assert obj == person
        obj = await self.Person.objects(name__iwholeword="rOSSU").first()
        assert obj is None

        # Test regex
        obj = await self.Person.objects(name__regex="^[Guido].*[Rossum]$").first()
        assert obj == person
        obj = await self.Person.objects(name__regex="^[guido].*[rossum]$").first()
        assert obj is None
        obj = await self.Person.objects(name__regex="^[uido].*[Rossum]$").first()
        assert obj is None

        # Test iregex
        obj = await self.Person.objects(name__iregex="^[guido].*[rossum]$").first()
        assert obj == person
        obj = await self.Person.objects(name__iregex="^[Uido].*[Rossum]$").first()
        assert obj is None

        # Test unsafe expressions
        person = self.Person(name="Guido van Rossum [.'Geek']")
        await person.save()

        obj = await self.Person.objects(name__icontains="[.'Geek").first()
        assert obj == person


    async def test_not(self):
        """Ensure that the __not operator works as expected."""
        alice = self.Person(name="Alice", age=25)
        await alice.save()

        obj = await self.Person.objects(name__iexact="alice").first()
        assert obj == alice

        obj = await self.Person.objects(name__not__iexact="alice").first()
        assert obj is None


    async def test_filter_chaining(self):
        """Ensure filters can be chained together."""

        class Blog(Document):
            id = StringField(primary_key=True)

        class BlogPost(Document):
            blog = ReferenceField(Blog)
            title = StringField()
            is_published = BooleanField()
            published_date = DateTimeField()

            @queryset_manager
            def published(doc_cls, queryset):
                return queryset(is_published=True)

        await Blog.drop_collection()
        await BlogPost.drop_collection()

        blog_1 = Blog(id="1")
        blog_2 = Blog(id="2")
        blog_3 = Blog(id="3")

        await blog_1.save()
        await blog_2.save()
        await blog_3.save()

        await BlogPost.objects.create(
            blog=blog_1,
            title="Blog Post #1",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 5, 0, 0, 0),
        )
        await BlogPost.objects.create(
            blog=blog_2,
            title="Blog Post #2",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 6, 0, 0, 0),
        )
        await BlogPost.objects.create(
            blog=blog_3,
            title="Blog Post #3",
            is_published=True,
            published_date=datetime.datetime(2010, 1, 7, 0, 0, 0),
        )

        # find all published blog posts before 2010-01-07
        published_posts = BlogPost.published()
        published_posts = published_posts.filter(
            published_date__lt=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )
        assert await published_posts.count() == 2

        blog_posts = BlogPost.objects
        blog_posts = blog_posts.filter(blog__in=[blog_1, blog_2])
        blog_posts = blog_posts.filter(blog=blog_3)
        assert await blog_posts.count() == 0

        await BlogPost.drop_collection()
        await Blog.drop_collection()


    async def test_filter_chaining_with_regex(self):
        person = self.Person(name="Guido van Rossum")
        await person.save()

        people = self.Person.objects
        people = (
            people.filter(name__startswith="Gui")
            .filter(name__not__endswith="tum")
            .filter(name__icontains="VAN")
            .filter(name__regex="^Guido")
            .filter(name__wholeword="Guido")
            .filter(name__wholeword="van")
        )
        assert await people.count() == 1


    async def test_ordering(self):
        """Ensure default ordering is applied and can be overridden."""

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.drop_collection()

        blog_post_1 = await BlogPost.objects.create(
            title="Blog Post #1", published_date=datetime.datetime(2010, 1, 5, 0, 0, 0)
        )
        blog_post_2 = await BlogPost.objects.create(
            title="Blog Post #2", published_date=datetime.datetime(2010, 1, 6, 0, 0, 0)
        )
        blog_post_3 = await BlogPost.objects.create(
            title="Blog Post #3", published_date=datetime.datetime(2010, 1, 7, 0, 0, 0)
        )

        # get the "first" BlogPost using default ordering
        # from BlogPost.meta.ordering
        expected = [blog_post_3, blog_post_2, blog_post_1]
        await self.assertSequence(BlogPost.objects.all(), expected)

        # override default ordering, order BlogPosts by "published_date"
        qs = BlogPost.objects.order_by("+published_date")
        expected = [blog_post_1, blog_post_2, blog_post_3]
        await self.assertSequence(qs, expected)


    async def test_clear_ordering(self):
        """Ensure that the default ordering can be cleared by calling
            order_by() w/o any arguments.
            """
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.drop_collection()

        # default ordering should be used by default
        async with db_ops_tracker() as q:
            await BlogPost.objects.filter(title="whatever").first()
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ops[0][CMD_QUERY_KEY][ORDER_BY_KEY] == {"published_date": -1}

        # calling order_by() should clear the default ordering
        async with db_ops_tracker() as q:
            await BlogPost.objects.filter(title="whatever").order_by().first()
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ORDER_BY_KEY not in ops[0][CMD_QUERY_KEY]

        # calling an explicit order_by should use a specified sort
        async with db_ops_tracker() as q:
            await BlogPost.objects.filter(title="whatever").order_by("published_date").first()
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ops[0][CMD_QUERY_KEY][ORDER_BY_KEY] == {"published_date": 1}

        # calling order_by() after an explicit sort should clear it
        async with db_ops_tracker() as q:
            qs = BlogPost.objects.filter(title="whatever").order_by("published_date")
            await qs.order_by().first()
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ORDER_BY_KEY not in ops[0][CMD_QUERY_KEY]


    async def test_no_ordering_for_get(self):
        """Ensure that Doc.objects.get doesn't use any ordering."""
        ORDER_BY_KEY, CMD_QUERY_KEY = get_key_compat(self.mongodb_version)

        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {"ordering": ["-published_date"]}

        await BlogPost.objects.create(
            title="whatever", published_date=datetime.datetime.utcnow()
        )

        async with db_ops_tracker() as q:
            await BlogPost.objects.get(title="whatever")
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ORDER_BY_KEY not in ops[0][CMD_QUERY_KEY]

        # Ordering should be ignored for .get even if we set it explicitly
        async with db_ops_tracker() as q:
            await BlogPost.objects.order_by("-title").get(title="whatever")
            ops = await q.get_ops()
            assert len(ops) == 1
            assert ORDER_BY_KEY not in ops[0][CMD_QUERY_KEY]


    async def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from
            different manners of querying.
            """

        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        await BlogPost.drop_collection()

        user = User(name="Test User")
        await BlogPost.objects.create(author=user, content="Had a good coffee today...")

        result = await BlogPost.objects.first()
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        result = await BlogPost.objects.get(author__name=user.name)
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        result = await BlogPost.objects.get(author={"name": user.name})
        assert isinstance(result.author, User)
        assert result.author.name == "Test User"

        # Fails, since the string is not a type that is able to represent the
        # author's document structure (should be dict)
        with pytest.raises(InvalidQueryError):
            await BlogPost.objects.get(author=user.name)


    async def test_find_empty_embedded(self):
        """Ensure that you can save and find an empty embedded document."""

        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        await BlogPost.drop_collection()

        await BlogPost.objects.create(content="Anonymous post...")

        result = await BlogPost.objects.get(author=None)
        assert result.author is None


    async def test_find_dict_item(self):
        """Ensure that DictField items may be found."""

        class BlogPost(Document):
            info = DictField()

        await BlogPost.drop_collection()

        post = BlogPost(info={"title": "test"})
        await post.save()

        post_obj = await BlogPost.objects(info__title="test").first()
        assert post_obj.id == post.id

        await BlogPost.drop_collection()


    @requires_mongodb_lt_42
    async def test_exec_js_query(self):
        """Ensure that queries are properly formed for use in exec_js."""

        class BlogPost(Document):
            hits = IntField()
            published = BooleanField()

        await BlogPost.drop_collection()

        post1 = BlogPost(hits=1, published=False)
        await post1.save()

        post2 = BlogPost(hits=1, published=True)
        await post2.save()

        post3 = BlogPost(hits=1, published=True)
        await post3.save()

        js_func = """
                function(hitsField) {
                    var count = 0;
                    db[collection].find(query).forEach(function(doc) {
                        count += doc[hitsField];
                    });
                    return count;
                }
            """

        # Ensure that normal queries work
        c = await BlogPost.objects(published=True).exec_js(js_func, "hits")
        assert c == 2

        c = await BlogPost.objects(published=False).exec_js(js_func, "hits")
        assert c == 1

        await BlogPost.drop_collection()


    @requires_mongodb_lt_42
    async def test_exec_js_field_sub(self):
        """Ensure that field substitutions occur properly in exec_js functions."""

        class Comment(EmbeddedDocument):
            content = StringField(db_field="body")

        class BlogPost(Document):
            name = StringField(db_field="doc-name")
            comments = ListField(EmbeddedDocumentField(Comment), db_field="cmnts")

        await BlogPost.drop_collection()

        comments1 = [Comment(content="cool"), Comment(content="yay")]
        post1 = BlogPost(name="post1", comments=comments1)
        await post1.save()

        comments2 = [Comment(content="nice stuff")]
        post2 = BlogPost(name="post2", comments=comments2)
        await post2.save()

        code = """
            function getComments() {
                var comments = [];
                db[collection].find(query).forEach(function(doc) {
                    var docComments = doc[~comments];
                    for (var i = 0; i < docComments.length; i++) {
                        comments.push({
                            'document': doc[~name],
                            'comment': doc[~comments][i][~comments.content]
                        });
                    }
                });
                return comments;
            }
            """

        sub_code = BlogPost.objects._sub_js_fields(code)
        code_chunks = ['doc["cmnts"];', 'doc["doc-name"],', 'doc["cmnts"][i]["body"]']
        for chunk in code_chunks:
            assert chunk in sub_code

        results = await BlogPost.objects.exec_js(code)
        expected_results = [
            {"comment": "cool", "document": "post1"},
            {"comment": "yay", "document": "post1"},
            {"comment": "nice stuff", "document": "post2"},
        ]
        assert results == expected_results

        # Test template style
        code = "{{~comments.content}}"
        sub_code = BlogPost.objects._sub_js_fields(code)
        assert "cmnts.body" == sub_code

        await BlogPost.drop_collection()


    async def test_delete(self):
        """Ensure that documents are properly deleted from the database."""
        await self.Person(name="User A", age=20).save()
        await self.Person(name="User B", age=30).save()
        await self.Person(name="User C", age=40).save()

        assert await self.Person.objects.count() == 3

        await self.Person.objects(age__lt=30).delete()
        assert await self.Person.objects.count() == 2

        await self.Person.objects.delete()
        assert await self.Person.objects.count() == 0


    async def test_reverse_delete_rule_cascade(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        await BlogPost.drop_collection()

        me = self.Person(name="Test User")
        await me.save()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        await BlogPost(content="Watching TV", author=me).save()
        await BlogPost(content="Chilling out", author=me).save()
        await BlogPost(content="Pro Testing", author=someoneelse).save()

        assert 3 == await BlogPost.objects.count()
        await self.Person.objects(name="Test User").delete()
        assert 1 == await BlogPost.objects.count()


    async def test_reverse_delete_rule_cascade_on_abstract_document(self):
        """Ensure cascading deletion of referring documents from the database
            does not fail on abstract document.
            """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.drop_collection()

        me = self.Person(name="Test User")
        await me.save()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        await BlogPost(content="Watching TV", author=me).save()
        await BlogPost(content="Chilling out", author=me).save()
        await BlogPost(content="Pro Testing", author=someoneelse).save()

        assert 3 == await BlogPost.objects.count()
        await self.Person.objects(name="Test User").delete()
        assert 1 == await BlogPost.objects.count()


    async def test_reverse_delete_rule_cascade_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
            a tree
            """

        class Dummy(Document):
            reference = ReferenceField("self", reverse_delete_rule=CASCADE)

        base = await Dummy().save()
        other = await Dummy(reference=base).save()
        base.reference = other
        await base.save()

        await base.delete()

        with pytest.raises(DoesNotExist):
            await base.reload()
        with pytest.raises(DoesNotExist):
            await other.reload()


    async def test_reverse_delete_rule_cascade_complex_cycle(self):
        """Ensure reference cascading doesn't loop if reference graph isn't
            a tree
            """

        class Category(Document):
            name = StringField()

        class Dummy(Document):
            reference = ReferenceField("self", reverse_delete_rule=CASCADE)
            cat = ReferenceField(Category, reverse_delete_rule=CASCADE)

        cat = await Category(name="cat").save()
        base = await Dummy(cat=cat).save()
        other = await Dummy(reference=base).save()
        other2 = await Dummy(reference=other).save()
        base.reference = other
        await base.save()

        await cat.delete()

        with pytest.raises(DoesNotExist):
            await base.reload()
        with pytest.raises(DoesNotExist):
            await other.reload()
        with pytest.raises(DoesNotExist):
            await other2.reload()


    async def test_reverse_delete_rule_cascade_self_referencing(self):
        """Ensure self-referencing CASCADE deletes do not result in infinite
            loop
            """

        class Category(Document):
            name = StringField()
            parent = ReferenceField("self", reverse_delete_rule=CASCADE)

        await Category.drop_collection()

        num_children = 3
        base = Category(name="Root")
        await base.save()

        # Create a simple parent-child tree
        for i in range(num_children):
            child_name = "Child-%i" % i
            child = Category(name=child_name, parent=base)
            await child.save()

            for i in range(num_children):
                child_child_name = "Child-Child-%i" % i
                child_child = Category(name=child_child_name, parent=child)
                await child_child.save()

        tree_size = 1 + num_children + (num_children * num_children)
        assert await tree_size == Category.objects.count()
        assert await num_children == Category.objects(parent=base).count()

        # The delete should effectively wipe out the Category collection
        # without resulting in infinite parent-child cascade recursion
        await base.delete()
        assert 0 == await Category.objects.count()


    async def test_reverse_delete_rule_nullify(self):
        """Ensure nullification of references to deleted documents."""

        class Category(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            category = ReferenceField(Category, reverse_delete_rule=NULLIFY)

        await BlogPost.drop_collection()
        await Category.drop_collection()

        lameness = Category(name="Lameness")
        await lameness.save()

        post = BlogPost(content="Watching TV", category=lameness)
        await post.save()

        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.first()).category.name == "Lameness"
        await Category.objects.delete()
        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.first()).category is None


    async def test_reverse_delete_rule_nullify_on_abstract_document(self):
        """Ensure nullification of references to deleted documents when
            reference is on an abstract document.
            """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.drop_collection()

        me = self.Person(name="Test User")
        await me.save()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        await BlogPost(content="Watching TV", author=me).save()

        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.first()).author == me
        await self.Person.objects(name="Test User").delete()
        assert await BlogPost.objects.count() == 1
        assert (await BlogPost.objects.first()).author is None


    async def test_reverse_delete_rule_deny(self):
        """Ensure deletion gets denied on documents that still have references
            to them.
            """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        await BlogPost.drop_collection()
        await self.Person.drop_collection()

        me = self.Person(name="Test User")
        await me.save()

        post = BlogPost(content="Watching TV", author=me)
        await post.save()

        with pytest.raises(OperationError):
            await self.Person.objects.delete()


    async def test_reverse_delete_rule_deny_on_abstract_document(self):
        """Ensure deletion gets denied on documents that still have references
            to them, when reference is on an abstract document.
            """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.drop_collection()

        me = self.Person(name="Test User")
        await me.save()

        await BlogPost(content="Watching TV", author=me).save()

        assert 1 == await BlogPost.objects.count()
        with pytest.raises(OperationError):
            await self.Person.objects.delete()


    async def test_reverse_delete_rule_pull(self):
        """Ensure pulling of references to deleted documents."""

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=PULL))

        await BlogPost.drop_collection()
        await self.Person.drop_collection()

        me = self.Person(name="Test User")
        await me.save()

        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        post = BlogPost(content="Watching TV", authors=[me, someoneelse])
        await post.save()

        another = BlogPost(content="Chilling Out", authors=[someoneelse])
        await another.save()

        await someoneelse.delete()
        await post.reload()
        await another.reload()

        assert post.authors == [me]
        assert another.authors == []


    async def test_reverse_delete_rule_pull_on_abstract_documents(self):
        """Ensure pulling of references to deleted documents when reference
            is defined on an abstract document..
            """

        class AbstractBlogPost(Document):
            meta = {"abstract": True}
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=PULL))

        class BlogPost(AbstractBlogPost):
            content = StringField()

        await BlogPost.drop_collection()
        await self.Person.drop_collection()

        me = self.Person(name="Test User")
        await me.save()

        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        post = BlogPost(content="Watching TV", authors=[me, someoneelse])
        await post.save()

        another = BlogPost(content="Chilling Out", authors=[someoneelse])
        await another.save()

        await someoneelse.delete()
        await post.reload()
        await another.reload()

        assert post.authors == [me]
        assert another.authors == []


    async def test_delete_with_limits(self):
        class Log(Document):
            pass

        await Log.drop_collection()

        for i in range(10):
            await Log().save()

        await Log.objects()[3:5].delete()
        assert 8 == await Log.objects.count()


    async def test_delete_with_limit_handles_delete_rules(self):
        """Ensure cascading deletion of referring documents from the database."""

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        await BlogPost.drop_collection()

        me = self.Person(name="Test User")
        await me.save()
        someoneelse = self.Person(name="Some-one Else")
        await someoneelse.save()

        await BlogPost(content="Watching TV", author=me).save()
        await BlogPost(content="Chilling out", author=me).save()
        await BlogPost(content="Pro Testing", author=someoneelse).save()

        assert 3 == await BlogPost.objects.count()
        await self.Person.objects()[:1].delete()
        assert 1 == await BlogPost.objects.count()


    async def test_delete_edge_case_with_write_concern_0_return_None(self):
        """Return None if the delete operation is unacknowledged.

            If we use an unack'd write concern, we don't really know how many
            documents have been deleted.
            """
        p1 = await self.Person(name="User Z", age=20).save()
        del_result = await p1.delete(w=0)
        assert del_result is None

