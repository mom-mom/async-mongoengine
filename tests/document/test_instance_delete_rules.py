import pytest

from mongoengine import *
from mongoengine import signals
from mongoengine.errors import InvalidDocumentError
from mongoengine.queryset import NULLIFY
from tests.utils import MongoDBTestCase


class TestInstanceDeleteRules(MongoDBTestCase):
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
