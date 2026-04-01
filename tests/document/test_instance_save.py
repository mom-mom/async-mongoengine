import uuid
from unittest.mock import AsyncMock

import pytest
from bson import DBRef

from mongoengine import *
from mongoengine.context_managers import query_counter
from mongoengine.errors import SaveConditionError
from mongoengine.pymongo_support import PYMONGO_VERSION
from tests.utils import MongoDBTestCase, db_ops_tracker, get_as_pymongo


class TestInstanceSave(MongoDBTestCase):
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


class TestCascadeSave(MongoDBTestCase):
    """Explicit tests for cascade_save() edge cases."""

    async def test_cascade_skips_none_reference(self):
        """cascade_save does nothing when reference field is None."""

        class Author(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Author)

        await Author.drop_collection()
        await Post.drop_collection()

        post = Post(title="no author")
        await post.save()
        # Should not raise even though author is None
        await post.cascade_save()

    async def test_cascade_skips_dbref(self):
        """cascade_save skips references stored as raw DBRef (not dereferenced)."""

        class Author(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Author)

        await Author.drop_collection()
        await Post.drop_collection()

        author = Author(name="Alice")
        await author.save()

        post = Post(title="test")
        post.author = author
        await post.save()

        # Reload to get raw DBRef (not dereferenced)
        await post.reload()
        assert isinstance(post._data["author"], DBRef)

        # Should skip without error
        await post.cascade_save()

    async def test_cascade_skips_unchanged_reference(self):
        """cascade_save skips references with no changed fields."""

        class Author(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Author)

        await Author.drop_collection()
        await Post.drop_collection()

        author = Author(name="Alice")
        await author.save()

        post = Post(title="test", author=author)
        await post.save()

        # Dereference and verify no changed fields
        persons = await Post.objects.select_related()
        loaded_post = persons[0]
        assert loaded_post.author._changed_fields == []

        # cascade_save should skip the unchanged author
        async with query_counter() as q:
            count_before = await q.get_count()
            await loaded_post.cascade_save()
            # No save queries should be issued for unchanged reference
            assert await q.get_count() == count_before

    async def test_cascade_saves_changed_reference(self):
        """cascade_save persists changes on referenced documents."""

        class Author(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Author)

        await Author.drop_collection()
        await Post.drop_collection()

        author = Author(name="Alice")
        await author.save()

        post = Post(title="test", author=author)
        await post.save()

        posts = await Post.objects.select_related()
        loaded = posts[0]
        loaded.author.name = "Bob"
        await loaded.cascade_save()

        await author.reload()
        assert author.name == "Bob"

    async def test_cascade_saves_multiple_references(self):
        """cascade_save handles multiple reference fields."""

        class Person(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Person)
            reviewer = ReferenceField(Person)

        await Person.drop_collection()
        await Post.drop_collection()

        author = Person(name="Alice")
        await author.save()
        reviewer = Person(name="Bob")
        await reviewer.save()

        post = Post(title="test", author=author, reviewer=reviewer)
        await post.save()

        posts = await Post.objects.select_related()
        loaded = posts[0]
        loaded.author.name = "Alice Updated"
        loaded.reviewer.name = "Bob Updated"
        await loaded.cascade_save()

        await author.reload()
        await reviewer.reload()
        assert author.name == "Alice Updated"
        assert reviewer.name == "Bob Updated"

    async def test_cascade_prevents_circular_save(self):
        """cascade_save prevents infinite loops on circular references."""

        class Node(Document):
            name = StringField()
            parent = ReferenceField("self")

        await Node.drop_collection()

        a = Node(name="A")
        await a.save()
        b = Node(name="B", parent=a)
        await b.save()

        # Create circular reference
        a.parent = b
        a.name = "A modified"
        await a.save()

        nodes = await Node.objects(name="A modified").select_related()
        loaded_a = nodes[0]
        loaded_a.parent.name = "B modified"
        # Should not infinite loop
        await loaded_a.cascade_save()

        await b.reload()
        assert b.name == "B modified"

    async def test_cascade_with_generic_reference(self):
        """cascade_save works with GenericReferenceField."""

        class Tag(Document):
            label = StringField()

        class Post(Document):
            title = StringField()
            related = GenericReferenceField()

        await Tag.drop_collection()
        await Post.drop_collection()

        tag = Tag(label="python")
        await tag.save()

        post = Post(title="test", related=tag)
        await post.save()

        posts = await Post.objects.select_related()
        loaded = posts[0]
        loaded.related.label = "golang"
        await loaded.cascade_save()

        await tag.reload()
        assert tag.label == "golang"

    async def test_cascade_clears_changed_fields_after_save(self):
        """cascade_save clears _changed_fields on the referenced doc."""

        class Author(Document):
            name = StringField()

        class Post(Document):
            title = StringField()
            author = ReferenceField(Author)

        await Author.drop_collection()
        await Post.drop_collection()

        author = Author(name="Alice")
        await author.save()

        post = Post(title="test", author=author)
        await post.save()

        posts = await Post.objects.select_related()
        loaded = posts[0]
        loaded.author.name = "Bob"
        assert loaded.author._changed_fields != []

        await loaded.cascade_save()
        assert loaded.author._changed_fields == []
