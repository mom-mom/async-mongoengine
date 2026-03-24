import pytest
from bson import DBRef, ObjectId

from mongoengine import (
    Document,
    GenericReferenceField,
    ListField,
    NotRegistered,
    StringField,
    ValidationError,
)
from mongoengine.base import _DocumentRegistry
from tests.utils import MongoDBTestCase, get_as_pymongo


class TestField(MongoDBTestCase):

    async def test_generic_reference_field_basics(self):
        """Ensure that a GenericReferenceField properly stores items."""

        class Link(Document):
            title = StringField()
            meta = {"allow_inheritance": False}

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField()

        await Link.drop_collection()
        await Post.drop_collection()
        await Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        bm = Bookmark(bookmark_object=post_1)
        await bm.save()

        bm = await Bookmark.objects(bookmark_object=post_1).first()
        assert await get_as_pymongo(bm) == {
            "_id": bm.id,
            "bookmark_object": {
                "_cls": "Post",
                "_ref": post_1.to_dbref(),
            },
        }
        # No auto-dereference: bookmark_object is a raw dict with _cls and _ref
        raw_ref = bm.bookmark_object
        assert isinstance(raw_ref, dict)
        assert raw_ref["_cls"] == "Post"

        bm.bookmark_object = link_1
        await bm.save()

        bm = await Bookmark.objects(bookmark_object=link_1).first()
        assert await get_as_pymongo(bm) == {
            "_id": bm.id,
            "bookmark_object": {
                "_cls": "Link",
                "_ref": link_1.to_dbref(),
            },
        }

        raw_ref = bm.bookmark_object
        assert isinstance(raw_ref, dict)
        assert raw_ref["_cls"] == "Link"

    async def test_generic_reference_works_with_in_operator(self):
        class SomeObj(Document):
            pass

        class OtherObj(Document):
            obj = GenericReferenceField()

        await SomeObj.drop_collection()
        await OtherObj.drop_collection()

        s1 = await SomeObj().save()
        await OtherObj(obj=s1).save()

        # Query using to_dbref
        assert await OtherObj.objects(obj__in=[s1.to_dbref()]).count() == 1

        # Query using id
        assert await OtherObj.objects(obj__in=[s1.id]).count() == 1

        # Query using document instance
        assert await OtherObj.objects(obj__in=[s1]).count() == 1

    async def test_generic_reference_list(self):
        """Ensure that a ListField properly stores generic references."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField())

        await Link.drop_collection()
        await Post.drop_collection()
        await User.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        user = User(bookmarks=[post_1, link_1])
        await user.save()

        user = await User.objects(bookmarks__all=[post_1, link_1]).first()

        # No auto-dereference: bookmarks are raw dicts
        assert isinstance(user.bookmarks[0], dict)
        assert isinstance(user.bookmarks[1], dict)

    async def test_generic_reference_document_not_registered(self):
        """Ensure dereferencing out of the document registry throws a
        `NotRegistered` error.
        """

        class Link(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField())

        await Link.drop_collection()
        await User.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        user = User(bookmarks=[link_1])
        await user.save()

        # Mimic User and Link definitions being in a different file
        # and the Link model not being imported in the User file.
        _DocumentRegistry.unregister("Link")

        user = await User.objects.first()
        # No auto-dereference, so accessing bookmarks returns raw data
        # which doesn't trigger NotRegistered
        assert user.bookmarks is not None

    async def test_generic_reference_is_none(self):
        class Person(Document):
            name = StringField()
            city = GenericReferenceField()

        await Person.drop_collection()

        await Person(name="Wilson Jr").save()
        results = [d async for d in Person.objects(city=None)]
        assert len(results) == 1
        assert repr(results[0]) == "<Person: Person object>"

    async def test_generic_reference_choices(self):
        """Ensure that a GenericReferenceField can handle choices."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))

        await Link.drop_collection()
        await Post.drop_collection()
        await Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        with pytest.raises(ValidationError):
            bm.validate()

        bm = Bookmark(bookmark_object=post_1)
        await bm.save()

        bm = await Bookmark.objects.first()
        # No auto-dereference: bookmark_object is a raw dict
        assert isinstance(bm.bookmark_object, dict)
        assert bm.bookmark_object["_cls"] == "Post"

    async def test_generic_reference_string_choices(self):
        """Ensure that a GenericReferenceField can handle choices as strings"""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=("Post", Link))

        await Link.drop_collection()
        await Post.drop_collection()
        await Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        await bm.save()

        bm = Bookmark(bookmark_object=post_1)
        await bm.save()

        bm = Bookmark(bookmark_object=bm)
        with pytest.raises(ValidationError):
            bm.validate()

    async def test_generic_reference_choices_no_dereference(self):
        """Ensure that a GenericReferenceField can handle choices on
        non-derefenreced (i.e. DBRef) elements
        """

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))
            other_field = StringField()

        await Post.drop_collection()
        await Bookmark.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        bm = Bookmark(bookmark_object=post_1)
        await bm.save()

        bm = await Bookmark.objects.get(id=bm.id)
        # bookmark_object is now a raw dict (no auto-deref)
        bm.other_field = "dummy_change"
        await bm.save()

    async def test_generic_reference_list_choices(self):
        """Ensure that a ListField properly stores generic references and
        respects choices.
        """

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Post,)))

        await Link.drop_collection()
        await Post.drop_collection()
        await User.drop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        user = User(bookmarks=[link_1])
        with pytest.raises(ValidationError):
            user.validate()

        user = User(bookmarks=[post_1])
        await user.save()

        user = await User.objects.first()
        # No auto-dereference
        assert isinstance(user.bookmarks[0], dict)

    async def test_generic_reference_list_item_modification(self):
        """Ensure that modifications of related documents (through generic reference) don't influence on querying"""

        class Post(Document):
            title = StringField()

        class User(Document):
            username = StringField()
            bookmarks = ListField(GenericReferenceField())

        await Post.drop_collection()
        await User.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.save()

        user = User(bookmarks=[post_1])
        await user.save()

        post_1.title = "Title was modified"
        user.username = "New username"
        await user.save()

        user = await User.objects(bookmarks__all=[post_1]).first()

        assert user is not None
        # No auto-dereference
        assert isinstance(user.bookmarks[0], dict)

    async def test_generic_reference_filter_by_dbref(self):
        """Ensure we can search for a specific generic reference by
        providing its ObjectId.
        """

        class Doc(Document):
            ref = GenericReferenceField()

        await Doc.drop_collection()

        doc1 = await Doc.objects.create()
        doc2 = await Doc.objects.create(ref=doc1)

        doc = await Doc.objects.get(ref=DBRef("doc", doc1.pk))
        assert doc == doc2

    async def test_generic_reference_is_not_tracked_in_parent_doc(self):
        """Ensure that modifications of related documents (through generic reference) don't influence
        the owner changed fields (#1934)
        """

        class Doc1(Document):
            name = StringField()

        class Doc2(Document):
            ref = GenericReferenceField()
            refs = ListField(GenericReferenceField())

        await Doc1.drop_collection()
        await Doc2.drop_collection()

        doc1 = await Doc1(name="garbage1").save()
        doc11 = await Doc1(name="garbage11").save()
        doc2 = await Doc2(ref=doc1, refs=[doc11]).save()

        # No auto-dereference: doc2.ref is a raw dict, not a Document
        # so we can't do doc2.ref.name - changed fields should be empty
        assert doc2._get_changed_fields() == []
        assert doc2._delta() == ({}, {})

    async def test_generic_reference_field(self):
        """Ensure we can search for a specific generic reference by
        providing its DBRef.
        """

        class Doc(Document):
            ref = GenericReferenceField()

        await Doc.drop_collection()

        doc1 = await Doc.objects.create()
        doc2 = await Doc.objects.create(ref=doc1)

        assert isinstance(doc1.pk, ObjectId)

        doc = await Doc.objects.get(ref=doc1.pk)
        assert doc == doc2
