import pytest
from bson.son import SON

from mongoengine import *
from mongoengine.queryset import Q, transform
from tests.utils import MongoDBTestCase


class TestTransform(MongoDBTestCase):
    def test_transform_str_datetime(self):
        data = {"date": {"$ne": "2015-12-01T00:00:00"}}
        assert transform.query(**data) == {"date": {"$ne": "2015-12-01T00:00:00"}}
        assert transform.query(date__ne="2015-12-01T00:00:00") == {"date": {"$ne": "2015-12-01T00:00:00"}}

    def test_transform_query(self):
        """Ensure that the _transform_query function operates correctly."""
        assert transform.query(name="test", age=30) == {"name": "test", "age": 30}
        assert transform.query(age__lt=30) == {"age": {"$lt": 30}}
        assert transform.query(age__gt=20, age__lt=50) == {"age": {"$gt": 20, "$lt": 50}}
        assert transform.query(age=20, age__gt=50) == {"$and": [{"age": {"$gt": 50}}, {"age": 20}]}
        assert transform.query(friend__age__gte=30) == {"friend.age": {"$gte": 30}}
        assert transform.query(name__exists=True) == {"name": {"$exists": True}}
        assert transform.query(name=["Mark"], __raw__={"name": {"$in": "Tom"}}) == {
            "$and": [{"name": ["Mark"]}, {"name": {"$in": "Tom"}}]
        }
        assert transform.query(name__in=["Tom"], __raw__={"name": "Mark"}) == {
            "$and": [{"name": {"$in": ["Tom"]}}, {"name": "Mark"}]
        }

    async def test_transform_update(self):
        class LisDoc(Document):
            foo = ListField(StringField())

        class DicDoc(Document):
            dictField = DictField()

        class Doc(Document):
            pass

        await LisDoc.drop_collection()
        await DicDoc.drop_collection()
        await Doc.drop_collection()

        await DicDoc().save()
        doc = await Doc().save()

        for k, v in (
            ("set", "$set"),
            ("set_on_insert", "$setOnInsert"),
            ("push", "$push"),
        ):
            update = transform.update(DicDoc, **{f"{k}__dictField__test": doc})
            assert isinstance(update[v]["dictField.test"], dict)

        # Update special cases
        update = transform.update(DicDoc, unset__dictField__test=doc)
        assert update["$unset"]["dictField.test"] == 1

        update = transform.update(DicDoc, pull__dictField__test=doc)
        assert isinstance(update["$pull"]["dictField"]["test"], dict)

        update = transform.update(LisDoc, pull__foo__in=["a"])
        assert update == {"$pull": {"foo": {"$in": ["a"]}}}

    def test_transform_update_push(self):
        """Ensure the differences in behvaior between 'push' and 'push_all'"""

        class BlogPost(Document):
            tags = ListField(StringField())

        update = transform.update(BlogPost, push__tags=["mongo", "db"])
        assert update == {"$push": {"tags": ["mongo", "db"]}}

        update = transform.update(BlogPost, push_all__tags=["mongo", "db"])
        assert update == {"$push": {"tags": {"$each": ["mongo", "db"]}}}

    def test_transform_update_no_operator_default_to_set(self):
        """Ensure the differences in behvaior between 'push' and 'push_all'"""

        class BlogPost(Document):
            tags = ListField(StringField())

        update = transform.update(BlogPost, tags=["mongo", "db"])
        assert update == {"$set": {"tags": ["mongo", "db"]}}

    async def test_query_field_name(self):
        """Ensure that the correct field name is used when querying."""

        class Comment(EmbeddedDocument):
            content = StringField(db_field="commentContent")

        class BlogPost(Document):
            title = StringField(db_field="postTitle")
            comments = ListField(EmbeddedDocumentField(Comment), db_field="postComments")

        await BlogPost.drop_collection()

        data = {"title": "Post 1", "comments": [Comment(content="test")]}
        post = BlogPost(**data)
        await post.save()

        qs = BlogPost.objects(title=data["title"])
        assert qs._query == {"postTitle": data["title"]}
        assert await qs.count() == 1

        qs = BlogPost.objects(pk=post.id)
        assert qs._query == {"_id": post.id}
        assert await qs.count() == 1

        qs = BlogPost.objects(comments__content="test")
        assert qs._query == {"postComments.commentContent": "test"}
        assert await qs.count() == 1

        await BlogPost.drop_collection()

    async def test_query_pk_field_name(self):
        """Ensure that the correct "primary key" field name is used when
        querying
        """

        class BlogPost(Document):
            title = StringField(primary_key=True, db_field="postTitle")

        await BlogPost.drop_collection()

        data = {"title": "Post 1"}
        post = BlogPost(**data)
        await post.save()

        assert "_id" in BlogPost.objects(pk=data["title"])._query
        assert "_id" in BlogPost.objects(title=data["title"])._query
        assert await BlogPost.objects(pk=data["title"]).count() == 1

        await BlogPost.drop_collection()

    async def test_chaining(self):
        class A(Document):
            pass

        class B(Document):
            a = ReferenceField(A)

        await A.drop_collection()
        await B.drop_collection()

        a1 = await A().save()
        a2 = await A().save()

        await B(a=a1).save()

        # Works
        q1 = B.objects.filter(a__in=[a1, a2], a=a1)._query

        # Doesn't work
        q2 = B.objects.filter(a__in=[a1, a2])
        q2 = q2.filter(a=a1)._query

        assert q1 == q2

    def test_raw_query_and_Q_objects(self):
        """
        Test raw plays nicely
        """

        class Foo(Document):
            name = StringField()
            a = StringField()
            b = StringField()
            c = StringField()

            meta = {"allow_inheritance": False}

        query = Foo.objects(__raw__={"$nor": [{"name": "bar"}]})._query
        assert query == {"$nor": [{"name": "bar"}]}

        q1 = {"$or": [{"a": 1}, {"b": 1}]}
        query = Foo.objects(Q(__raw__=q1) & Q(c=1))._query
        assert query == {"$or": [{"a": 1}, {"b": 1}], "c": 1}

    def test_raw_and_merging(self):
        class Doc(Document):
            meta = {"allow_inheritance": False}

        raw_query = Doc.objects(
            __raw__={
                "deleted": False,
                "scraped": "yes",
                "$nor": [
                    {"views.extracted": "no"},
                    {"attachments.views.extracted": "no"},
                ],
            }
        )._query

        assert raw_query == {
            "deleted": False,
            "scraped": "yes",
            "$nor": [{"views.extracted": "no"}, {"attachments.views.extracted": "no"}],
        }

    def test_geojson_PointField(self):
        class Location(Document):
            loc = PointField()

        update = transform.update(Location, set__loc=[1, 2])
        assert update == {"$set": {"loc": {"type": "Point", "coordinates": [1, 2]}}}

        update = transform.update(Location, set__loc={"type": "Point", "coordinates": [1, 2]})
        assert update == {"$set": {"loc": {"type": "Point", "coordinates": [1, 2]}}}

    def test_geojson_LineStringField(self):
        class Location(Document):
            line = LineStringField()

        update = transform.update(Location, set__line=[[1, 2], [2, 2]])
        assert update == {"$set": {"line": {"type": "LineString", "coordinates": [[1, 2], [2, 2]]}}}

        update = transform.update(Location, set__line={"type": "LineString", "coordinates": [[1, 2], [2, 2]]})
        assert update == {"$set": {"line": {"type": "LineString", "coordinates": [[1, 2], [2, 2]]}}}

    def test_geojson_PolygonField(self):
        class Location(Document):
            poly = PolygonField()

        update = transform.update(Location, set__poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]])
        assert update == {
            "$set": {
                "poly": {
                    "type": "Polygon",
                    "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]],
                }
            }
        }

        update = transform.update(
            Location,
            set__poly={
                "type": "Polygon",
                "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]],
            },
        )
        assert update == {
            "$set": {
                "poly": {
                    "type": "Polygon",
                    "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]],
                }
            }
        }

    async def test_type(self):
        class Doc(Document):
            df = DynamicField()

        await Doc(df=True).save()
        await Doc(df=7).save()
        await Doc(df="df").save()
        assert await Doc.objects(df__type=1).count() == 0  # double
        assert await Doc.objects(df__type=8).count() == 1  # bool
        assert await Doc.objects(df__type=2).count() == 1  # str
        assert await Doc.objects(df__type=16).count() == 1  # int

    async def test_embedded_field_name_like_operator(self):
        class EmbeddedItem(EmbeddedDocument):
            type = StringField()
            name = StringField()

        class Doc(Document):
            item = EmbeddedDocumentField(EmbeddedItem)

        await Doc.drop_collection()

        doc = Doc(item=EmbeddedItem(type="axe", name="Heroic axe"))
        await doc.save()

        assert 1 == await Doc.objects(item__type__="axe").count()
        assert 1 == await Doc.objects(item__name__="Heroic axe").count()

        await Doc.objects(id=doc.id).update(set__item__type__="sword")
        assert 1 == await Doc.objects(item__type__="sword").count()
        assert 0 == await Doc.objects(item__type__="axe").count()

    async def test_regular_field_named_like_operator(self):
        class SimpleDoc(Document):
            size = StringField()
            type = StringField()

        await SimpleDoc.drop_collection()
        await SimpleDoc(type="ok", size="ok").save()

        qry = transform.query(SimpleDoc, type="testtype")
        assert qry == {"type": "testtype"}

        assert await SimpleDoc.objects(type="ok").count() == 1
        assert await SimpleDoc.objects(size="ok").count() == 1

        update = transform.update(SimpleDoc, set__type="testtype")
        assert update == {"$set": {"type": "testtype"}}

        await SimpleDoc.objects.update(set__type="testtype")
        await SimpleDoc.objects.update(set__size="testsize")

        s = await SimpleDoc.objects.first()
        assert s.type == "testtype"
        assert s.size == "testsize"

    async def test_understandable_error_raised(self):
        class Event(Document):
            title = StringField()
            location = GeoPointField()

        box = [(35.0, -125.0), (40.0, -100.0)]
        # I *meant* to execute location__within_box=box
        events = Event.objects(location__within=box)
        with pytest.raises(InvalidQueryError):
            await events.count()

    def test_update_pull_for_list_fields(self):
        """
        Test added to check pull operation in update for
        EmbeddedDocumentListField which is inside a EmbeddedDocumentField
        """

        class Word(EmbeddedDocument):
            word = StringField()
            index = IntField()

        class SubDoc(EmbeddedDocument):
            heading = ListField(StringField())
            text = EmbeddedDocumentListField(Word)

        class MainDoc(Document):
            title = StringField()
            content = EmbeddedDocumentField(SubDoc)

        word = Word(word="abc", index=1)
        update = transform.update(MainDoc, pull__content__text=word)
        assert update == {"$pull": {"content.text": SON([("word", "abc"), ("index", 1)])}}

        update = transform.update(MainDoc, pull__content__heading="xyz")
        assert update == {"$pull": {"content.heading": "xyz"}}

        update = transform.update(MainDoc, pull__content__text__word__in=["foo", "bar"])
        assert update == {"$pull": {"content.text": {"word": {"$in": ["foo", "bar"]}}}}

        update = transform.update(MainDoc, pull__content__text__word__nin=["foo", "bar"])
        assert update == {"$pull": {"content.text": {"word": {"$nin": ["foo", "bar"]}}}}

    async def test_transform_embedded_document_list_fields(self):
        """
        Test added to check filtering
        EmbeddedDocumentListField which is inside a EmbeddedDocumentField
        """

        class Drink(EmbeddedDocument):
            id = StringField()
            meta = {"strict": False}

        class Shop(Document):
            drinks = EmbeddedDocumentListField(Drink)

        await Shop.drop_collection()
        drinks = [Drink(id="drink_1"), Drink(id="drink_2")]
        await Shop.objects.create(drinks=drinks)
        q_obj = transform.query(Shop, drinks__all=[{"$elemMatch": {"_id": x.id}} for x in drinks])
        assert q_obj == {"drinks": {"$all": [{"$elemMatch": {"_id": x.id}} for x in drinks]}}

        await Shop.drop_collection()

    async def test_transform_generic_reference_field(self):
        class Object(Document):
            field = GenericReferenceField()

        await Object.drop_collection()
        objects = await Object.objects.insert([Object() for _ in range(8)])
        # singular queries
        assert transform.query(Object, field=objects[0].pk) == {"field._ref.$id": objects[0].pk}
        assert transform.query(Object, field=objects[1].to_dbref()) == {"field._ref": objects[1].to_dbref()}

        # iterable queries
        assert transform.query(Object, field__in=[objects[2].pk, objects[3].pk]) == {
            "field._ref.$id": {"$in": [objects[2].pk, objects[3].pk]}
        }
        assert transform.query(Object, field__in=[objects[4].to_dbref(), objects[5].to_dbref()]) == {
            "field._ref": {"$in": [objects[4].to_dbref(), objects[5].to_dbref()]}
        }

        # invalid query
        with pytest.raises(match="cannot be applied to mixed queries"):
            transform.query(Object, field__in=[objects[6].pk, objects[7].to_dbref()])

        await Object.drop_collection()
