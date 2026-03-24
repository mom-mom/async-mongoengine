import pytest
from pymongo.read_preferences import ReadPreference

from mongoengine import Document, IntField, PointField, StringField
from tests.utils import MongoDBTestCase, db_ops_tracker


class TestQuerysetAggregate(MongoDBTestCase):
    async def test_read_preference_aggregation_framework(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        # Aggregates with read_preference
        pipeline = []
        bars = await Bar.objects.read_preference(
            ReadPreference.SECONDARY_PREFERRED
        ).aggregate(pipeline)
        if hasattr(bars, "_CommandCursor__collection"):
            read_pref = bars._CommandCursor__collection.read_preference
        else:  # pymongo >= 4.9
            read_pref = bars._collection.read_preference
        assert read_pref == ReadPreference.SECONDARY_PREFERRED

    async def test_queryset_aggregation_framework(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects(age__lte=22).aggregate(pipeline)

        assert [doc async for doc in data] == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
        ]

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects(age__lte=22).order_by("-name").aggregate(pipeline)

        assert [doc async for doc in data] == [
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
        ]

        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": 1}, "avg": {"$avg": "$age"}}}
        ]
        data = await (
            Person.objects(age__gte=17, age__lte=40)
            .order_by("-age")
            .aggregate(pipeline)
        )
        assert [doc async for doc in data] == [{"_id": None, "avg": 29, "total": 2}]

        pipeline = [{"$match": {"name": "Isabella Luanna"}}]
        data = await Person.objects().aggregate(pipeline)
        assert [doc async for doc in data] == [{"_id": p1.pk, "age": 16, "name": "Isabella Luanna"}]

    async def test_queryset_aggregation_with_skip(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.skip(1).aggregate(pipeline)

        assert [doc async for doc in data] == [
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
        ]

    async def test_aggregation_propagates_hint_collation_and_comment(self):
        """Make sure adding a hint/comment/collation to the query gets added to the query"""

        base = {"locale": "en", "strength": 2}
        index_name = "name_1"

        class AggPerson(Document):
            name = StringField()
            meta = {
                "indexes": [{"fields": ["name"], "name": index_name, "collation": base}]
            }

        await AggPerson.drop_collection()
        _ = await AggPerson.objects.first()

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        comment = "test_comment"

        async with db_ops_tracker() as q:
            data = await AggPerson.objects.comment(comment).aggregate(pipeline)
            _ = [doc async for doc in data]
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(length=1))[0]
            assert "hint" not in query_op["command"]
            assert query_op["command"]["comment"] == comment
            assert "collation" not in query_op["command"]

        async with db_ops_tracker() as q:
            data = await AggPerson.objects.hint(index_name).aggregate(pipeline)
            _ = [doc async for doc in data]
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(length=1))[0]
            assert query_op["command"]["hint"] == "name_1"
            assert "comment" not in query_op["command"]
            assert "collation" not in query_op["command"]

        async with db_ops_tracker() as q:
            data = await AggPerson.objects.collation(base).aggregate(pipeline)
            _ = [doc async for doc in data]
            query_op = (await q.db.system.profile.find({"ns": "mongoenginetest.agg_person"}).to_list(length=1))[0]
            assert "hint" not in query_op["command"]
            assert "comment" not in query_op["command"]
            assert query_op["command"]["collation"] == base

    async def test_queryset_aggregation_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.limit(1).aggregate(pipeline)

        assert [doc async for doc in data] == [{"_id": p1.pk, "name": "ISABELLA LUANNA"}]

    async def test_queryset_aggregation_with_sort(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.order_by("name").aggregate(pipeline)

        assert [doc async for doc in data] == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
        ]

    async def test_queryset_aggregation_with_skip_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.skip(1).limit(1).aggregate(pipeline)
        data = [doc async for doc in data]

        assert data == [{"_id": p2.pk, "name": "WILSON JUNIOR"}]

        # Make sure limit/skip chaining order has no impact
        data2 = await Person.objects.limit(1).skip(1).aggregate(pipeline)

        assert data == [doc async for doc in data2]

    async def test_queryset_aggregation_with_sort_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert [doc async for doc in data] == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
        ]

        # Verify adding limit/skip steps works as expected
        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}, {"$limit": 1}]
        data = await Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert [doc async for doc in data] == [{"_id": p1.pk, "name": "ISABELLA LUANNA"}]

        pipeline = [
            {"$project": {"name": {"$toUpper": "$name"}}},
            {"$skip": 1},
            {"$limit": 1},
        ]
        data = await Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert [doc async for doc in data] == [{"_id": p3.pk, "name": "SANDRA MARA"}]

    async def test_queryset_aggregation_with_sort_with_skip(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.order_by("name").skip(2).aggregate(pipeline)

        assert [doc async for doc in data] == [{"_id": p2.pk, "name": "WILSON JUNIOR"}]

    async def test_queryset_aggregation_with_sort_with_skip_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects.order_by("name").skip(1).limit(1).aggregate(pipeline)

        assert [doc async for doc in data] == [{"_id": p3.pk, "name": "SANDRA MARA"}]

    async def test_queryset_aggregation_old_interface_not_working(self):
        class Person(Document):
            name = StringField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna")
        p2 = Person(name="Wilson Junior")
        p3 = Person(name="Sandra Mara")
        await Person.objects.insert([p1, p2, p3])

        _1_step_pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]

        # Make sure old interface raises an error as we changed it >= 1.0
        with pytest.raises(TypeError, match="pipeline must be a list/tuple"):
            await Person.objects.order_by("name").limit(2).aggregate(*_1_step_pipeline)

        _2_step_pipeline = [
            {"$project": {"name": {"$toUpper": "$name"}}},
            {"$limit": 1},
        ]
        with pytest.raises(
            TypeError, match="takes 2 positional arguments but 3 were given"
        ):
            await Person.objects.order_by("name").limit(2).aggregate(*_2_step_pipeline)

    async def test_queryset_aggregation_geonear_aggregation_on_pointfield(self):
        """test ensures that $geonear can be used as a 1-stage pipeline and that
        MongoEngine does not interfer with such pipeline (#2473)
        """

        class Aggr(Document):
            name = StringField()
            c = PointField()

        await Aggr.drop_collection()

        agg1 = await Aggr(name="X", c=[10.634584, 35.8245029]).save()
        agg2 = await Aggr(name="Y", c=[10.634584, 35.8245029]).save()

        pipeline = [
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": [10.634584, 35.8245029]},
                    "distanceField": "c",
                    "spherical": True,
                }
            }
        ]
        data = await Aggr.objects.aggregate(pipeline)
        assert [doc async for doc in data] == [
            {"_id": agg1.id, "c": 0.0, "name": "X"},
            {"_id": agg2.id, "c": 0.0, "name": "Y"},
        ]

    async def test_queryset_aggregation_none(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        await Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        await Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = await Person.objects().none().order_by("name").aggregate(pipeline)

        assert [doc async for doc in data] == []

    async def test_aggregate_geo_near_used_as_initial_step_before_cls_implicit_step(self):
        class BaseClass(Document):
            meta = {"allow_inheritance": True}

        class Aggr(BaseClass):
            name = StringField()
            c = PointField()

        await BaseClass.drop_collection()

        x = await Aggr(name="X", c=[10.634584, 35.8245029]).save()
        y = await Aggr(name="Y", c=[10.634584, 35.8245029]).save()

        pipeline = [
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": [10.634584, 35.8245029]},
                    "distanceField": "c",
                    "spherical": True,
                }
            }
        ]
        data = await Aggr.objects.aggregate(pipeline)
        res = [doc async for doc in data]
        assert res == [
            {"_cls": "BaseClass.Aggr", "_id": x.id, "c": 0.0, "name": "X"},
            {"_cls": "BaseClass.Aggr", "_id": y.id, "c": 0.0, "name": "Y"},
        ]

    async def test_aggregate_collstats_used_as_initial_step_before_cls_implicit_step(self):
        class SomeDoc(Document):
            name = StringField()

        await SomeDoc.drop_collection()

        await SomeDoc(name="X").save()
        await SomeDoc(name="Y").save()

        pipeline = [{"$collStats": {"count": {}}}]
        data = await SomeDoc.objects.aggregate(pipeline)
        res = [doc async for doc in data]
        assert len(res) == 1
        assert res[0]["count"] == 2
