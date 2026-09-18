"""Runtime contract of ``in_bulk()`` and ``from_json()``.

The static contract (``dict[PK, R]`` / ``list[T]``) is checked by
``tests/typing/cases/check_bulk_json.py``; these tests pin the runtime values:
missing ids are absent, values follow the projection mode, keys are the
Python primary-key values (the ids are converted for the query) and
``from_json()`` rebuilds subclasses.
"""

import uuid
from enum import Enum

import pytest
from bson import ObjectId

from mongoengine import Document, EnumField, IntField, SequenceField, StringField, UUIDField, connect
from mongoengine.errors import ValidationError
from tests.utils import MONGO_TEST_DB, MongoDBTestCase


class TestQuerySetInBulkAndFromJson(MongoDBTestCase):
    def setup_method(self, method=None):
        class Item(Document):
            name = StringField()
            count = IntField()

            meta = {"allow_inheritance": True}

        class SubItem(Item):
            extra = StringField()

        class Coded(Document[str]):
            code = StringField(primary_key=True)
            name = StringField()

        self.Item = Item
        self.SubItem = SubItem
        self.Coded = Coded

    async def test_in_bulk_documents_and_missing_ids(self):
        a = await self.Item(name="a", count=1).save()
        b = await self.Item(name="b", count=2).save()
        c = await self.Item(name="c", count=3).save()
        missing = ObjectId()

        docs = await self.Item.objects.in_bulk([a.id, c.id, missing])
        assert set(docs) == {a.id, c.id}
        assert isinstance(docs[a.id], self.Item)
        assert docs[a.id].name == "a"
        assert docs[c.id].count == 3
        assert b.id not in docs
        assert missing not in docs
        assert docs.get(missing) is None
        assert await self.Item.objects.in_bulk([]) == {}
        assert await self.Item.objects.in_bulk([missing]) == {}

    async def test_in_bulk_accepts_any_iterable(self):
        a = await self.Item(name="a").save()
        b = await self.Item(name="b").save()
        expected = {a.id, b.id}
        assert set(await self.Item.objects.in_bulk((a.id, b.id))) == expected
        assert set(await self.Item.objects.in_bulk({a.id, b.id})) == expected
        assert set(await self.Item.objects.in_bulk(doc_id for doc_id in [a.id, b.id])) == expected
        assert set(await self.Item.objects.in_bulk(iter([a.id, b.id]))) == expected

    async def test_in_bulk_scalar_and_raw_modes(self):
        a = await self.Item(name="a", count=1).save()
        b = await self.Item(name="b", count=2).save()
        missing = ObjectId()

        # Whatever the projection mode, the keys are the Python primary-key
        # values (the ObjectIds here).
        assert await self.Item.objects.scalar("name").in_bulk([a.id, b.id, missing]) == {a.id: "a", b.id: "b"}
        assert await self.Item.objects.values_list("name").in_bulk([a.id]) == {a.id: "a"}
        assert await self.Item.objects.scalar("name", "count").in_bulk([a.id, b.id]) == {
            a.id: ("a", 1),
            b.id: ("b", 2),
        }
        assert await self.Item.objects.scalar("name").scalar().in_bulk([a.id]) == {a.id: a}

        raw = await self.Item.objects.as_pymongo().in_bulk([a.id, missing])
        assert set(raw) == {a.id}
        assert isinstance(raw[a.id], dict)
        assert raw[a.id]["_id"] == a.id
        assert raw[a.id]["name"] == "a"
        # The projection modes are mutually exclusive: the last switch wins at
        # runtime as well as statically. (Behaviour change: the combination
        # used to be inconsistent, iteration let as_pymongo() win whatever the
        # order and in_bulk() applied scalar() first.)
        assert await self.Item.objects.as_pymongo().scalar("name").in_bulk([a.id, b.id]) == {a.id: "a", b.id: "b"}
        assert await self.Item.objects(id=a.id).as_pymongo().scalar("name").first() == "a"
        assert await self.Item.objects(id=a.id).as_pymongo().values_list("name", "count").first() == ("a", 1)
        assert await self.Item.objects.as_pymongo().scalar().in_bulk([a.id]) == {a.id: a}
        assert isinstance(await self.Item.objects(id=a.id).as_pymongo().scalar().first(), self.Item)
        # as_pymongo() keeps the field selection scalar("name") made with only("name").
        combined = await self.Item.objects.scalar("name").as_pymongo().in_bulk([a.id])
        assert isinstance(combined[a.id], dict)
        assert (combined[a.id]["_id"], combined[a.id]["name"]) == (a.id, "a")
        assert "count" not in combined[a.id]
        row = await self.Item.objects(id=a.id).scalar("name").as_pymongo().first()
        assert isinstance(row, dict)
        assert (row["_id"], row["name"]) == (a.id, "a")
        assert "count" not in row
        restored = await self.Item.objects(id=a.id).scalar("name").as_pymongo().scalar().to_list()
        assert [type(doc) for doc in restored] == [self.Item]
        assert restored[0].count == 1  # scalar() without fields resets the selection

    async def test_in_bulk_custom_primary_key(self):
        await self.Coded(code="x", name="X").save()
        await self.Coded(code="y", name="Y").save()

        docs = await self.Coded.objects.in_bulk(["x", "missing"])
        assert set(docs) == {"x"}
        assert isinstance(docs["x"], self.Coded)
        assert docs["x"].pk == "x"
        assert docs["x"].name == "X"
        assert await self.Coded.objects.scalar("name").in_bulk(["x", "y"]) == {"x": "X", "y": "Y"}
        raw = await self.Coded.objects.as_pymongo().in_bulk(("y",))
        assert raw == {"y": {"_id": "y", "name": "Y"}}

    async def test_in_bulk_converts_uuid_primary_keys(self):
        """``in_bulk()`` takes and returns ``PK`` values, not the stored form (issue #33).

        ``UUIDField(binary=False)`` stores the key as ``str``. The ids go
        through the field's ``prepare_query_value`` (as ``filter(pk__in=...)``
        does), so a ``uuid.UUID`` matches, and the keys are the ``uuid.UUID``
        values a loaded document exposes, whatever form was given. The models
        use their own aliases so that both UUID representations are covered:
        only the stored ``str`` form ever reaches the driver, so the
        representation must not matter (with PyMongo's default, unspecified
        representation bson refuses to encode a native ``uuid.UUID``, which
        is what an unconverted query used to run into).
        """
        connect(db=MONGO_TEST_DB, alias="uuid_standard", uuidRepresentation="standard")
        connect(db=MONGO_TEST_DB, alias="uuid_unspecified")

        class Session(Document[uuid.UUID]):
            id = UUIDField(primary_key=True, binary=False)
            name = StringField()

            meta = {"db_alias": "uuid_standard"}

        class LegacySession(Document[uuid.UUID]):
            id = UUIDField(primary_key=True, binary=False)
            name = StringField()

            meta = {"db_alias": "uuid_unspecified"}

        for model in (Session, LegacySession):
            session_id = uuid.uuid4()
            session = await model(id=session_id, name="a").save()
            missing = uuid.uuid4()
            assert (await model.objects.as_pymongo().get(id=session_id))["_id"] == str(session_id)

            docs = await model.objects.in_bulk([session_id, missing])
            assert list(docs) == [session_id]
            assert type(next(iter(docs))) is uuid.UUID
            assert docs[session_id] == session
            assert type(docs[session_id].id) is uuid.UUID

            # The stored form is accepted as well; the keys stay the Python values.
            by_stored_form = await model.objects.in_bulk([str(session_id)])
            assert list(by_stored_form) == [session_id]
            assert by_stored_form[session_id] == session

            # Raw and scalar modes key by the Python value too; the raw dict
            # keeps the stored form in its "_id" entry.
            raw = await model.objects.as_pymongo().in_bulk([session_id])
            assert raw == {session_id: {"_id": str(session_id), "name": "a"}}
            assert await model.objects.scalar("name").in_bulk([session_id, missing]) == {session_id: "a"}
            assert await model.objects.in_bulk([missing]) == {}
            assert await model.objects.in_bulk([]) == {}

    async def test_in_bulk_converts_enum_primary_keys(self):
        """``EnumField`` keys: members and stored values match, the keys are members."""

        class Status(Enum):  # deliberately not a str subclass: "active" != Status.ACTIVE
            ACTIVE = "active"
            DONE = "done"
            ARCHIVED = "archived"

        class Flagged(Document[Status]):
            id = EnumField(Status, primary_key=True)
            name = StringField()

        active = await Flagged(id=Status.ACTIVE, name="a").save()
        done = await Flagged(id=Status.DONE, name="d").save()
        assert (await Flagged.objects.as_pymongo().get(id=Status.ACTIVE))["_id"] == "active"

        docs = await Flagged.objects.in_bulk([Status.ACTIVE, Status.ARCHIVED])
        assert list(docs) == [Status.ACTIVE]
        assert docs[Status.ACTIVE] == active
        assert docs[Status.ACTIVE].pk is Status.ACTIVE
        # The stored values are accepted as well; the keys are enum members.
        by_value = await Flagged.objects.in_bulk(["active", "done"])
        assert set(by_value) == {Status.ACTIVE, Status.DONE}
        assert by_value[Status.DONE] == done
        assert await Flagged.objects.as_pymongo().in_bulk([Status.DONE]) == {Status.DONE: {"_id": "done", "name": "d"}}
        assert await Flagged.objects.scalar("name").in_bulk(["active", Status.DONE]) == {
            Status.ACTIVE: "a",
            Status.DONE: "d",
        }
        assert await Flagged.objects.in_bulk([Status.ARCHIVED]) == {}

    async def test_in_bulk_converts_sequence_primary_keys(self):
        """``SequenceField(value_decorator=str)`` keys: the stored and Python forms agree (``str``)."""

        class Ticket(Document[str]):
            id = SequenceField(primary_key=True, value_decorator=str)
            name = StringField()

        first = await Ticket(name="a").save()
        second = await Ticket(name="b").save()
        assert (first.pk, second.pk) == ("1", "2")

        docs = await Ticket.objects.in_bulk(["1", "3"])
        assert docs == {"1": first}
        assert type(next(iter(docs))) is str
        assert await Ticket.objects.in_bulk(["2"]) == {"2": second}
        assert await Ticket.objects.scalar("name").in_bulk(["1", "2"]) == {"1": "a", "2": "b"}
        assert await Ticket.objects.as_pymongo().in_bulk(["1"]) == {"1": {"_id": "1", "name": "a"}}
        # The ids are the generated keys; value_decorator is not applied to
        # them again, so the raw counter value does not match.
        assert await Ticket.objects.in_bulk([2]) == {}

    async def test_in_bulk_does_not_reapply_a_sequence_value_decorator(self):
        """A non-idempotent ``value_decorator`` runs once, when the key is generated.

        ``filter(pk__in=...)`` would decorate the given keys again
        (``"T1"`` -> ``"TT1"``, ``"T0001"`` -> ``ValueError``); ``in_bulk()``
        converts them with ``to_mongo`` instead.
        """

        class Prefixed(Document[str]):
            id = SequenceField(primary_key=True, value_decorator=lambda n: f"T{n}")
            name = StringField()

        class Padded(Document[str]):
            id = SequenceField(primary_key=True, value_decorator=lambda n: f"T{n:04d}")
            name = StringField()

        first = await Prefixed(name="a").save()
        second = await Prefixed(name="b").save()
        assert (first.pk, second.pk) == ("T1", "T2")
        assert await Prefixed.objects.in_bulk([first.pk, second.pk, "T3"]) == {"T1": first, "T2": second}
        assert await Prefixed.objects.scalar("name").in_bulk(["T2"]) == {"T2": "b"}

        padded = await Padded(name="p").save()
        assert padded.pk == "T0001"
        assert await Padded.objects.in_bulk([padded.pk]) == {"T0001": padded}
        assert await Padded.objects.as_pymongo().in_bulk(["T0001"]) == {"T0001": {"_id": "T0001", "name": "p"}}

    async def test_in_bulk_accepts_object_id_strings(self):
        """``ObjectIdField`` keys: a 24-character hex string is converted like a filter value.

        The static type is ``Iterable[ObjectId]``; this is the lenient runtime
        behaviour of the query conversion. The keys are ``ObjectId`` values
        whatever form was given, and an invalid id raises like a filter does.
        """
        a = await self.Item(name="a", count=1).save()
        b = await self.Item(name="b", count=2).save()

        docs = await self.Item.objects.in_bulk([str(a.id), b.id])
        assert set(docs) == {a.id, b.id}
        assert all(type(key) is ObjectId for key in docs)
        assert docs[a.id].name == "a"
        raw = await self.Item.objects.as_pymongo().in_bulk([str(a.id)])
        assert set(raw) == {a.id}
        assert raw[a.id]["_id"] == a.id
        assert await self.Item.objects.scalar("name").in_bulk([str(b.id)]) == {b.id: "b"}
        assert await self.Item.objects.in_bulk([str(ObjectId())]) == {}
        with pytest.raises(ValidationError):
            await self.Item.objects.in_bulk(["not-an-object-id"])

    async def test_in_bulk_reconstructs_subclasses(self):
        item = await self.Item(name="i").save()
        sub = await self.SubItem(name="s", extra="e").save()

        docs = await self.Item.objects.in_bulk([item.id, sub.id])
        assert type(docs[item.id]) is self.Item
        assert type(docs[sub.id]) is self.SubItem
        assert docs[sub.id].extra == "e"
        assert type((await self.SubItem.objects.in_bulk([sub.id]))[sub.id]) is self.SubItem

    async def test_from_json_reconstructs_subclasses(self):
        item = await self.Item(name="a", count=1).save()
        sub = await self.SubItem(name="b", extra="e").save()
        json_data = await self.Item.objects.order_by("name").to_json(sort_keys=True, separators=(",", ":"))

        restored = self.Item.objects.from_json(json_data)
        assert isinstance(restored, list)
        assert [type(doc) for doc in restored] == [self.Item, self.SubItem]
        assert restored == [item, sub]
        assert restored[0].count == 1
        assert restored[1].extra == "e"
        assert restored[1].id == sub.id
        assert self.Item.objects.from_json("[]") == []

        # from_json() always builds documents, whatever the projection mode.
        assert [type(doc) for doc in self.Item.objects.as_pymongo().from_json(json_data)] == [
            self.Item,
            self.SubItem,
        ]
        assert [type(doc) for doc in self.Item.objects.scalar("name").from_json(json_data)] == [
            self.Item,
            self.SubItem,
        ]

    async def test_from_json_custom_primary_key(self):
        coded = await self.Coded(code="x", name="X").save()
        json_data = await self.Coded.objects.to_json(sort_keys=True, separators=(",", ":"))
        restored = self.Coded.objects.from_json(json_data)
        assert [type(doc) for doc in restored] == [self.Coded]
        assert restored[0].pk == "x"
        assert restored[0] == coded
