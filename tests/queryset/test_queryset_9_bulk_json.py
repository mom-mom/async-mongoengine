"""Runtime contract of ``in_bulk()`` and ``from_json()``.

The static contract (``dict[PK, R]`` / ``list[T]``) is checked by
``tests/typing/cases/check_bulk_json.py``; these tests pin the runtime values:
missing ids are absent, values follow the projection mode, keys follow the
model's primary-key type and ``from_json()`` rebuilds subclasses.
"""

import uuid

from bson import ObjectId

from mongoengine import Document, IntField, StringField, UUIDField
from tests.utils import MongoDBTestCase


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

        class Session(Document[uuid.UUID]):
            id = UUIDField(primary_key=True, binary=False)
            name = StringField()

        self.Item = Item
        self.SubItem = SubItem
        self.Coded = Coded
        self.Session = Session

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

    async def test_in_bulk_matches_stored_primary_key_values(self):
        """Pins the current behaviour for issue #33 (PK Python type vs stored type).

        ``in_bulk()`` matches the given ids against the stored ``_id`` values
        without the field's query conversion: with ``UUIDField(binary=False)``
        the stored form is ``str``, so the ``uuid.UUID`` the static type asks
        for does not match (``get()`` converts and does).
        """
        session_id = uuid.uuid4()
        session = await self.Session(id=session_id, name="a").save()
        assert type(session.id) is uuid.UUID

        by_stored_form = await self.Session.objects.in_bulk([str(session_id)])
        assert list(by_stored_form) == [str(session_id)]  # keys are the stored values too
        assert by_stored_form[str(session_id)] == session
        assert type(by_stored_form[str(session_id)].id) is uuid.UUID

        assert await self.Session.objects.in_bulk([session_id]) == {}
        assert (await self.Session.objects.get(id=session_id)).name == "a"

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
