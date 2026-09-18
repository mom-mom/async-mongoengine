"""Runtime contract of ``QuerySet.insert()`` result shapes.

The static contract (overloads on the input shape and ``load_bulk``) is
checked by ``tests/typing/cases/check_insert.py``; these tests pin the runtime
values the overloads describe, the reload fallback and the bulk-insert signals.
"""

from bson import ObjectId

from mongoengine import Document, StringField, signals
from mongoengine.queryset.base import BaseQuerySet
from tests.utils import MongoDBTestCase


class TestQuerySetInsertResult(MongoDBTestCase):
    def setup_method(self, method=None):
        class Item(Document):
            name = StringField()

        class Coded(Document[str]):
            code = StringField(primary_key=True)
            name = StringField()

        self.Item = Item
        self.Coded = Coded

    async def test_insert_single_returns_reloaded_document(self):
        item = self.Item(name="a")
        inserted = await self.Item.objects.insert(item)
        assert isinstance(inserted, self.Item)
        assert inserted is not item  # reloaded from the database
        assert isinstance(inserted.id, ObjectId)
        assert inserted.id == item.id
        assert inserted.name == "a"
        assert not inserted._created
        assert await self.Item.objects.count() == 1

    async def test_insert_single_load_bulk_false_returns_primary_key(self):
        item = self.Item(name="a")
        inserted_id = await self.Item.objects.insert(item, load_bulk=False)
        assert isinstance(inserted_id, ObjectId)
        assert item.pk == inserted_id
        assert (await self.Item.objects.get(id=inserted_id)).name == "a"

    async def test_insert_batch_returns_documents_in_order(self):
        items = [self.Item(name="a"), self.Item(name="b"), self.Item(name="c")]
        inserted = await self.Item.objects.insert(items)
        assert isinstance(inserted, list)
        assert [type(doc) for doc in inserted] == [self.Item] * 3
        assert [doc.name for doc in inserted] == ["a", "b", "c"]
        assert [doc.id for doc in inserted] == [item.id for item in items]
        assert all(doc is not item for doc, item in zip(inserted, items))
        # Any sequence works, and an explicit load_bulk=True is the default.
        more = await self.Item.objects.insert((self.Item(name="d"),), load_bulk=True)
        assert [doc.name for doc in more] == ["d"]

    async def test_insert_batch_load_bulk_false_returns_primary_keys(self):
        items = [self.Item(name="a"), self.Item(name="b")]
        inserted_ids = await self.Item.objects.insert(items, load_bulk=False)
        assert isinstance(inserted_ids, list)
        assert all(isinstance(inserted_id, ObjectId) for inserted_id in inserted_ids)
        assert inserted_ids == [item.pk for item in items]
        assert await self.Item.objects.count() == 2

    async def test_insert_custom_primary_key(self):
        coded = await self.Coded.objects.insert(self.Coded(code="a", name="A"))
        assert isinstance(coded, self.Coded)
        assert coded.pk == "a"
        assert coded.name == "A"
        assert await self.Coded.objects.insert(self.Coded(code="b"), load_bulk=False) == "b"
        assert await self.Coded.objects.insert([self.Coded(code="c"), self.Coded(code="d")], load_bulk=False) == [
            "c",
            "d",
        ]
        docs = await self.Coded.objects.insert([self.Coded(code="e"), self.Coded(code="f")])
        assert [doc.pk for doc in docs] == ["e", "f"]
        assert all(isinstance(doc, self.Coded) for doc in docs)

    async def test_insert_falls_back_to_in_memory_document_when_reload_misses(self, monkeypatch):
        async def empty_in_bulk(self, object_ids):
            return {}

        monkeypatch.setattr(BaseQuerySet, "in_bulk", empty_in_bulk)

        item = self.Item(name="a")
        inserted = await self.Item.objects.insert(item)
        assert inserted is item
        assert isinstance(item.pk, ObjectId)

        items = [self.Item(name="b"), self.Item(name="c")]
        inserted_batch = await self.Item.objects.insert(items)
        assert inserted_batch == items
        assert all(doc is item for doc, item in zip(inserted_batch, items))
        assert None not in inserted_batch

        coded = self.Coded(code="x")
        assert await self.Coded.objects.insert(coded) is coded
        assert await self.Item.objects.count() == 3

    async def test_insert_partial_reload_keeps_positions(self, monkeypatch):
        original_in_bulk = BaseQuerySet.in_bulk

        async def drop_first(self, object_ids):
            ids = list(object_ids)
            documents = await original_in_bulk(self, ids)
            documents.pop(ids[0], None)
            return documents

        monkeypatch.setattr(BaseQuerySet, "in_bulk", drop_first)

        items = [self.Item(name="a"), self.Item(name="b")]
        inserted = await self.Item.objects.insert(items)
        assert inserted[0] is items[0]  # in-memory fallback
        assert inserted[1] is not items[1]  # reloaded
        assert inserted[1].id == items[1].id
        assert [doc.name for doc in inserted] == ["a", "b"]

    async def test_insert_signals_still_fire_with_loaded_flag(self, monkeypatch):
        received = []

        def pre_bulk_insert(sender, documents, **kwargs):
            received.append(("pre", sender, list(documents), kwargs))

        def post_bulk_insert(sender, documents, **kwargs):
            received.append(("post", sender, list(documents), kwargs))

        signals.pre_bulk_insert.connect(pre_bulk_insert, sender=self.Item)
        signals.post_bulk_insert.connect(post_bulk_insert, sender=self.Item)
        try:
            item = self.Item(name="a")
            inserted = await self.Item.objects.insert(item, signal_kwargs={"tag": 1})
            assert received == [
                ("pre", self.Item, [item], {"tag": 1}),
                ("post", self.Item, [inserted], {"loaded": True, "tag": 1}),
            ]
            assert received[0][2][0] is item
            assert received[1][2][0] is inserted

            received.clear()
            items = [self.Item(name="b"), self.Item(name="c")]
            await self.Item.objects.insert(items, load_bulk=False)
            assert received == [
                ("pre", self.Item, items, {}),
                ("post", self.Item, items, {"loaded": False}),
            ]
            assert all(doc is item for doc, item in zip(received[1][2], items))

            # The reload fallback still reports loaded=True with the documents.
            async def empty_in_bulk(self, object_ids):
                return {}

            monkeypatch.setattr(BaseQuerySet, "in_bulk", empty_in_bulk)
            received.clear()
            fallback = self.Item(name="d")
            await self.Item.objects.insert(fallback)
            assert received[1][0] == "post"
            assert received[1][2][0] is fallback
            assert received[1][3] == {"loaded": True}
        finally:
            signals.pre_bulk_insert.disconnect(pre_bulk_insert)
            signals.post_bulk_insert.disconnect(post_bulk_insert)
