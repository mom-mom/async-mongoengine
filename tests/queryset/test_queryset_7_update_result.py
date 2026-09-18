"""Runtime contract of ``update()`` / ``update_one()`` / ``Document.update()`` results.

The static contract (overloads on ``full_result``) is checked by
``tests/typing/cases/check_update.py``; these tests pin the runtime values the
overloads describe.
"""

import pytest
from bson import ObjectId
from pymongo.results import UpdateResult

from mongoengine import Document, IntField, StringField
from mongoengine.context_managers import query_counter
from mongoengine.errors import OperationError
from mongoengine.queryset.base import BaseQuerySet
from tests.utils import MongoDBTestCase


class TestQuerySetUpdateResult(MongoDBTestCase):
    def setup_method(self, method=None):
        class Item(Document):
            name = StringField()
            count = IntField(default=0)

        self.Item = Item

    async def _seed(self):
        await self.Item(name="a", count=0).save()
        await self.Item(name="a", count=0).save()
        await self.Item(name="b", count=0).save()

    async def test_update_default_returns_matched_count(self):
        await self._seed()
        assert await self.Item.objects(name="a").update(set__count=1) == 2
        assert await self.Item.objects(name="a").update(full_result=False, set__count=2) == 2
        assert await self.Item.objects(name="b").update(inc__count=1) == 1
        assert await self.Item.objects(name="missing").update(set__count=1) == 0

    async def test_update_full_result_returns_update_result(self):
        await self._seed()
        result = await self.Item.objects(name="a").update(full_result=True, set__count=1)
        assert isinstance(result, UpdateResult)
        assert result.acknowledged
        assert result.matched_count == 2
        assert result.modified_count == 2
        assert result.upserted_id is None
        assert not result.did_upsert

    async def test_update_dynamic_bool_flag(self):
        await self._seed()
        for flag, expected_type in ((False, int), (True, UpdateResult)):
            result = await self.Item.objects(name="a").update(full_result=flag, set__count=3)
            assert isinstance(result, expected_type)

    async def test_update_no_op_counts_matched_not_modified(self):
        await self.Item(name="a", count=5).save()
        # The count form is the matched count, so a no-op update still reports 1.
        assert await self.Item.objects(name="a").update(set__count=5) == 1
        result = await self.Item.objects(name="a").update(full_result=True, set__count=5)
        assert result.matched_count == 1
        assert result.modified_count == 0
        assert not result.did_upsert

    async def test_update_no_match(self):
        await self._seed()
        assert await self.Item.objects(name="missing").update(set__count=1) == 0
        result = await self.Item.objects(name="missing").update(full_result=True, set__count=1)
        assert isinstance(result, UpdateResult)
        assert result.acknowledged
        assert result.matched_count == 0
        assert result.modified_count == 0
        assert result.upserted_id is None
        assert not result.did_upsert

    async def test_update_upsert(self):
        result = await self.Item.objects(name="new").update(upsert=True, full_result=True, set__count=1)
        assert isinstance(result, UpdateResult)
        assert result.did_upsert
        assert isinstance(result.upserted_id, ObjectId)
        assert result.matched_count == 0
        assert result.modified_count == 0
        created = await self.Item.objects.get(id=result.upserted_id)
        assert created.name == "new"
        assert created.count == 1
        # The count form reports the upserted document as matched.
        assert await self.Item.objects(name="other").update(upsert=True, set__count=1) == 1
        assert await self.Item.objects.count() == 2
        # Upserting an existing match updates it instead.
        again = await self.Item.objects(name="new").update(upsert=True, full_result=True, set__count=2)
        assert not again.did_upsert
        assert again.upserted_id is None
        assert again.matched_count == 1

    async def test_update_one_forms(self):
        await self._seed()
        assert await self.Item.objects(name="a").update_one(set__count=1) == 1
        assert await self.Item.objects(name="a").update_one(full_result=False, set__count=1) == 1
        result = await self.Item.objects(name="a").update_one(full_result=True, set__count=2)
        assert isinstance(result, UpdateResult)
        assert result.matched_count == 1
        assert result.modified_count == 1
        assert await self.Item.objects(count=2).count() == 1
        for flag, expected_type in ((False, int), (True, UpdateResult)):
            assert isinstance(
                await self.Item.objects(name="a").update_one(full_result=flag, set__count=3), expected_type
            )
        assert await self.Item.objects(name="missing").update_one(set__count=1) == 0
        upserted = await self.Item.objects(name="missing").update_one(upsert=True, full_result=True, set__count=9)
        assert upserted.did_upsert
        assert isinstance(upserted.upserted_id, ObjectId)

    async def test_none_and_empty_slice_full_result_without_db_access(self):
        await self.Item(name="a", count=0).save()
        expected_raw = {"n": 0, "nModified": 0, "ok": 1.0, "updatedExisting": False}
        async with query_counter() as q:
            for queryset in (self.Item.objects.none(), self.Item.objects[5:5], self.Item.objects(name="a").none()):
                result = await queryset.update(full_result=True, set__count=1)
                assert isinstance(result, UpdateResult)
                assert result.acknowledged
                assert result.matched_count == 0
                assert result.modified_count == 0
                assert result.upserted_id is None
                assert not result.did_upsert
                assert result.raw_result == expected_raw
                assert await queryset.update(set__count=1) == 0
                assert await queryset.update(full_result=False, set__count=1) == 0
                upserted = await queryset.update(upsert=True, full_result=True, set__count=1)
                assert upserted.raw_result == expected_raw

                one = await queryset.update_one(full_result=True, set__count=1)
                assert isinstance(one, UpdateResult)
                assert one.matched_count == 0
                assert one.raw_result == expected_raw
                assert await queryset.update_one(set__count=1) == 0
            assert await q.get_count() == 0
        assert (await self.Item.objects.get(name="a")).count == 0

    async def test_document_update_saved(self):
        doc = await self.Item(name="a", count=0).save()
        assert await doc.update(set__count=1) == 1
        assert await doc.update(full_result=False, set__count=2) == 1
        result = await doc.update(full_result=True, set__count=3)
        assert isinstance(result, UpdateResult)
        assert result.matched_count == 1
        assert result.modified_count == 1
        assert not result.did_upsert
        no_op = await doc.update(full_result=True, set__count=3)
        assert no_op.matched_count == 1
        assert no_op.modified_count == 0
        for flag, expected_type in ((False, int), (True, UpdateResult)):
            assert isinstance(await doc.update(full_result=flag, set__count=4), expected_type)
        await doc.reload()
        assert doc.count == 4

    async def test_document_update_unsaved_upsert(self):
        # The unsaved document's field values become the upsert query.
        assert await self.Item(name="up", count=1).update(upsert=True, set__count=2) == 1
        assert await self.Item.objects(name="up").count() == 1
        assert (await self.Item.objects.get(name="up")).count == 2

        result = await self.Item(name="up2", count=1).update(upsert=True, full_result=True, set__count=5)
        assert isinstance(result, UpdateResult)
        assert result.did_upsert
        assert isinstance(result.upserted_id, ObjectId)
        assert (await self.Item.objects.get(id=result.upserted_id)).count == 5

        # An unsaved document whose values match an existing one updates it.
        matched = await self.Item(name="up2", count=5).update(upsert=True, full_result=True, set__count=6)
        assert not matched.did_upsert
        assert matched.matched_count == 1
        assert await self.Item.objects(name="up2").count() == 1

    async def test_document_update_unsaved_raises(self):
        with pytest.raises(OperationError):
            await self.Item(name="x").update(set__count=1)
        with pytest.raises(OperationError):
            await self.Item(name="x").update(full_result=True, set__count=1)
        with pytest.raises(OperationError):
            await self.Item(name="x").update(upsert=False, full_result=True, set__count=1)
        assert await self.Item.objects.count() == 0

    async def test_upsert_one_returns_documents(self):
        created = await self.Item.objects(name="a").upsert_one(set__count=1)
        assert isinstance(created, self.Item)
        assert created.count == 1
        updated = await self.Item.objects(name="a").upsert_one(set__count=2)
        assert isinstance(updated, self.Item)
        assert updated.id == created.id
        assert updated.count == 2
        assert await self.Item.objects.count() == 1

    async def test_upsert_one_returns_documents_whatever_the_projection_mode(self):
        # as_pymongo(): the inserted branch, then the existing-document branch.
        created = await self.Item.objects(name="a").as_pymongo().upsert_one(set__count=1)
        assert isinstance(created, self.Item)
        assert (created.name, created.count) == ("a", 1)
        existing = await self.Item.objects(name="a").as_pymongo().upsert_one(set__count=2)
        assert isinstance(existing, self.Item)
        assert existing.id == created.id
        assert existing.count == 2
        assert await self.Item.objects.count() == 1

        # scalar(): both branches too; its field selection is not applied to
        # the returned document.
        created = await self.Item.objects(name="b").scalar("name").upsert_one(set__count=3)
        assert isinstance(created, self.Item)
        assert (created.name, created.count) == ("b", 3)
        existing = await self.Item.objects(name="b").scalar("name", "count").upsert_one(set__count=4)
        assert isinstance(existing, self.Item)
        assert existing.id == created.id
        assert (existing.name, existing.count) == ("b", 4)
        assert await self.Item.objects.count() == 2

        # scalar() followed by as_pymongo() leaves scalar()'s field selection
        # in force for reads; the returned document is still complete.
        existing = await self.Item.objects(name="b").scalar("name").as_pymongo().upsert_one(set__count=9)
        assert isinstance(existing, self.Item)
        assert (existing.name, existing.count) == ("b", 9)
        existing = await self.Item.objects(name="b").only("name").upsert_one(set__count=10)
        assert (existing.name, existing.count) == ("b", 10)
        assert await self.Item.objects.count() == 2

    async def test_upsert_one_raises_when_upserted_document_cannot_be_read_back(self, monkeypatch):
        async def missing_with_id(self, object_id):
            return None

        monkeypatch.setattr(BaseQuerySet, "with_id", missing_with_id)
        with pytest.raises(OperationError, match="could not be read back"):
            await self.Item.objects(name="a").upsert_one(set__count=1)
        # The upsert itself did happen.
        assert await self.Item.objects(name="a").count() == 1
