import pytest
from bson import ObjectId

from mongoengine import Document, ObjectIdField, ValidationError
from tests.utils import MongoDBTestCase, get_as_pymongo


class TestObjectIdField(MongoDBTestCase):
    async def test_storage(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid=ObjectId())
        await doc.save()
        assert await get_as_pymongo(doc) == {"_id": doc.id, "oid": doc.oid}

    def test_constructor_converts_str_to_ObjectId(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid=str(ObjectId()))
        assert isinstance(doc.oid, ObjectId)

    async def test_validation_works(self):
        class MyDoc(Document):
            oid = ObjectIdField()

        doc = MyDoc(oid="not-an-oid!")
        with pytest.raises(ValidationError, match="Invalid ObjectID"):
            await doc.save()

    async def test_query_none_value_dont_raise(self):
        # cf issue #2681
        class MyDoc(Document):
            oid = ObjectIdField(null=True)

        result = []
        async for doc in MyDoc.objects(oid=None):
            result.append(doc)
