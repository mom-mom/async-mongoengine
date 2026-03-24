from mongoengine import Document
from mongoengine.pymongo_support import count_documents
from tests.utils import MongoDBTestCase


class TestPymongoSupport(MongoDBTestCase):
    async def test_count_documents(self):
        class Test(Document):
            pass

        await Test.drop_collection()
        await Test().save()
        await Test().save()
        assert await count_documents(await Test._get_collection(), filter={}) == 2
        assert await count_documents(await Test._get_collection(), filter={}, skip=1) == 1
        assert await count_documents(await Test._get_collection(), filter={}, limit=0) == 0
