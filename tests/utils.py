from mongoengine.connection import get_db
from mongoengine.context_managers import query_counter

MONGO_TEST_DB = "mongoenginetest"  # standard name for the test database


class MongoDBTestCase:
    """Base class for tests that need a mongodb connection.

    Connection setup, teardown, and per-test DB cleanup are handled
    by the ``_mongo_connection`` and ``_clean_db`` async fixtures
    defined in conftest.py.
    """

    pass


async def get_as_pymongo(doc):
    """Fetch the pymongo version of a certain Document"""
    return await doc.__class__.objects.as_pymongo().get(id=doc.id)


class db_ops_tracker(query_counter):
    async def get_ops(self):
        ignore_query = dict(self._ignored_query)
        ignore_query["command.count"] = {
            "$ne": "system.profile"
        }  # Ignore the query issued by query_counter
        cursor = self.db.system.profile.find(ignore_query)
        return [doc async for doc in cursor]
