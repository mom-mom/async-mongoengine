import functools
import operator

import pymongo
import pytest

from mongoengine.connection import get_db
from mongoengine.context_managers import query_counter
from mongoengine.mongodb_support import get_mongodb_version

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

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


def requires_mongodb_lt_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.lt)


def requires_mongodb_gte_40(func):
    return _decorated_with_ver_requirement(func, (4, 0), oper=operator.ge)


def requires_mongodb_gte_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.ge)


def requires_mongodb_gte_44(func):
    return _decorated_with_ver_requirement(func, (4, 4), oper=operator.ge)


def requires_mongodb_gte_50(func):
    return _decorated_with_ver_requirement(func, (5, 0), oper=operator.ge)


def requires_mongodb_gte_60(func):
    return _decorated_with_ver_requirement(func, (6, 0), oper=operator.ge)


def requires_mongodb_gte_70(func):
    return _decorated_with_ver_requirement(func, (7, 0), oper=operator.ge)


def _decorated_with_ver_requirement(func, mongo_version_req, oper):
    """Return a MongoDB version requirement decorator."""

    @functools.wraps(func)
    async def _inner(*args, **kwargs):
        mongodb_v = await get_mongodb_version()
        if oper(mongodb_v, mongo_version_req):
            return await func(*args, **kwargs)
        else:
            pretty_version = ".".join(str(n) for n in mongo_version_req)
            pytest.skip(f"Needs MongoDB {oper.__name__} v{pretty_version}")

    return _inner


class db_ops_tracker(query_counter):
    async def get_ops(self):
        ignore_query = dict(self._ignored_query)
        ignore_query["command.count"] = {
            "$ne": "system.profile"
        }  # Ignore the query issued by query_counter
        cursor = self.db.system.profile.find(ignore_query)
        return [doc async for doc in cursor]
