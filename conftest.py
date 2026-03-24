import asyncio

import pytest
import pytest_asyncio

from mongoengine import connect
from mongoengine.connection import get_db

MONGO_TEST_DB = "mongoenginetest"
_CACHED = {}


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _mongo_connection():
    """Create MongoDB connection once for the entire test session."""
    from mongoengine.connection import _connections, _dbs, _connection_settings

    _connections.clear()
    _dbs.clear()
    _connection_settings.clear()

    conn = connect(db=MONGO_TEST_DB, uuidRepresentation="standard")
    connect(db="mongoenginetest2", alias="test2", uuidRepresentation="standard")

    from mongoengine.mongodb_support import get_mongodb_version

    _CACHED["mongodb_version"] = await get_mongodb_version()
    _CACHED["conn"] = conn
    yield conn

    _connections.clear()
    _dbs.clear()
    _connection_settings.clear()


def _is_mongo_testcase(cls):
    """Check if class is a MongoDBTestCase subclass."""
    if cls is None:
        return False
    return any(c.__name__ == "MongoDBTestCase" for c in cls.__mro__)


async def _clean_all_collections(db):
    """Delete all documents from all non-system collections.

    Uses delete_many instead of drop to avoid race conditions with
    MongoDB's async background drop processing.
    """
    names = await db.list_collection_names()
    for name in names:
        if not name.startswith("system."):
            await db[name].delete_many({})


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _clean_db(_mongo_connection, request):
    """Clean all collections before each test."""
    if not _is_mongo_testcase(request.cls):
        yield
        return

    from mongoengine.connection import _connection_settings

    if "default" not in _connection_settings:
        connect(db=MONGO_TEST_DB, uuidRepresentation="standard")

    db = get_db()
    request.cls._connection = _CACHED["conn"]
    request.cls.db = db
    request.cls.mongodb_version = _CACHED["mongodb_version"]

    await _clean_all_collections(db)
    yield
    await _clean_all_collections(db)


def pytest_collection_modifyitems(items):
    """Automatically add asyncio marker with session loop scope to all async test functions."""
    _session_marker = pytest.mark.asyncio(loop_scope="session")
    for item in items:
        if item.get_closest_marker("asyncio") is None:
            is_async = False
            if hasattr(item, "function") and hasattr(item.function, "__wrapped__"):
                is_async = asyncio.iscoroutinefunction(item.function.__wrapped__)
            elif hasattr(item, "function"):
                is_async = asyncio.iscoroutinefunction(item.function)
            if is_async:
                item.add_marker(_session_marker)
