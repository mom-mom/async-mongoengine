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
    from mongoengine.connection import _connection_settings, _connections, _dbs

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


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _clean_db(_mongo_connection, request):
    """Drop the test database before and after each test."""
    if not _is_mongo_testcase(request.cls):
        yield
        return

    from mongoengine.connection import _connection_settings, _connections, _dbs

    # Reset connection state: keep only "default" and "test2" from session fixture.
    # Tests may register extra aliases (testdb-1, testdb-2, etc.) that leak into
    # subsequent tests if not cleaned up.
    preserved_aliases = {"default", "test2"}
    for alias in list(_connection_settings.keys()):
        if alias not in preserved_aliases:
            _connection_settings.pop(alias, None)
            _connections.pop(alias, None)
            _dbs.pop(alias, None)

    if "default" not in _connection_settings:
        connect(db=MONGO_TEST_DB, uuidRepresentation="standard")
    if "test2" not in _connection_settings:
        connect(db="mongoenginetest2", alias="test2", uuidRepresentation="standard")

    db = get_db()
    request.cls._connection = _CACHED["conn"]
    request.cls.db = db
    request.cls.mongodb_version = _CACHED["mongodb_version"]

    await db.client.drop_database(MONGO_TEST_DB)
    yield
    await db.client.drop_database(MONGO_TEST_DB)


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
