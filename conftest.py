import pytest
import pytest_asyncio

from mongoengine import connect
from mongoengine.connection import get_db

MONGO_TEST_DB = "mongoenginetest"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _mongo_connection():
    """Create MongoDB connection once for the entire test session.

    The AsyncMongoClient must be created inside the event loop that
    pytest-asyncio manages, otherwise it binds to a different loop
    and raises RuntimeError on use.
    """
    from mongoengine.connection import _connections, _dbs, _connection_settings

    _connections.clear()
    _dbs.clear()
    _connection_settings.clear()

    conn = connect(db=MONGO_TEST_DB)
    connect(db="mongoenginetest2", alias="test2")
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
    """Drop the test database before and after each test.

    Also sets ``cls._connection`` and ``cls.db`` on MongoDBTestCase
    subclasses for backward compatibility.  Re-establishes the
    connection if a prior test (e.g. ConnectionTest) cleared it.
    """
    if not _is_mongo_testcase(request.cls):
        yield
        return

    # Re-establish connection if it was cleared by another test
    from mongoengine.connection import _connection_settings

    if "default" not in _connection_settings:
        connect(db=MONGO_TEST_DB)

    db = get_db()
    request.cls._connection = _mongo_connection
    request.cls.db = db

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
                import asyncio

                is_async = asyncio.iscoroutinefunction(item.function.__wrapped__)
            elif hasattr(item, "function"):
                import asyncio

                is_async = asyncio.iscoroutinefunction(item.function)
            if is_async:
                item.add_marker(_session_marker)
