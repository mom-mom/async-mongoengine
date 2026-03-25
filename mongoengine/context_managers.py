import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from types import TracebackType
from typing import Any

from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from mongoengine.connection import (
    DEFAULT_CONNECTION_NAME,
    _clear_session,
    _get_session,
    _set_session,
    get_connection,
    get_db,
)
from mongoengine.pymongo_support import count_documents

__all__ = (
    "switch_db",
    "switch_collection",
    "no_sub_classes",
    "query_counter",
    "set_write_concern",
    "set_read_write_concern",
    "run_in_transaction",
)


class switch_db:
    """switch_db alias context manager.

    Example ::

        # Register connections
        register_connection('default', 'mongoenginetest')
        register_connection('testdb-1', 'mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_db(Group, 'testdb-1') as Group:
            Group(name='hello testdb!').save()  # Saves in testdb-1
    """

    def __init__(self, cls: type[Any], db_alias: str) -> None:
        """Construct the switch_db context manager

        :param cls: the class to change the registered db
        :param db_alias: the name of the specific database to use
        """
        self.cls: type[Any] = cls
        self.collection: Any = cls._collection
        self.db_alias: str = db_alias
        self.ori_db_alias: str = cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME)

    async def __aenter__(self) -> type[Any]:
        """Change the db_alias and clear the cached collection."""
        self.cls._meta["db_alias"] = self.db_alias
        self.cls._collection = None
        return self.cls

    async def __aexit__(
        self,
        t: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Reset the db_alias and collection."""
        self.cls._meta["db_alias"] = self.ori_db_alias
        self.cls._collection = self.collection


class switch_collection:
    """switch_collection alias context manager.

    Example ::

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_collection(Group, 'group1') as Group:
            Group(name='hello testdb!').save()  # Saves in group1 collection
    """

    def __init__(self, cls: type[Any], collection_name: str) -> None:
        """Construct the switch_collection context manager.

        :param cls: the class to change the registered db
        :param collection_name: the name of the collection to use
        """
        self.cls: type[Any] = cls
        self.ori_collection: Any = cls._collection
        self.ori_get_collection_name: Any = cls._get_collection_name
        self.collection_name: str = collection_name

    async def __aenter__(self) -> type[Any]:
        """Change the _get_collection_name and clear the cached collection."""

        @classmethod
        def _get_collection_name(cls: type[Any]) -> str:
            return self.collection_name

        self.cls._get_collection_name = _get_collection_name
        self.cls._collection = None
        return self.cls

    async def __aexit__(
        self,
        t: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Reset the collection."""
        self.cls._collection = self.ori_collection
        self.cls._get_collection_name = self.ori_get_collection_name


class no_sub_classes:
    """no_sub_classes context manager.

    Only returns instances of this class and no sub (inherited) classes::

        with no_sub_classes(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls: type[Any]) -> None:
        """Construct the no_sub_classes context manager.

        :param cls: the class to turn querying subclasses on
        """
        self.cls: type[Any] = cls
        self.cls_initial_subclasses: tuple[str, ...] | None = None

    def __enter__(self) -> type[Any]:
        """Restrict subclass queries to only the given class."""
        self.cls_initial_subclasses = self.cls._subclasses
        self.cls._subclasses = (self.cls._class_name,)
        return self.cls

    def __exit__(
        self,
        t: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Restore the original subclass list."""
        self.cls._subclasses = self.cls_initial_subclasses


class query_counter:
    """Query_counter context manager to get the number of queries.
    This works by updating the `profiling_level` of the database so that all queries get logged,
    resetting the db.system.profile collection at the beginning of the context and counting the new entries.

    This was designed for debugging purpose. In fact it is a global counter so queries issued by other threads/processes
    can interfere with it

    Usage:

    .. code-block:: python

        class User(Document):
            name = StringField()

        async with query_counter() as q:
            user = User(name='Bob')
            assert await q.get_count() == 0  # no query fired yet
            await user.save()
            assert await q.get_count() == 1  # 1 query was fired, an 'insert'
            user_bis = await User.objects.first()
            assert await q.get_count() == 2  # a 2nd query was fired

    Be aware that:

    - Iterating over large amount of documents (>101) makes pymongo issue `getmore` queries to fetch the next batch of documents (https://www.mongodb.com/docs/manual/tutorial/iterate-a-cursor/#cursor-batches)
    - Some queries are ignored by default by the counter (killcursors, db.system.indexes)
    """

    def __init__(self, alias: str = DEFAULT_CONNECTION_NAME) -> None:
        self.db: Any = get_db(alias=alias)
        self.initial_profiling_level: int | None = None
        self._ctx_query_counter: int = 0

        self._ignored_query: dict[str, Any] = {
            "ns": {"$ne": f"{self.db.name}.system.indexes"},
            "op": {"$ne": "killcursors"},  # MONGODB < 3.2
            "command.killCursors": {"$exists": False},  # MONGODB >= 3.2
        }

    async def _turn_on_profiling(self) -> None:
        profile_update_res: dict[str, Any] = await self.db.command({"profile": 0}, session=_get_session())
        self.initial_profiling_level = profile_update_res["was"]

        await self.db.system.profile.drop()
        await self.db.command({"profile": 2}, session=_get_session())

    async def _resets_profiling(self) -> None:
        await self.db.command({"profile": self.initial_profiling_level})

    async def __aenter__(self) -> "query_counter":
        await self._turn_on_profiling()
        return self

    async def __aexit__(
        self,
        t: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._resets_profiling()

    def __repr__(self) -> str:
        return "query_counter()"

    async def get_count(self) -> int:
        """Get the number of queries issued since the context was entered.

        Usage::

            async with query_counter() as q:
                await user.save()
                assert await q.get_count() == 1

        Each call to ``get_count()`` itself issues one query to
        ``db.system.profile``, which is accounted for automatically.
        """
        count: int = await count_documents(self.db.system.profile, self._ignored_query) - self._ctx_query_counter
        self._ctx_query_counter += 1  # Account for the query we just issued to gather the information
        return count


@contextmanager
def set_write_concern(collection: Any, write_concerns: dict[str, Any]) -> Iterator[Any]:
    combined_concerns: dict[str, Any] = dict(collection.write_concern.document.items())
    combined_concerns.update(write_concerns)
    yield collection.with_options(write_concern=WriteConcern(**combined_concerns))


@contextmanager
def set_read_write_concern(
    collection: Any,
    write_concerns: dict[str, Any] | None,
    read_concerns: dict[str, Any] | None,
) -> Iterator[Any]:
    combined_write_concerns: dict[str, Any] = dict(collection.write_concern.document.items())

    if write_concerns is not None:
        combined_write_concerns.update(write_concerns)

    combined_read_concerns: dict[str, Any] = dict(collection.read_concern.document.items())

    if read_concerns is not None:
        combined_read_concerns.update(read_concerns)

    yield collection.with_options(
        write_concern=WriteConcern(**combined_write_concerns),
        read_concern=ReadConcern(**combined_read_concerns),
    )


async def _commit_with_retry(session: AsyncClientSession) -> None:
    while True:
        try:
            # Commit uses write concern set at transaction start.
            await session.commit_transaction()
            break
        except (ConnectionFailure, OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                logging.warning("UnknownTransactionCommitResult, retrying commit operation ...")
                continue
            else:
                # Error during commit
                raise


@asynccontextmanager
async def run_in_transaction(
    alias: str = DEFAULT_CONNECTION_NAME,
    session_kwargs: dict[str, Any] | None = None,
    transaction_kwargs: dict[str, Any] | None = None,
) -> AsyncIterator[None]:
    """run_in_transaction context manager
    Execute queries within the context in a database transaction.

    Usage:

    .. code-block:: python

        class A(Document):
            name = StringField()

        async with run_in_transaction():
            a_doc = await A.objects.create(name="a")
            await a_doc.update(name="b")

    Be aware that:
    - Mongo transactions run inside a session which is bound to a connection. If you attempt to
      execute a transaction across a different connection alias, pymongo will raise an exception. In
      other words: you cannot create a transaction that crosses different database connections. That
      said, multiple transaction can be nested within the same session for particular connection.

    For more information regarding pymongo transactions: https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html#transactions
    """
    conn: Any = get_connection(alias)
    session_kwargs = session_kwargs or {}
    async with conn.start_session(**session_kwargs) as session:
        transaction_kwargs = transaction_kwargs or {}
        await session.start_transaction(**transaction_kwargs)
        try:
            _set_session(session)
            yield
            await _commit_with_retry(session)
        except Exception:
            await session.abort_transaction()
            raise
        finally:
            _clear_session()
