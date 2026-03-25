from collections.abc import AsyncIterator, Coroutine
from typing import Any

from pymongo.asynchronous.command_cursor import AsyncCommandCursor

__all__ = ("AggregationResult",)


class AggregationResult[T = dict[str, Any]]:
    """Lazy wrapper around an aggregation coroutine.

    Supports multiple consumption patterns without requiring ``await`` on
    the ``aggregate()`` call itself::

        # as a list (await)
        results = await qs.aggregate(pipeline)

        # streaming (async for)
        async for doc in qs.aggregate(pipeline):
            ...

        # explicit list
        results = await qs.aggregate(pipeline).to_list()

        # raw AsyncCommandCursor
        cursor = await qs.aggregate(pipeline).get_cursor()

        # with type hint
        results = await qs.aggregate(pipeline).typed(MyTypedDict)
    """

    __slots__ = ("_coro", "_cursor")

    def __init__(self, coro: Coroutine[Any, Any, AsyncCommandCursor]) -> None:
        self._coro = coro
        self._cursor: AsyncCommandCursor | None = None

    def typed[R](self, _: type[R]) -> "AggregationResult[R]":
        """Narrow the result type for static type checkers.

        This is a no-op at runtime — it simply returns ``self`` cast to
        ``AggregationResult[R]``::

            class CityCount(TypedDict):
                _id: str
                count: int

            results = await qs.aggregate(pipeline).typed(CityCount)
            # type checker sees: list[CityCount]
        """
        return self  # type: ignore[return-value]

    async def get_cursor(self) -> AsyncCommandCursor:
        """Return the underlying ``AsyncCommandCursor``.

        The cursor is created lazily on the first call and cached for
        subsequent access.
        """
        if self._cursor is None:
            self._cursor = await self._coro
        return self._cursor

    async def to_list(self) -> list[T]:
        """Execute the aggregation and return all results as a list."""
        cursor = await self.get_cursor()
        return await cursor.to_list()

    def __aiter__(self) -> AsyncIterator[T]:
        return self._async_iter()

    async def _async_iter(self) -> AsyncIterator[T]:
        cursor = await self.get_cursor()
        async for doc in cursor:
            yield doc

    def __await__(self):
        return self.to_list().__await__()
