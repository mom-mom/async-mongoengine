from collections.abc import Coroutine
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

        # one-by-one (anext)
        result = qs.aggregate(pipeline)
        first = await anext(result)

        # explicit list
        results = await qs.aggregate(pipeline).to_list()

        # raw AsyncCommandCursor
        cursor = await qs.aggregate(pipeline).get_cursor()

        # with type hint
        results = await qs.aggregate(pipeline).typed(MyTypedDict)

    .. note::

        An ``AggregationResult`` is **single-use**.  Once consumed (via
        ``await``, ``async for``, ``anext()``, ``to_list()``, or
        ``get_cursor()``), it cannot be consumed again via a different path.
        Call ``aggregate()`` again to get a new result.
    """

    __slots__ = ("_consumed", "_coro", "_cursor", "_iterating")

    def __init__(self, coro: Coroutine[Any, Any, AsyncCommandCursor]) -> None:
        self._coro: Coroutine[Any, Any, AsyncCommandCursor] | None = coro
        self._cursor: AsyncCommandCursor | None = None
        self._consumed = False
        self._iterating = False

    def _check_consumed(self) -> None:
        if self._consumed or self._iterating:
            raise RuntimeError(
                "This AggregationResult has already been consumed. "
                "Call aggregate() again for a new result."
            )

    async def _ensure_cursor(self) -> AsyncCommandCursor:
        if self._cursor is None:
            assert self._coro is not None
            self._cursor = await self._coro
            self._coro = None
        return self._cursor

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

        The cursor is created lazily on the first call.  After this call the
        result is considered consumed and cannot be re-used.
        """
        self._check_consumed()
        self._consumed = True
        return await self._ensure_cursor()

    async def to_list(self) -> list[T]:
        """Execute the aggregation and return all results as a list."""
        self._check_consumed()
        self._consumed = True
        cursor = await self._ensure_cursor()
        return await cursor.to_list()

    def __aiter__(self) -> "AggregationResult[T]":
        if not self._iterating:
            self._check_consumed()
            self._iterating = True
        return self

    async def __anext__(self) -> T:
        if not self._iterating:
            self._check_consumed()
            self._iterating = True
        cursor = await self._ensure_cursor()
        try:
            return await cursor.__anext__()
        except StopAsyncIteration:
            self._consumed = True
            raise

    def __await__(self):
        return self.to_list().__await__()
