from mongoengine.errors import OperationError
from mongoengine.queryset.base import (
    CASCADE,
    DENY,
    DO_NOTHING,
    NULLIFY,
    PULL,
    BaseQuerySet,
)

__all__ = (
    "QuerySet",
    "QuerySetNoCache",
    "DO_NOTHING",
    "NULLIFY",
    "CASCADE",
    "DENY",
    "PULL",
)

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20
ITER_CHUNK_SIZE = 100


class QuerySet(BaseQuerySet):
    """The default queryset, that builds queries and handles a set of results
    returned from a query.

    Wraps a MongoDB cursor, providing :class:`~mongoengine.Document` objects as
    the results.
    """

    _has_more = True
    _len = None
    _result_cache = None

    async def __aiter__(self):
        """Async iteration utilises a results cache which iterates the cursor
        in batches of ``ITER_CHUNK_SIZE``.

        If ``self._has_more`` the cursor hasn't been exhausted so cache then
        batch. Otherwise iterate the result_cache.
        """
        await self._ensure_collection()
        self._iter = True

        if self._select_related_depth > 0 and not self._as_pymongo:
            # Populate full cache for bulk dereference (only on first iteration)
            if self._has_more:
                while self._has_more:
                    await self._populate_cache()
                if self._result_cache:
                    await self._dereference(
                        self._result_cache,
                        max_depth=self._select_related_depth,
                    )
            for item in self._result_cache or []:
                yield item
            return

        if self._has_more:
            async for item in self._iter_results():
                yield item
        else:
            for item in self._result_cache:
                yield item

    async def _iter_results(self):
        """An async generator for iterating over the result cache.

        Also populates the cache if there are more possible results to
        yield. Raises StopAsyncIteration when there are no more results.
        """
        if self._result_cache is None:
            self._result_cache = []

        pos = 0
        while True:
            while pos < len(self._result_cache):
                yield self._result_cache[pos]
                pos += 1

            if not self._has_more:
                return

            if len(self._result_cache) <= pos:
                await self._populate_cache()

    async def _populate_cache(self):
        """
        Populates the result cache with ``ITER_CHUNK_SIZE`` more entries
        (until the cursor is exhausted).
        """
        if self._result_cache is None:
            self._result_cache = []

        if not self._has_more:
            return

        try:
            for _ in range(ITER_CHUNK_SIZE):
                doc = await self.__anext__()
                self._result_cache.append(doc)
        except StopAsyncIteration:
            self._has_more = False

    async def count(self, with_limit_and_skip=False):
        """Count the selected elements in the query."""
        if with_limit_and_skip is False:
            return await super().count(with_limit_and_skip)

        if self._len is None:
            self._len = await super().count(with_limit_and_skip)

        return self._len

    def __repr__(self):
        """Provide a string representation of the QuerySet"""
        if self._iter:
            return ".. queryset mid-iteration .."

        if self._result_cache is not None:
            data = self._result_cache[: REPR_OUTPUT_SIZE + 1]
            if len(data) > REPR_OUTPUT_SIZE:
                data[-1] = "...(remaining elements truncated)..."
            return repr(data)

        return f"{self._document._class_name} async queryset"

    def no_cache(self):
        """Convert to a non-caching queryset"""
        if self._result_cache is not None:
            raise OperationError("QuerySet already cached")

        return self._clone_into(QuerySetNoCache(self._document, self._collection))


class QuerySetNoCache(BaseQuerySet):
    """A non caching QuerySet"""

    def cache(self):
        """Convert to a caching queryset"""
        return self._clone_into(QuerySet(self._document, self._collection))

    def __repr__(self):
        """Provides the string representation of the QuerySet"""
        if self._iter:
            return ".. queryset mid-iteration .."
        return f"{self._document._class_name} async queryset (no cache)"

    async def __aiter__(self):
        queryset = self
        if queryset._iter:
            queryset = self.clone()
        queryset._iter = True

        if queryset._select_related_depth > 0 and not queryset._as_pymongo:
            docs = []
            async for doc in queryset._get_async_cursor():
                if queryset._none or queryset._empty:
                    return
                result = queryset._document._from_son(doc)
                if queryset._scalar:
                    docs.append(queryset._get_scalar(result))
                else:
                    docs.append(result)
            if docs:
                await queryset._dereference(docs, max_depth=queryset._select_related_depth)
            for doc in docs:
                yield doc
            return

        async for doc in queryset._get_async_cursor():
            if queryset._none or queryset._empty:
                return
            if queryset._as_pymongo:
                yield doc
            else:
                result = queryset._document._from_son(doc)
                if queryset._scalar:
                    yield queryset._get_scalar(result)
                else:
                    yield result

    async def _get_async_cursor(self):
        """Iterate over the async cursor."""
        await self._ensure_collection()
        async for doc in self._cursor:
            yield doc
