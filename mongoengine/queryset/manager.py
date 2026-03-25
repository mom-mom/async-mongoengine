from collections.abc import Callable
from functools import partial
from typing import Any, overload

from mongoengine.queryset.queryset import QuerySet

__all__ = ("queryset_manager", "QuerySetManager")


class QuerySetManager:
    """
    The default QuerySet Manager.

    Custom QuerySet Manager functions can extend this class and users can
    add extra queryset functionality.  Any custom manager methods must accept a
    :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument.

    The method function should return a :class:`~mongoengine.queryset.QuerySet`
    , probably the same one that was passed in, but modified in some way.
    """

    get_queryset: Callable[..., Any] | None = None
    default: type[QuerySet[Any]] = QuerySet

    def __init__(self, queryset_func: Callable[..., Any] | None = None) -> None:
        if queryset_func:
            self.get_queryset = queryset_func

    @overload
    def __get__[D](self, instance: None, owner: type[D]) -> QuerySet[D]: ...

    @overload
    def __get__(self, instance: Any, owner: Any) -> "QuerySetManager": ...

    def __get__(self, instance: Any, owner: Any) -> Any:
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        if instance is not None:
            # Document object being used rather than a document class
            return self

        # owner is the document that contains the QuerySetManager
        # Use cached _collection (may be None; QuerySet will resolve lazily)
        queryset_class: type[QuerySet[Any]] = owner._meta.get("queryset_class", self.default)
        queryset: QuerySet[Any] = queryset_class(owner, owner._collection)
        if self.get_queryset:
            arg_count: int = self.get_queryset.__code__.co_argcount
            if arg_count == 1:
                queryset = self.get_queryset(queryset)
            elif arg_count == 2:
                queryset = self.get_queryset(owner, queryset)
            else:
                queryset = partial(self.get_queryset, owner, queryset)  # type: ignore[assignment]
        return queryset


def queryset_manager(func: Callable[..., Any]) -> QuerySetManager:
    """Decorator that allows you to define custom QuerySet managers on
    :class:`~mongoengine.Document` classes. The manager must be a function that
    accepts a :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument. The method
    function should return a :class:`~mongoengine.queryset.QuerySet`, probably
    the same one that was passed in, but modified in some way.
    """
    return QuerySetManager(func)
