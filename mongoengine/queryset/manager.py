from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any, overload

from bson import ObjectId

from mongoengine.queryset.queryset import QuerySet

if TYPE_CHECKING:
    from mongoengine.document import Document

__all__ = ("queryset_manager", "QuerySetManager")


class QuerySetManager[PK = ObjectId]:
    """
    The default QuerySet Manager.

    Custom QuerySet Manager functions can extend this class and users can
    add extra queryset functionality.  Any custom manager methods must accept a
    :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument.

    The method function should return a :class:`~mongoengine.queryset.QuerySet`
    , probably the same one that was passed in, but modified in some way.

    The class is generic over the primary-key type for static typing only:
    ``Document[PK]`` declares ``objects: QuerySetManager[PK]``, so
    ``Model.objects`` is a ``QuerySet[Model, Model, PK]`` (``ObjectId`` unless
    the model subclasses ``Document[str]`` etc.).

    .. note:: **Typing limitation with custom queryset classes.**
        When using ``meta = {"queryset_class": CustomQuerySet}``, the type
        checker sees ``objects`` as ``QuerySet[MyDoc]`` and custom methods
        on ``CustomQuerySet`` are not visible. To work around this, add a
        ``TYPE_CHECKING`` annotation in your model. A custom queryset for
        models with an ``ObjectId`` primary key only needs the model type
        parameter; the fully general form repeats all three parameters of
        ``QuerySet[T, R, PK]`` (``R`` is the result type, switched by
        ``scalar()`` / ``as_pymongo()``). Bound the model type parameter with
        ``Document[Any]``: bare ``Document`` means ``Document[ObjectId]`` and
        would reject models with a custom primary-key type::

            from typing import TYPE_CHECKING, Any, ClassVar

            class CustomQuerySet[T: Document[Any]](QuerySet[T]):
                def published(self) -> "CustomQuerySet[T]":
                    return self.filter(published=True)

            class GeneralQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
                def published(self) -> "GeneralQuerySet[T, R, PK]":
                    return self.filter(published=True)

            class Post(Document):
                if TYPE_CHECKING:
                    objects: ClassVar[CustomQuerySet["Post"]]
                meta = {"queryset_class": CustomQuerySet}

            # Now Post.objects.published() is visible to the type checker.

    .. note:: **Custom methods after a projection switch.**
        ``Self`` cannot re-parametrise the result type ``R``, so after
        ``as_pymongo()`` / ``scalar()`` / ``values_list()`` the static type
        falls back to the plain ``QuerySet[...]`` and the custom methods are
        no longer visible (``Post.objects.as_pymongo().published()`` is an
        attribute error). To keep them, re-declare the three projection
        methods under ``TYPE_CHECKING`` so that they return the subclass; the
        runtime implementation stays inherited. The shape is the one
        ``QuerySet`` itself uses in ``mongoengine/queryset/queryset.py``::

            from typing import TYPE_CHECKING, Any, overload

            class GeneralQuerySet[T: Document[Any], R = T, PK = ObjectId](QuerySet[T, R, PK]):
                def published(self) -> "GeneralQuerySet[T, R, PK]":
                    return self.filter(published=True)

                if TYPE_CHECKING:
                    def as_pymongo(self) -> "GeneralQuerySet[T, dict[str, Any], PK]": ...

                    @overload
                    def scalar(self) -> "GeneralQuerySet[T, T, PK]": ...
                    @overload
                    def scalar(self, field: str, /) -> "GeneralQuerySet[T, Any, PK]": ...
                    @overload
                    def scalar(self, field1: str, field2: str, /, *fields: str) -> "GeneralQuerySet[T, tuple[Any, ...], PK]": ...
                    def scalar(self, *fields: str) -> "GeneralQuerySet[T, Any, PK]": ...

                    # ... and the same three overloads for values_list().

            # Post.objects.as_pymongo().published() now type-checks.
    """

    get_queryset: Callable[..., Any] | None = None
    default: type[QuerySet[Any, Any, Any]] = QuerySet

    def __init__(self, queryset_func: Callable[..., Any] | None = None) -> None:
        if queryset_func:
            self.get_queryset = queryset_func

    @overload
    def __get__[D: Document[Any]](self, instance: None, owner: type[D]) -> QuerySet[D, D, PK]: ...

    @overload
    def __get__(self, instance: Any, owner: Any) -> QuerySetManager[PK]: ...

    def __get__(self, instance: Any, owner: Any) -> Any:
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        if instance is not None:
            # Document object being used rather than a document class
            return self

        # owner is the document that contains the QuerySetManager
        # Use cached _collection (may be None; QuerySet will resolve lazily)
        queryset_class: type[QuerySet[Any, Any, Any]] = owner._meta.get("queryset_class", self.default)
        queryset: QuerySet[Any, Any, Any] = queryset_class(owner, owner._collection)
        if self.get_queryset:
            arg_count: int = self.get_queryset.__code__.co_argcount
            if arg_count == 1:
                queryset = self.get_queryset(queryset)
            elif arg_count == 2:
                queryset = self.get_queryset(owner, queryset)
            else:
                queryset = partial(self.get_queryset, owner, queryset)  # type: ignore[assignment]
        return queryset


def queryset_manager(func: Callable[..., Any]) -> QuerySetManager[Any]:
    """Decorator that allows you to define custom QuerySet managers on
    :class:`~mongoengine.Document` classes. The manager must be a function that
    accepts a :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument. The method
    function should return a :class:`~mongoengine.queryset.QuerySet`, probably
    the same one that was passed in, but modified in some way.

    The decorator cannot know the model's primary-key type, so a manager
    declared this way is a ``QuerySetManager[Any]``: ``Model.manager`` is a
    ``QuerySet[Model, Model, Any]``.
    """
    return QuerySetManager(func)
