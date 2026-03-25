import copy
import warnings
from typing import Any

from mongoengine.errors import InvalidQueryError
from mongoengine.queryset import transform

__all__ = ("Q", "QNode")


def warn_empty_is_deprecated() -> None:
    msg = "'empty' property is deprecated in favour of using 'not bool(filter)'"
    warnings.warn(msg, DeprecationWarning, stacklevel=2)


class QNodeVisitor:
    """Base visitor class for visiting Q-object nodes in a query tree."""

    def visit_combination(self, combination: "QCombination") -> "QNode":
        """Called by QCombination objects."""
        return combination

    def visit_query(self, query: "Q") -> "QNode":
        """Called by (New)Q objects."""
        return query


class DuplicateQueryConditionsError(InvalidQueryError):
    pass


class SimplificationVisitor(QNodeVisitor):
    """Simplifies query trees by combining unnecessary 'and' connection nodes
    into a single Q-object.
    """

    def visit_combination(self, combination: "QCombination") -> "QNode":
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [n.query for n in combination.children]
                try:
                    return Q(**self._query_conjunction(queries))
                except DuplicateQueryConditionsError:
                    # Cannot be simplified
                    pass
        return combination

    def _query_conjunction(self, queries: list[dict[str, Any]]) -> dict[str, Any]:
        """Merges query dicts - effectively &ing them together."""
        query_ops: set[str] = set()
        combined_query: dict[str, Any] = {}
        for query in queries:
            ops = set(query.keys())
            # Make sure that the same operation isn't applied more than once
            # to a single field
            intersection = ops.intersection(query_ops)
            if intersection:
                raise DuplicateQueryConditionsError()

            query_ops.update(ops)
            combined_query.update(copy.deepcopy(query))
        return combined_query


class QueryCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, document: Any) -> None:
        self.document: Any = document

    def visit_combination(self, combination: "QCombination") -> dict[str, Any]:
        operator = "$and"
        if combination.operation == combination.OR:
            operator = "$or"
        return {operator: combination.children}

    def visit_query(self, query: "Q") -> dict[str, Any]:
        return transform.query(self.document, **query.query)


class QNode:
    """Base class for nodes in query trees."""

    AND: int = 0
    OR: int = 1

    def to_query(self, document: Any) -> dict[str, Any]:
        query = self.accept(SimplificationVisitor())
        query = query.accept(QueryCompilerVisitor(document))
        return query

    def accept(self, visitor: QNodeVisitor) -> Any:
        raise NotImplementedError

    def _combine(self, other: "QNode", operation: int) -> "QNode":
        """Combine this node with another node into a QCombination
        object.
        """
        # If the other Q() is empty, ignore it and just use `self`.
        if not bool(other):
            return self

        # Or if this Q is empty, ignore it and just use `other`.
        if not bool(self):
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self) -> bool:
        warn_empty_is_deprecated()
        return False

    def __or__(self, other: "QNode") -> "QNode":
        return self._combine(other, self.OR)

    def __and__(self, other: "QNode") -> "QNode":
        return self._combine(other, self.AND)


class QCombination(QNode):
    """Represents the combination of several conditions by a given
    logical operator.
    """

    def __init__(self, operation: int, children: list["QNode"]) -> None:
        self.operation: int = operation
        self.children: list[QNode] = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def __repr__(self) -> str:
        op = " & " if self.operation is self.AND else " | "
        return f"({op.join([repr(node) for node in self.children])})"

    def __bool__(self) -> bool:
        return bool(self.children)

    def accept(self, visitor: QNodeVisitor) -> Any:
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)

        return visitor.visit_combination(self)

    @property
    def empty(self) -> bool:
        warn_empty_is_deprecated()
        return not bool(self.children)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__ and self.operation == other.operation and self.children == other.children
        )


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query: Any) -> None:
        self.query: dict[str, Any] = query

    def __repr__(self) -> str:
        return f"Q(**{repr(self.query)})"

    def __bool__(self) -> bool:
        return bool(self.query)

    def __eq__(self, other: object) -> bool:
        return self.__class__ == other.__class__ and self.query == other.query

    def accept(self, visitor: QNodeVisitor) -> Any:
        return visitor.visit_query(self)

    @property
    def empty(self) -> bool:
        warn_empty_is_deprecated()
        return not bool(self.query)
