from collections import defaultdict
from typing import Any

__all__ = (
    "NotRegistered",
    "InvalidDocumentError",
    "LookUpError",
    "DoesNotExist",
    "MultipleObjectsReturned",
    "InvalidQueryError",
    "OperationError",
    "NotUniqueError",
    "BulkWriteError",
    "FieldDoesNotExist",
    "ValidationError",
    "SaveConditionError",
    "DeprecatedError",
)


class MongoEngineException(Exception):
    pass


class NotRegistered(MongoEngineException):
    pass


class InvalidDocumentError(MongoEngineException):
    pass


class LookUpError(AttributeError):
    pass


class DoesNotExist(MongoEngineException):
    pass


class MultipleObjectsReturned(MongoEngineException):
    pass


class InvalidQueryError(MongoEngineException):
    pass


class OperationError(MongoEngineException):
    pass


class NotUniqueError(OperationError):
    pass


class BulkWriteError(OperationError):
    pass


class SaveConditionError(OperationError):
    pass


class FieldDoesNotExist(MongoEngineException):
    """Raised when trying to set a field
    not declared in a :class:`~mongoengine.Document`
    or an :class:`~mongoengine.EmbeddedDocument`.

    To avoid this behavior on data loading,
    you should set the :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
    """


class ValidationError(AssertionError):
    """Validation exception.

    May represent an error validating a field or a
    document containing fields with validation errors.

    :ivar errors: A dictionary of errors for fields within this
        document or list, or None if the error is for an
        individual field.
    """

    errors: dict[str, Any] = {}
    field_name: str | None = None
    _message: str | None = None

    def __init__(self, message: str = "", **kwargs: Any) -> None:
        super().__init__(message)
        self.errors = kwargs.get("errors", {})
        self.field_name = kwargs.get("field_name")
        self.message = message

    def __str__(self) -> str:
        return str(self.message)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message},)"

    def __getattribute__(self, name: str) -> Any:
        message = super().__getattribute__(name)
        if name == "message":
            if self.field_name:
                message = f"{message}"
            if self.errors:
                message = f"{message}({self._format_errors()})"
        return message

    def _get_message(self) -> str | None:
        return self._message

    def _set_message(self, message: str) -> None:
        self._message = message

    message = property(_get_message, _set_message)

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary of all errors within a document

        Keys are field names or list indices and values are the
        validation error messages, or a nested dictionary of
        errors for an embedded document or list.
        """

        def build_dict(source: Any) -> dict[str, Any] | str:
            errors_dict: dict[str, Any] = {}
            if isinstance(source, dict):
                for field_name, error in source.items():
                    errors_dict[field_name] = build_dict(error)
            elif isinstance(source, ValidationError) and source.errors:
                return build_dict(source.errors)
            else:
                return str(source)

            return errors_dict

        if not self.errors:
            return {}

        return build_dict(self.errors)  # type: ignore[return-value]

    def _format_errors(self) -> str:
        """Returns a string listing all errors within a document"""

        def generate_key(value: Any, prefix: str = "") -> str:
            if isinstance(value, list):
                value = " ".join([generate_key(k) for k in value])
            elif isinstance(value, dict):
                value = " ".join([generate_key(v, k) for k, v in value.items()])

            results = f"{prefix}.{value}" if prefix else value
            return results

        error_dict: defaultdict[str, list[str]] = defaultdict(list)
        for k, v in self.to_dict().items():
            error_dict[generate_key(v)].append(k)
        return " ".join([f"{k}: {v}" for k, v in error_dict.items()])


class DeprecatedError(MongoEngineException):
    """Raise when a user uses a feature that has been Deprecated"""

    pass
