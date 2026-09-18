# Import everything from each submodule so that it can be accessed via
# mongoengine, e.g. instead of `from mongoengine.connection import connect`,
# users can simply use `from mongoengine import connect`, or even
# `from mongoengine import *` and then `connect('testdb')`.
from mongoengine.connection import *  # noqa: F401
from mongoengine.document import *  # noqa: F401
from mongoengine.errors import *  # noqa: F401
from mongoengine.fields import *  # noqa: F401
from mongoengine.queryset import *  # noqa: F401
from mongoengine.signals import *  # noqa: F401

# Static re-export list: the union of the submodule ``__all__`` tuples above.
# tests/test_package_exports.py asserts that it cannot drift from them.
__all__ = (
    # mongoengine.document
    "Document",
    "EmbeddedDocument",
    "DynamicDocument",
    "DynamicEmbeddedDocument",
    "OperationError",
    "InvalidCollectionError",
    "NotUniqueError",
    "MapReduceDocument",
    # mongoengine.fields
    "StringField",
    "URLField",
    "EmailField",
    "IntField",
    "FloatField",
    "DecimalField",
    "BooleanField",
    "DateTimeField",
    "DateField",
    "ComplexDateTimeField",
    "EmbeddedDocumentField",
    "ObjectIdField",
    "GenericEmbeddedDocumentField",
    "DynamicField",
    "ListField",
    "SortedListField",
    "EmbeddedDocumentListField",
    "DictField",
    "MapField",
    "ReferenceField",
    "CachedReferenceField",
    "LazyReferenceField",
    "GenericLazyReferenceField",
    "GenericReferenceField",
    "BinaryField",
    "GeoPointField",
    "PointField",
    "LineStringField",
    "PolygonField",
    "SequenceField",
    "UUIDField",
    "EnumField",
    "MultiPointField",
    "MultiLineStringField",
    "MultiPolygonField",
    "GeoJsonBaseField",
    "Decimal128Field",
    # mongoengine.connection
    "DEFAULT_CONNECTION_NAME",
    "DEFAULT_DATABASE_NAME",
    "ConnectionFailure",
    "connect",
    "disconnect",
    "disconnect_all",
    "get_connection",
    "get_db",
    "register_connection",
    # mongoengine.queryset
    "AggregationResult",
    "QuerySet",
    "QuerySetNoCache",
    "Q",
    "queryset_manager",
    "QuerySetManager",
    "QueryFieldList",
    "DO_NOTHING",
    "NULLIFY",
    "CASCADE",
    "DENY",
    "PULL",
    "DoesNotExist",
    "InvalidQueryError",
    "MultipleObjectsReturned",
    # mongoengine.signals
    "pre_init",
    "post_init",
    "pre_save",
    "pre_save_async",
    "pre_save_post_validation",
    "pre_save_post_validation_async",
    "post_save",
    "post_save_async",
    "pre_delete",
    "pre_delete_async",
    "post_delete",
    "post_delete_async",
    "pre_bulk_insert",
    "pre_bulk_insert_async",
    "post_bulk_insert",
    "post_bulk_insert_async",
    # mongoengine.errors
    "NotRegistered",
    "InvalidDocumentError",
    "LookUpError",
    "BulkWriteError",
    "FieldDoesNotExist",
    "ValidationError",
    "SaveConditionError",
    "DeprecatedError",
)


VERSION = (0, 29, 0)


def get_version():
    """Return the VERSION as a string.

    For example, if `VERSION == (0, 10, 7)`, return '0.10.7'.
    """
    return ".".join(map(str, VERSION))


__version__ = get_version()
