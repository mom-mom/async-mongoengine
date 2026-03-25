"""
Helper functions, constants, and types to aid with PyMongo support.
"""

import pymongo
from bson import binary, json_util

from mongoengine import connection

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

LEGACY_JSON_OPTIONS = json_util.LEGACY_JSON_OPTIONS.with_options(
    uuid_representation=binary.UuidRepresentation.PYTHON_LEGACY,
)


async def count_documents(collection, filter, skip=None, limit=None, hint=None, collation=None):
    """Count documents using pymongo's count_documents"""
    if limit == 0:
        return 0

    kwargs = {}
    if skip is not None:
        kwargs["skip"] = skip
    if limit is not None:
        kwargs["limit"] = limit
    if hint not in (-1, None):
        kwargs["hint"] = hint
    if collation is not None:
        kwargs["collation"] = collation

    is_active_session = connection._get_session() is not None
    if not filter and set(kwargs) <= {"max_time_ms"} and not is_active_session:
        return await collection.estimated_document_count(**kwargs)
    else:
        return await collection.count_documents(filter=filter, session=connection._get_session(), **kwargs)


async def list_collection_names(db, include_system_collections=False):
    """List collection names using pymongo's list_collection_names"""
    collections = await db.list_collection_names(session=connection._get_session())

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
