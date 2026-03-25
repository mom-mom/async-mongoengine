import re
from typing import TYPE_CHECKING, Any, ClassVar, Self

import pymongo
from bson.dbref import DBRef
from pymongo.read_preferences import ReadPreference

from mongoengine import signals
from mongoengine.base import (
    BaseDict,
    BaseDocument,
    BaseList,
    DocumentMetaclass,
    EmbeddedDocumentList,
    TopLevelDocumentMetaclass,
    _DocumentRegistry,
)
from mongoengine.base.utils import NonOrderedList
from mongoengine.common import _import_class
from mongoengine.connection import (
    DEFAULT_CONNECTION_NAME,
    _get_session,
    get_db,
)
from mongoengine.context_managers import (
    set_write_concern,
    switch_collection,
    switch_db,
)
from mongoengine.errors import (
    InvalidDocumentError,
    InvalidQueryError,
    SaveConditionError,
)
from mongoengine.pymongo_support import list_collection_names
from mongoengine.queryset import (
    NotUniqueError,
    OperationError,
    QuerySet,
    transform,
)

__all__ = (
    "Document",
    "EmbeddedDocument",
    "DynamicDocument",
    "DynamicEmbeddedDocument",
    "OperationError",
    "InvalidCollectionError",
    "NotUniqueError",
    "MapReduceDocument",
)


def includes_cls(fields: list[Any] | tuple[Any, ...]) -> bool:
    """Helper function used for ensuring and comparing indexes."""
    first_field: Any = None
    if len(fields):
        if isinstance(fields[0], str):
            first_field = fields[0]
        elif isinstance(fields[0], (list, tuple)) and len(fields[0]):
            first_field = fields[0][0]
    return first_field == "_cls"


class InvalidCollectionError(Exception):
    pass


async def _generate_async_fields(doc: Any) -> None:
    """Pre-generate SequenceField values for *doc* and any embedded
    sub-documents recursively.

    Handles all nesting patterns:
    - ``EmbeddedDocumentField(Comment)``
    - ``EmbeddedDocumentListField(Comment)``
    - ``ListField(EmbeddedDocumentField(Comment))``

    Must be called before ``validate()`` and ``to_mongo()``.
    """
    SequenceField = _import_class("SequenceField")
    EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
    ComplexBaseField = _import_class("ComplexBaseField")

    for name, field in doc._fields.items():
        if isinstance(field, SequenceField) and doc._data.get(name) is None:
            doc._data[name] = await field.generate()
        elif isinstance(field, EmbeddedDocumentField):
            item = doc._data.get(name)
            if item is not None and hasattr(item, "_fields"):
                await _generate_async_fields(item)
        elif isinstance(field, ComplexBaseField):
            # Covers ListField(EmbeddedDocumentField(...)),
            # EmbeddedDocumentListField, and similar wrappers.
            inner = getattr(field, "field", None)
            if isinstance(inner, EmbeddedDocumentField):
                items = doc._data.get(name) or []
                if isinstance(items, dict):
                    items = items.values()
                for item in items:
                    if item is not None and hasattr(item, "_fields"):
                        await _generate_async_fields(item)


class EmbeddedDocument(BaseDocument, metaclass=DocumentMetaclass):
    r"""A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.

    A :class:`~mongoengine.EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To enable this behaviour set :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.
    """

    __slots__ = ("_instance",)

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass

    # A generic embedded document doesn't have any immutable properties
    # that describe it uniquely, hence it shouldn't be hashable. You can
    # define your own __hash__ method on a subclass if you need your
    # embedded documents to be hashable.
    __hash__ = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._instance: Any = None
        self._changed_fields: list[str] = []

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self._data == other._data
        return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __getstate__(self) -> dict[str, Any]:
        data = super().__getstate__()
        data["_instance"] = None
        return data

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)
        self._instance = state["_instance"]

    def to_mongo(self, *args: Any, **kwargs: Any) -> Any:
        data = super().to_mongo(*args, **kwargs)

        # remove _id from the SON if it's in it and it's None
        if "_id" in data and data["_id"] is None:
            del data["_id"]

        return data


class Document(BaseDocument, metaclass=TopLevelDocumentMetaclass):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to snake_case. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To enable this behaviour set :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. :attr:`max_size` is rounded up
    to the next multiple of 256 by MongoDB internally and mongoengine before.
    Use also a multiple of 256 to avoid confusions.  If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10485760 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    :attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a
    deployment system.

    By default, _cls will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting cls to False on the specific index or
    by setting index_cls to False on the meta dictionary for the document.

    By default, any extra attribute existing in stored data but not declared
    in your model will raise a :class:`~mongoengine.FieldDoesNotExist` error.
    This can be disabled by setting :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
    """

    if TYPE_CHECKING:
        from mongoengine.queryset.manager import QuerySetManager

        objects: ClassVar[QuerySetManager[Self]]

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass

    __slots__ = ("_objects",)

    @property
    def pk(self) -> Any:
        """Get the primary key."""
        if "id_field" not in self._meta:
            return None
        return getattr(self, self._meta["id_field"])

    @pk.setter
    def pk(self, value: Any) -> None:
        """Set the primary key."""
        return setattr(self, self._meta["id_field"], value)

    def __hash__(self) -> int:
        """Return the hash based on the PK of this document. If it's new
        and doesn't have a PK yet, return the default object hash instead.
        """
        if self.pk is None:
            return super(BaseDocument, self).__hash__()

        return hash(self.pk)

    @classmethod
    def _get_db(cls) -> Any:
        """Some Model using other db_alias"""
        return get_db(cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME))

    @classmethod
    def _disconnect(cls) -> None:
        """Detach the Document class from the (cached) database collection"""
        cls._collection = None

    @classmethod
    async def _get_collection(cls) -> Any:
        """Return the PyMongo collection corresponding to this document.

        Upon first call, this method:
        1. Initializes a :class:`~pymongo.collection.Collection` corresponding
           to this document.
        2. Creates indexes defined in this document's :attr:`meta` dictionary.
           This happens only if `auto_create_index` is True.
        """
        if not hasattr(cls, "_collection") or cls._collection is None:
            # Get the collection, either capped or regular.
            if cls._meta.get("max_size") or cls._meta.get("max_documents"):
                cls._collection = await cls._get_capped_collection()
            elif cls._meta.get("timeseries"):
                cls._collection = await cls._get_timeseries_collection()
            else:
                db = cls._get_db()
                collection_name = cls._get_collection_name()
                cls._collection = db[collection_name]

            # Ensure indexes on the collection unless auto_create_index was
            # set to False. Plus, there is no need to ensure indexes on slave.
            db = cls._get_db()
            if cls._meta.get("auto_create_index", True) and await db.client.is_primary:
                await cls.ensure_indexes()

        return cls._collection

    @classmethod
    async def _get_capped_collection(cls) -> Any:
        """Create a new or get an existing capped PyMongo collection."""
        db = cls._get_db()
        collection_name = cls._get_collection_name()

        # Get max document limit and max byte size from meta.
        max_size: int = cls._meta.get("max_size") or 10 * 2**20  # 10MB default
        max_documents: int | None = cls._meta.get("max_documents")

        # MongoDB will automatically raise the size to make it a multiple of
        # 256 bytes. We raise it here ourselves to be able to reliably compare
        # the options below.
        if max_size % 256:
            max_size = (max_size // 256 + 1) * 256

        # If the collection already exists and has different options
        # (i.e. isn't capped or has different max/size), raise an error.
        if collection_name in await list_collection_names(db, include_system_collections=True):
            collection = db[collection_name]
            options: dict[str, Any] = await collection.options()
            if options.get("max") != max_documents or options.get("size") != max_size:
                raise InvalidCollectionError(
                    f'Cannot create collection "{cls._collection}" as a capped collection as it already exists'
                )

            return collection

        # Create a new capped collection.
        opts: dict[str, Any] = {"capped": True, "size": max_size}
        if max_documents:
            opts["max"] = max_documents

        return await db.create_collection(collection_name, session=_get_session(), **opts)

    @classmethod
    async def _get_timeseries_collection(cls) -> Any:
        """Create a new or get an existing timeseries PyMongo collection."""
        db = cls._get_db()
        collection_name = cls._get_collection_name()  # type: ignore[assignment]
        timeseries_opts: dict[str, Any] | None = cls._meta.get("timeseries")

        if collection_name in await list_collection_names(db, include_system_collections=True):
            collection = db[collection_name]
            await collection.options()
            return collection

        opts: dict[str, Any] = {"expireAfterSeconds": timeseries_opts.pop("expireAfterSeconds", None)}
        return await db.create_collection(
            name=collection_name,
            timeseries=timeseries_opts,
            **opts,
        )

    def to_mongo(self, *args: Any, **kwargs: Any) -> Any:
        data = super().to_mongo(*args, **kwargs)

        # If '_id' is None, try and set it from self._data. If that
        # doesn't exist either, remove '_id' from the SON completely.
        if data["_id"] is None:
            if self._data.get("id") is None:
                del data["_id"]
            else:
                data["_id"] = self._data["id"]

        return data

    async def modify(self, query: dict[str, Any] | None = None, **update: Any) -> bool:
        """Perform an atomic update of the document in the database and reload
        the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn't match the query.

        .. note:: All unsaved changes that have been made to the document are
            rejected if the method returns True.

        :param query: the update will be performed only if the document in the
            database matches the query
        :param update: Django-style update keyword arguments
        """
        if query is None:
            query = {}

        if self.pk is None:
            raise InvalidDocumentError("The document does not have a primary key.")

        id_field: str = self._meta["id_field"]
        query = query.copy() if isinstance(query, dict) else query.to_query(self)

        if id_field not in query:
            query[id_field] = self.pk
        elif query[id_field] != self.pk:
            raise InvalidQueryError("Invalid document modify query: it must modify only this document.")

        # Need to add shard key to query, or you get an error
        query.update(self._object_key)

        updated: Any = await self._qs(**query).modify(new=True, **update)
        if updated is None:
            return False

        for field in self._fields_ordered:
            setattr(self, field, self._reload(field, updated[field]))

        self._changed_fields = updated._changed_fields
        self._created = False

        return True

    async def save(
        self,
        force_insert: bool = False,
        validate: bool = True,
        clean: bool = True,
        write_concern: dict[str, Any] | None = None,
        cascade: bool | None = None,
        cascade_kwargs: dict[str, Any] | None = None,
        _refs: list[Any] | None = None,
        save_condition: dict[str, Any] | None = None,
        signal_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created. Returns the saved object instance.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        .. versionchanged:: 0.26
           save() no longer calls :meth:`~mongoengine.Document.ensure_indexes`
           unless ``meta['auto_create_index_on_save']`` is set to True.

        """
        signal_kwargs = signal_kwargs or {}

        if self._meta.get("abstract"):
            raise InvalidDocumentError("Cannot save an abstract document.")

        signals.pre_save.send(self.__class__, document=self, **signal_kwargs)
        await signals.pre_save_async.send_async(self.__class__, document=self, **signal_kwargs)

        if write_concern is None:
            write_concern = {}

        # Pre-generate async field values BEFORE validation and to_mongo().
        # Must run before validate() because SequenceField._auto_gen=True
        # exempts it from required-field checks only when value is already set.
        # Must run before to_mongo() because to_mongo() is sync.
        # Recurses into embedded documents.
        await _generate_async_fields(self)

        if validate:
            self.validate(clean=clean)

        doc_id: Any = self.to_mongo(fields=[self._meta["id_field"]])
        created: bool = "_id" not in doc_id or self._created or force_insert

        signals.pre_save_post_validation.send(self.__class__, document=self, created=created, **signal_kwargs)
        await signals.pre_save_post_validation_async.send_async(
            self.__class__, document=self, created=created, **signal_kwargs
        )
        # it might be refreshed by the pre_save_post_validation hook, e.g., for etag generation
        doc: Any = self.to_mongo()

        # Initialize the Document's underlying pymongo.Collection (+create indexes) if not already initialized
        # Important to do this here to avoid that the index creation gets wrapped in the try/except block below
        # and turned into mongoengine.OperationError
        if self._collection is None:
            _ = await self._get_collection()
        elif self._meta.get("auto_create_index_on_save", False):
            # ensure_indexes is called as part of _get_collection so no need to re-call it again here
            await self.ensure_indexes()

        try:
            # Save a new document or update an existing one
            if created:
                object_id: Any = await self._save_create(
                    doc=doc, force_insert=force_insert, write_concern=write_concern
                )
            else:
                object_id, created = await self._save_update(doc, save_condition, write_concern)

            if cascade is None:
                cascade = self._meta.get("cascade", False) or cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade,
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs["_refs"] = _refs
                await self.cascade_save(**kwargs)

        except pymongo.errors.DuplicateKeyError as err:
            message = "Tried to save duplicate unique keys (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Make sure we store the PK on this document now that it's saved
        id_field: str = self._meta["id_field"]
        if created or id_field not in self._meta.get("shard_key", []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        signals.post_save.send(self.__class__, document=self, created=created, **signal_kwargs)
        await signals.post_save_async.send_async(self.__class__, document=self, created=created, **signal_kwargs)

        self._clear_changed_fields()
        self._created = False

        return self

    async def _save_create(self, doc: Any, force_insert: bool, write_concern: dict[str, Any]) -> Any:
        """Save a new document.

        Helper method, should only be used inside save().
        """
        # Use instance-level _collection if set (e.g. via switch_db/switch_collection),
        # otherwise fall back to the class-level async _get_collection()
        collection: Any = self._collection if self._collection is not None else await self.__class__._get_collection()
        with set_write_concern(collection, write_concern) as wc_collection:
            if force_insert:
                return (await wc_collection.insert_one(doc, session=_get_session())).inserted_id
            # insert_one will provoke UniqueError alongside save does not
            # therefore, it need to catch and call replace_one.
            if "_id" in doc:
                select_dict: dict[str, Any] = {"_id": doc["_id"]}
                select_dict = self._integrate_shard_key(doc, select_dict)
                raw_object: Any = await wc_collection.find_one_and_replace(select_dict, doc, session=_get_session())
                if raw_object:
                    return doc["_id"]

            object_id: Any = (await wc_collection.insert_one(doc, session=_get_session())).inserted_id

        return object_id

    def _get_update_doc(self) -> dict[str, Any]:
        """Return a dict containing all the $set and $unset operations
        that should be sent to MongoDB based on the changes made to this
        Document.
        """
        updates, removals = self._delta()

        update_doc: dict[str, Any] = {}
        if updates:
            update_doc["$set"] = updates
        if removals:
            update_doc["$unset"] = removals

        return update_doc

    def _integrate_shard_key(self, doc: Any, select_dict: dict[str, Any]) -> dict[str, Any]:
        """Integrates the collection's shard key to the `select_dict`, which will be used for the query.
        The value from the shard key is taken from the `doc` and finally the select_dict is returned.
        """

        # Need to add shard key to query, or you get an error
        shard_key: tuple[str, ...] = self._meta.get("shard_key", tuple())
        for k in shard_key:
            path = self._lookup_field(k.split("."))
            actual_key: list[str] = [p.db_field for p in path]
            val: Any = doc
            for ak in actual_key:
                val = val[ak]
            select_dict[".".join(actual_key)] = val

        return select_dict

    async def _save_update(
        self, doc: Any, save_condition: dict[str, Any] | None, write_concern: dict[str, Any]
    ) -> tuple[Any, bool]:
        """Update an existing document.

        Helper method, should only be used inside save().
        """
        collection: Any = self._collection if self._collection is not None else await self.__class__._get_collection()
        object_id: Any = doc["_id"]
        created: bool = False

        select_dict: dict[str, Any] = {}
        if save_condition is not None:
            select_dict = transform.query(self.__class__, **save_condition)

        select_dict["_id"] = object_id

        select_dict = self._integrate_shard_key(doc, select_dict)

        update_doc: dict[str, Any] = self._get_update_doc()
        if update_doc:
            upsert: bool = save_condition is None
            with set_write_concern(collection, write_concern) as wc_collection:
                last_error: dict[str, Any] | None = (
                    await wc_collection.update_one(select_dict, update_doc, upsert=upsert, session=_get_session())
                ).raw_result
            if not upsert and last_error is not None and last_error["n"] == 0:
                raise SaveConditionError("Race condition preventing document update detected")
            if last_error is not None:
                updated_existing: bool | None = last_error.get("updatedExisting")
                if updated_existing is False:
                    created = True
                    # !!! This is bad, means we accidentally created a new,
                    # potentially corrupted document. See
                    # https://github.com/MongoEngine/mongoengine/issues/564

        return object_id, created

    async def cascade_save(self, **kwargs: Any) -> None:
        """Recursively save any references and generic references on the
        document.
        """
        _refs: list[str] = kwargs.get("_refs") or []

        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")

        for name, cls in self._fields.items():
            if not isinstance(cls, (ReferenceField, GenericReferenceField)):
                continue

            ref: Any = self._data.get(name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, "_changed_fields", True):
                continue

            ref_id: str = f"{ref.__class__.__name__},{str(ref._data)}"
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                await ref.save(**kwargs)
                ref._changed_fields = []

    @property
    def _qs(self) -> Any:
        """Return the default queryset corresponding to this document."""
        try:
            qs = self._objects
        except AttributeError:
            qs = None
        if qs is None:
            queryset_class = self._meta.get("queryset_class", QuerySet)
            self._objects = queryset_class(self.__class__, self.__class__._collection)
        return self._objects

    @property
    def _object_key(self) -> dict[str, Any]:
        """Return a query dict that can be used to fetch this document.

        Most of the time the dict is a simple PK lookup, but in case of
        a sharded collection with a compound shard key, it can contain a more
        complex query.

        Note that the dict returned by this method uses MongoEngine field
        names instead of PyMongo field names (e.g. "pk" instead of "_id",
        "some__nested__field" instead of "some.nested.field", etc.).
        """
        select_dict: dict[str, Any] = {"pk": self.pk}
        shard_key: tuple[str, ...] = self.__class__._meta.get("shard_key", tuple())
        for k in shard_key:
            val: Any = self
            field_parts: list[str] = k.split(".")
            for part in field_parts:
                val = getattr(val, part)
            select_dict["__".join(field_parts)] = val
        return select_dict

    async def update(self, **kwargs: Any) -> None:
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        if self.pk is None:
            if kwargs.get("upsert", False):
                query: Any = self.to_mongo()
                if "_cls" in query:
                    del query["_cls"]
                return await self._qs.filter(**query).update_one(**kwargs)
            else:
                raise OperationError("attempt to update a document not yet saved")

        # Need to add shard key to query, or you get an error
        return await self._qs.filter(**self._object_key).update_one(**kwargs)

    async def delete(self, signal_kwargs: dict[str, Any] | None = None, **write_concern: Any) -> None:
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant ``getLastError`` command.
            For example, ``save(..., w: 2, fsync: True)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """
        signal_kwargs = signal_kwargs or {}
        signals.pre_delete.send(self.__class__, document=self, **signal_kwargs)
        await signals.pre_delete_async.send_async(self.__class__, document=self, **signal_kwargs)

        try:
            await self._qs.filter(**self._object_key).delete(write_concern=write_concern, _from_doc_delete=True)
        except pymongo.errors.OperationFailure as err:
            message = f"Could not delete document ({err.args})"
            raise OperationError(message)
        signals.post_delete.send(self.__class__, document=self, **signal_kwargs)
        await signals.post_delete_async.send_async(self.__class__, document=self, **signal_kwargs)

    async def switch_db(self, db_alias: str, keep_created: bool = True) -> Self:
        """
        Temporarily switch the database for a document instance.

        Only really useful for archiving off data and calling `save()`::

            user = User.objects.get(id=user_id)
            await user.switch_db('archive-db')
            await user.save()

        :param str db_alias: The database alias to use for saving the document

        :param bool keep_created: keep self._created value after switching db, else is reset to True


        .. seealso::
            Use :class:`~mongoengine.context_managers.switch_collection`
            if you need to read from another collection
        """
        async with switch_db(self.__class__, db_alias) as cls:
            collection: Any = await cls._get_collection()
        self._collection = collection
        self._created = True if not keep_created else self._created
        queryset_class = self._meta.get("queryset_class", QuerySet)
        self._objects = queryset_class(self.__class__, collection)
        return self

    async def switch_collection(self, collection_name: str, keep_created: bool = True) -> Self:
        """
        Temporarily switch the collection for a document instance.

        Only really useful for archiving off data and calling `save()`::

            user = User.objects.get(id=user_id)
            await user.switch_collection('old-users')
            await user.save()

        :param str collection_name: The database alias to use for saving the
            document

        :param bool keep_created: keep self._created value after switching collection, else is reset to True


        .. seealso::
            Use :class:`~mongoengine.context_managers.switch_db`
            if you need to read from another database
        """
        async with switch_collection(self.__class__, collection_name) as cls:
            collection: Any = await cls._get_collection()
        self._collection = collection
        self._created = True if not keep_created else self._created
        queryset_class = self._meta.get("queryset_class", QuerySet)
        self._objects = queryset_class(self.__class__, collection)
        return self

    async def select_related(self, max_depth: int = 1) -> Self:
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.
        """
        DeReference = _import_class("DeReference")
        await DeReference()([self], max_depth + 1)
        return self

    async def reload(self, *fields: str, **kwargs: Any) -> Self:
        """Reloads all attributes from the database.

        :param fields: (optional) args list of fields to reload
        :param max_depth: (optional) depth of dereferencing to follow
        """
        if fields and isinstance(fields[0], int):
            fields[0]
            fields = fields[1:]
        elif "max_depth" in kwargs:
            kwargs["max_depth"]

        if self.pk is None:
            raise self.DoesNotExist("Document does not exist")

        queryset: Any = (
            self._qs.read_preference(ReadPreference.PRIMARY).filter(**self._object_key).only(*fields).limit(1)
        )

        obj: Any = await queryset.first()
        if obj is None:
            raise self.DoesNotExist("Document does not exist")
        for field in obj._data:
            if not fields or field in fields:
                try:
                    setattr(self, field, self._reload(field, obj[field]))
                except (KeyError, AttributeError):
                    try:
                        # If field is a special field, e.g. items is stored as _reserved_items,
                        # a KeyError is thrown. So try to retrieve the field from _data
                        setattr(self, field, self._reload(field, obj._data.get(field)))
                    except KeyError:
                        # If field is removed from the database while the object
                        # is in memory, a reload would cause a KeyError
                        # i.e. obj.update(unset__field=1) followed by obj.reload()
                        delattr(self, field)

        self._changed_fields = list(set(self._changed_fields) - set(fields)) if fields else obj._changed_fields
        self._created = False
        return self

    def _reload(self, key: str, value: Any) -> Any:
        """Used by :meth:`~mongoengine.Document.reload` to ensure the
        correct instance is linked to self.
        """
        if isinstance(value, BaseDict):
            value = [(k, self._reload(k, v)) for k, v in value.items()]
            value = BaseDict(value, self, key)
        elif isinstance(value, EmbeddedDocumentList):
            value = [self._reload(key, v) for v in value]
            value = EmbeddedDocumentList(value, self, key)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            value = BaseList(value, self, key)
        elif isinstance(value, (EmbeddedDocument, DynamicEmbeddedDocument)):
            value._instance = None
            value._changed_fields = []
        return value

    def to_dbref(self) -> DBRef:
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if self.pk is None:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)  # type: ignore[arg-type]

    @classmethod
    def register_delete_rule(cls, document_cls: Any, field_name: str, rule: int) -> None:
        """This method registers the delete rules to apply when removing this
        object.
        """
        classes: list[Any] = [
            _DocumentRegistry.get(class_name) for class_name in cls._subclasses if class_name != cls.__name__
        ] + [cls]
        documents: list[Any] = [
            _DocumentRegistry.get(class_name)
            for class_name in document_cls._subclasses
            if class_name != document_cls.__name__
        ] + [document_cls]

        for klass in classes:
            for document_cls in documents:
                delete_rules: dict[tuple[Any, str], int] = klass._meta.get("delete_rules") or {}
                delete_rules[(document_cls, field_name)] = rule
                klass._meta["delete_rules"] = delete_rules

    @classmethod
    async def drop_collection(cls) -> None:
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.

        Raises :class:`OperationError` if the document has no collection set
        (i.g. if it is `abstract`)
        """
        coll_name: str | None = cls._get_collection_name()
        if not coll_name:
            raise OperationError(f"Document {cls} has no collection defined (is it abstract ?)")
        cls._collection = None
        db: Any = cls._get_db()
        await db.drop_collection(coll_name, session=_get_session())

    @classmethod
    async def create_index(cls, keys: str | list[Any], background: bool = False, **kwargs: Any) -> str:
        """Creates the given indexes if required.

        :param keys: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        :param background: Allows index creation in the background
        """
        index_spec: dict[str, Any] = cls._build_index_spec(keys)
        index_spec = index_spec.copy()
        fields: list[tuple[str, int]] = index_spec.pop("fields")
        index_spec["background"] = background
        index_spec.update(kwargs)

        collection: Any = await cls._get_collection()
        return await collection.create_index(fields, session=_get_session(), **index_spec)

    @classmethod
    async def ensure_indexes(cls) -> None:
        """Checks the document meta data and ensures all the indexes exist.

        Global defaults can be set in the meta - see :doc:`guide/defining-documents`

        By default, this will get called automatically upon first interaction with the
        Document collection (query, save, etc) so unless you disabled `auto_create_index`, you
        shouldn't have to call this manually.

        This also gets called upon every call to Document.save if `auto_create_index_on_save` is set to True

        If called multiple times, MongoDB will not re-recreate indexes if they exist already

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        background: bool = cls._meta.get("index_background", False)
        index_opts: dict[str, Any] = cls._meta.get("index_opts") or {}
        index_cls: bool = cls._meta.get("index_cls", True)

        collection: Any = await cls._get_collection()

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed: bool = False

        # Ensure document-defined indexes are created
        if cls._meta["index_specs"]:
            index_spec: list[dict[str, Any]] = cls._meta["index_specs"]
            for spec in index_spec:
                spec = spec.copy()
                fields: list[tuple[str, int]] = spec.pop("fields")
                cls_indexed = cls_indexed or includes_cls(fields)
                opts: dict[str, Any] = index_opts.copy()
                opts.update(spec)

                # we shouldn't pass 'cls' to the collection.ensureIndex options
                # because of https://jira.mongodb.org/browse/SERVER-769
                if "cls" in opts:
                    del opts["cls"]

                await collection.create_index(fields, background=background, session=_get_session(), **opts)

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if index_cls and not cls_indexed and cls._meta.get("allow_inheritance"):
            # we shouldn't pass 'cls' to the collection.ensureIndex options
            # because of https://jira.mongodb.org/browse/SERVER-769
            if "cls" in index_opts:
                del index_opts["cls"]

            await collection.create_index("_cls", background=background, session=_get_session(), **index_opts)

    @classmethod
    async def list_indexes(cls) -> list[list[tuple[str, int]]]:
        """Lists all indexes that should be created for the Document collection.
        It includes all the indexes from super- and sub-classes.

        Note that it will only return the indexes' fields, not the indexes' options
        """
        if cls._meta.get("abstract"):
            return []

        # get all the base classes, subclasses and siblings
        classes: list[Any] = []

        async def get_classes(cls: Any) -> None:
            if cls not in classes and isinstance(cls, TopLevelDocumentMetaclass):
                classes.append(cls)

            for base_cls in cls.__bases__:
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and base_cls != Document
                    and not base_cls._meta.get("abstract")
                    and (await base_cls._get_collection()).full_name == (await cls._get_collection()).full_name
                    and base_cls not in classes
                ):
                    classes.append(base_cls)
                    await get_classes(base_cls)
            for subclass in cls.__subclasses__():
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and (await subclass._get_collection()).full_name == (await cls._get_collection()).full_name
                    and subclass not in classes
                ):
                    classes.append(subclass)
                    await get_classes(subclass)

        await get_classes(cls)

        # get the indexes spec for all the gathered classes
        def get_indexes_spec(cls: Any) -> list[list[tuple[str, int]]]:
            indexes: list[list[tuple[str, int]]] = []

            if cls._meta["index_specs"]:
                index_spec: list[dict[str, Any]] = cls._meta["index_specs"]
                for spec in index_spec:
                    spec = spec.copy()
                    fields: list[tuple[str, int]] = spec.pop("fields")
                    indexes.append(fields)
            return indexes

        indexes: list[list[tuple[str, int]]] = []
        for klass in classes:
            for index in get_indexes_spec(klass):
                if index not in indexes:
                    indexes.append(index)

        # finish up by appending { '_id': 1 } and { '_cls': 1 }, if needed
        if [("_id", 1)] not in indexes:
            indexes.append([("_id", 1)])
        if cls._meta.get("index_cls", True) and cls._meta.get("allow_inheritance"):
            indexes.append([("_cls", 1)])

        return indexes

    @classmethod
    async def compare_indexes(cls) -> dict[str, list[list[tuple[str, int]]]]:
        """Compares the indexes defined in MongoEngine with the ones
        existing in the database. Returns any missing/extra indexes.
        """

        required: list[list[tuple[str, int]]] = await cls.list_indexes()

        existing: list[Any] = []
        collection: Any = await cls._get_collection()
        for info in (await collection.index_information(session=_get_session())).values():
            if "_fts" in info["key"][0]:
                # Useful for text indexes (but not only)
                index_type: Any = info["key"][0][1]
                text_index_fields: Any = info.get("weights").keys()
                # Use NonOrderedList to avoid order comparison, see #2612
                existing.append(NonOrderedList([(key, index_type) for key in text_index_fields]))
            else:
                existing.append(info["key"])

        missing: list[Any] = [index for index in required if index not in existing]
        extra: list[Any] = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [("_cls", 1)] in missing:
            cls_obsolete: bool = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([("_cls", 1)])

        return {"missing": missing, "extra": extra}


class DynamicDocument(Document, metaclass=TopLevelDocumentMetaclass):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expanded style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.fields.DynamicField` and data can be attributed to that
    field.

    .. note::

        There is one caveat on Dynamic Documents: undeclared fields cannot start with `_`
    """

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args: Any, **kwargs: Any) -> None:
        """Delete the attribute by setting to None and allowing _delta
        to unset it.
        """
        field_name: str = args[0]
        if field_name in self._dynamic_fields:
            setattr(self, field_name, None)
            self._dynamic_fields[field_name].null = False
        else:
            super().__delattr__(*args, **kwargs)


class DynamicEmbeddedDocument(EmbeddedDocument, metaclass=DocumentMetaclass):
    """A Dynamic Embedded Document class allowing flexible, expandable and
    uncontrolled schemas. See :class:`~mongoengine.DynamicDocument` for more
    information about dynamic documents.
    """

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args: Any, **kwargs: Any) -> None:
        """Delete the attribute by setting to None and allowing _delta
        to unset it.
        """
        field_name: str = args[0]
        if field_name in self._fields:
            default: Any = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            setattr(self, field_name, None)


class MapReduceDocument:
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.
    """

    def __init__(self, document: Any, collection: Any, key: Any, value: Any) -> None:
        self._document: Any = document
        self._collection: Any = collection
        self.key: Any = key
        self.value: Any = value

    async def get_object(self) -> Any:
        """Async version of object property. Lazy-load the object referenced
        by ``self.key``. ``self.key`` should be the ``primary_key``.
        """
        id_field: Any = self._document()._meta["id_field"]
        id_field_type: type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except Exception:
                raise Exception(f"Could not cast key as {id_field_type.__name__}")

        if not hasattr(self, "_key_object"):
            self._key_object: Any = await self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object
