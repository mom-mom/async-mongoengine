import numbers
import warnings
import weakref
from functools import partial
from typing import Any, Self

import pymongo
from bson import SON, DBRef, ObjectId, json_util

from mongoengine import signals
from mongoengine.base.common import _DocumentRegistry
from mongoengine.base.datastructures import (
    BaseDict,
    BaseList,
    EmbeddedDocumentList,
    LazyReference,
    StrictDict,
)
from mongoengine.base.fields import BaseField, ComplexBaseField
from mongoengine.common import _import_class
from mongoengine.errors import (
    FieldDoesNotExist,
    InvalidDocumentError,
    LookUpError,
    OperationError,
    ValidationError,
)
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS

__all__ = ("BaseDocument", "NON_FIELD_ERRORS")


class _MongoDict(dict):
    """A plain dict with a to_dict() method for SON compatibility.

    SON (bson.son.SON) maintains key order via an internal list, making
    __setitem__/pop/update O(n).  Python 3.7+ dicts are insertion-ordered
    and much faster.  This subclass adds only the to_dict() convenience
    method that existing code may call on to_mongo() return values.
    """

    __slots__ = ()

    def to_dict(self) -> dict[str, Any]:
        return dict(self)


NON_FIELD_ERRORS = "__all__"

# Keys that may appear in a SON dict but are not user-defined fields.
_KNOWN_EXTRA_KEYS = frozenset({"_cls", "_text_score"})

# Feature flag: when True, __init__ writes directly to _data bypassing
# __setattr__ and field descriptors for non-dynamic, non-STRICT documents.
FAST_INIT = True

# Module-level set of keys allowed in addition to declared fields
_INIT_ALLOWED_EXTRA_KEYS = frozenset(("id", "pk", "_cls", "_text_score"))

try:
    GEOHAYSTACK = pymongo.GEOHAYSTACK
except AttributeError:
    GEOHAYSTACK = None


class BaseDocument:
    # TODO simplify how `_changed_fields` is used.
    # Currently, handling of `_changed_fields` seems unnecessarily convoluted:
    # 1. `BaseDocument` defines `_changed_fields` in its `__slots__`, yet it's
    #    not setting it to `[]` (or any other value) in `__init__`.
    # 2. `EmbeddedDocument` sets `_changed_fields` to `[]` it its overloaded
    #    `__init__`.
    # 3. `Document` does NOT set `_changed_fields` upon initialization. The
    #    field is primarily set via `_from_son` or `_clear_changed_fields`,
    #    though there are also other methods that manipulate it.
    # 4. The codebase is littered with `hasattr` calls for `_changed_fields`.
    __slots__ = (
        "_changed_fields",
        "_initialised",
        "_created",
        "_data",
        "_dynamic_fields",
        "_auto_id_field",
        "_db_field_map",
        "__weakref__",
    )

    _dynamic: bool = False
    _dynamic_lock: bool = True
    STRICT: bool = False

    # Cached _import_class result for EmbeddedDocument (used in fast __init__)
    _init_embedded_doc_type: type | None = None

    def __init__(self, *args: Any, **values: Any) -> None:
        """
        Initialise a document or an embedded document.

        :param values: A dictionary of keys and values for the document.
            It may contain additional reserved keywords, e.g. "__auto_convert".
        :param __auto_convert: If True, supplied values will be converted
            to Python-type values via each field's `to_python` method.
        :param _created: Indicates whether this is a brand new document
            or whether it's already been persisted before. Defaults to true.
        """
        if args:
            raise TypeError(
                "Instantiating a document with positional arguments is not "
                "supported. Please use `field_name=value` keyword arguments."
            )

        __auto_convert: bool = values.pop("__auto_convert", True)
        _created: bool = values.pop("_created", True)

        # Fast path: bypass __setattr__ and field descriptors entirely.
        # Requirements: FAST_INIT flag on, non-dynamic, non-STRICT, no
        # signal receivers, and auto_convert enabled.
        cls = self.__class__
        if (
            FAST_INIT
            and not self._dynamic
            and not self.STRICT
            and __auto_convert
            and not signals.pre_init.has_receivers_for(cls)
            and not signals.post_init.has_receivers_for(cls)
        ):
            self._fast_init(values, _created)
            return

        self._initialised = False
        self._created = True

        signals.pre_init.send(cls, document=self, values=values)

        # Check if there are undefined fields supplied to the constructor,
        # if so raise an Exception.
        if not self._dynamic and (self._meta.get("strict", True) or _created):
            _undefined_fields = set(values.keys()) - set(
                list(self._fields.keys()) + ["id", "pk", "_cls", "_text_score"]
            )
            if _undefined_fields:
                msg = f'The fields "{_undefined_fields}" do not exist on the document "{self._class_name}"'
                raise FieldDoesNotExist(msg)

        if self.STRICT and not self._dynamic:
            self._data = StrictDict.create(allowed_keys=self._fields_ordered)()
        else:
            self._data = {}

        self._dynamic_fields = SON()

        # Assign default values for fields
        # not set in the constructor
        for field_name in self._fields:
            if field_name in values:
                continue
            value = getattr(self, field_name, None)
            setattr(self, field_name, value)

        if "_cls" not in values:
            self._cls = self._class_name  # pyright: ignore[reportGeneralTypeIssues]

        # Set actual values
        dynamic_data: dict[str, Any] = {}
        for key, value in values.items():
            field = self._fields.get(key)
            if field or key in ("id", "pk", "_cls"):
                if __auto_convert and value is not None:
                    if field:
                        value = field.to_python(value)
                setattr(self, key, value)
            else:
                if self._dynamic:
                    dynamic_data[key] = value
                else:
                    # For strict Document
                    self._data[key] = value

        # Set any get_<field>_display methods
        self.__set_field_display()

        if self._dynamic:
            self._dynamic_lock = False  # pyright: ignore[reportGeneralTypeIssues]
            for key, value in dynamic_data.items():
                setattr(self, key, value)

        # Flag initialised
        self._initialised = True
        self._created = _created

        signals.post_init.send(cls, document=self)

    def _fast_init(self, values: dict[str, Any], _created: bool) -> None:
        """Fast __init__ path: bypasses __setattr__ overhead and batches
        embedded document _instance wiring.

        Only used for non-dynamic, non-STRICT documents without signal
        receivers.  Calls field.__set__ directly for fields with custom
        __set__ (e.g. BinaryField, EnumField, ComplexBaseField) to
        preserve field-specific conversion logic.
        """
        self._initialised = False
        self._created = True

        cls = self.__class__
        fields = cls._fields

        # Validate: reject undefined fields for strict documents
        if cls._meta.get("strict", True) or _created:
            undefined = set(values.keys()) - set(fields.keys()) - _INIT_ALLOWED_EXTRA_KEYS
            if undefined:
                msg = f'The fields "{undefined}" do not exist on the document "{cls._class_name}"'
                raise FieldDoesNotExist(msg)

        data: dict[str, Any] = {}
        self._data = data
        self._dynamic_fields = SON()

        # Cache EmbeddedDocument class for _instance wiring
        EmbeddedDocumentCls = BaseDocument._init_embedded_doc_type
        if EmbeddedDocumentCls is None:
            EmbeddedDocumentCls = _import_class("EmbeddedDocument")
            BaseDocument._init_embedded_doc_type = EmbeddedDocumentCls

        # 1. Populate defaults for fields not in values.
        #    Use field descriptor __set__ for fields with custom logic
        #    (e.g. EnumField, ComplexDateTimeField) to ensure proper conversion.
        _BaseField_set = BaseField.__set__
        for field_name, field in fields.items():
            if field_name in values:
                continue
            default = field.default
            if default is not None:
                if callable(default):
                    default = default()
            if type(field).__set__ is not _BaseField_set:
                field.__set__(self, default)
            else:
                data[field_name] = default

        # 2. Set _cls when allow_inheritance is True
        if "_cls" in fields and "_cls" not in values:
            data["_cls"] = cls._class_name

        # 3. Set supplied values with to_python conversion.
        #    For fields with custom __set__ (BinaryField, EnumField, etc.),
        #    delegate to the descriptor to preserve conversion logic.
        #    For standard BaseField fields, write directly to _data.
        #    id/pk go through setattr to honor Document.pk property.
        for key, value in values.items():
            field = fields.get(key)
            if field:
                if value is not None:
                    value = field.to_python(value)

                # Check if field has custom __set__ beyond BaseField
                if type(field).__set__ is not _BaseField_set:
                    field.__set__(self, value)
                else:
                    # Inline BaseField.__set__ null handling
                    if value is None:
                        if not field.null and field.default is not None:
                            value = field.default
                            if callable(value):
                                value = value()
                    data[key] = value
            elif key in ("id", "pk", "_cls"):
                # Use setattr to honor Document.pk property setter
                setattr(self, key, value)
            else:
                data[key] = value

        # 4. Wire up _instance on embedded documents (batch)
        proxy = weakref.proxy(self)
        for value in data.values():
            if isinstance(value, EmbeddedDocumentCls):
                value._instance = proxy
            elif isinstance(value, (list, tuple)):
                for v in value:
                    if isinstance(v, EmbeddedDocumentCls):
                        v._instance = proxy

        # 5. Set up get_<field>_display methods
        self.__set_field_display()

        # 6. Finalize
        self._initialised = True
        self._created = _created

    def __delattr__(self, *args: Any, **kwargs: Any) -> None:
        """Handle deletions of fields"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            super().__delattr__(*args, **kwargs)

    def __setattr__(self, name: str, value: Any) -> None:
        # Handle dynamic data only if an initialised dynamic document
        if self._dynamic and not self._dynamic_lock:
            if name not in self._fields_ordered and not name.startswith("_"):
                DynamicField = _import_class("DynamicField")
                field = DynamicField(db_field=name, null=True)
                field.name = name
                self._dynamic_fields[name] = field
                self._fields_ordered += (name,)  # pyright: ignore[reportGeneralTypeIssues]

            if not name.startswith("_"):
                value = self.__expand_dynamic_values(name, value)

            # Handle marking data as changed
            if name in self._dynamic_fields:
                self._data[name] = value
                if hasattr(self, "_changed_fields"):
                    self._mark_as_changed(name)
        try:
            self__created = self._created
        except AttributeError:
            self__created = True

        if (
            self._is_document
            and not self__created
            and name in self._meta.get("shard_key", tuple())
            and self._data.get(name) != value
        ):
            msg = f"Shard Keys are immutable. Tried to update {name}"
            raise OperationError(msg)

        try:
            self__initialised = self._initialised
        except AttributeError:
            self__initialised = False

        # Check if the user has created a new instance of a class
        if self._is_document and self__initialised and self__created and name == self._meta.get("id_field"):
            # When setting the ID field of an instance already instantiated and that was user-created (i.e not saved in db yet)
            # Typically this is when calling .save()
            super().__setattr__("_created", False)

        super().__setattr__(name, value)

    def __getstate__(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for k in (
            "_changed_fields",
            "_initialised",
            "_created",
            "_dynamic_fields",
            "_fields_ordered",
        ):
            if hasattr(self, k):
                data[k] = getattr(self, k)
        data["_data"] = self.to_mongo()
        return data

    def __setstate__(self, data: dict[str, Any]) -> None:
        if isinstance(data["_data"], (SON, _MongoDict)):
            data["_data"] = self.__class__._from_son(data["_data"])._data
        for k in (
            "_changed_fields",
            "_initialised",
            "_created",
            "_data",
            "_dynamic_fields",
        ):
            if k in data:
                setattr(self, k, data[k])
        if "_fields_ordered" in data:
            if self._dynamic:
                self._fields_ordered = data["_fields_ordered"]  # pyright: ignore[reportGeneralTypeIssues]
            else:
                _super_fields_ordered = type(self)._fields_ordered
                self._fields_ordered = _super_fields_ordered  # pyright: ignore[reportGeneralTypeIssues]

        dynamic_fields = data.get("_dynamic_fields") or SON()
        for k in dynamic_fields.keys():
            setattr(self, k, data["_data"].get(k))

    def __iter__(self) -> Any:
        return iter(self._fields_ordered)

    def __getitem__(self, name: str) -> Any:
        """Dictionary-style field access, return a field's value if present."""
        try:
            if name in self._fields_ordered:
                return getattr(self, name)
        except AttributeError:
            pass
        raise KeyError(name)

    def __setitem__(self, name: str, value: Any) -> None:
        """Dictionary-style field access, set a field's value."""
        # Ensure that the field exists before settings its value
        if not self._dynamic and name not in self._fields:
            raise KeyError(name)
        return setattr(self, name, value)

    def __contains__(self, name: object) -> bool:
        try:
            val = getattr(self, name)  # type: ignore[arg-type]
            return val is not None
        except AttributeError:
            return False

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        try:
            u = self.__str__()
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = "[Bad Unicode data]"
        repr_type = str if u is None else type(u)
        return repr_type(f"<{self.__class__.__name__}: {u}>")

    def __str__(self) -> str:
        # TODO this could be simpler?
        if hasattr(self, "__unicode__"):
            return self.__unicode__()
        return f"{self.__class__.__name__} object"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__) and hasattr(other, "id") and other.id is not None:
            return self.id == other.id
        if isinstance(other, DBRef):
            return self._get_collection_name() == other.collection and self.id == other.id
        if self.id is None:
            return self is other
        return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def clean(self) -> None:
        """
        Hook for doing document level data cleaning (usually validation or assignment)
        before validation is run.

        Any ValidationError raised by this method will not be associated with
        a particular field; it will have a special-case association with the
        field defined by NON_FIELD_ERRORS.
        """
        pass

    def get_text_score(self) -> Any:
        """
        Get text score from text query
        """

        if "_text_score" not in self._data:
            raise InvalidDocumentError(
                "This document is not originally built from a text query (or text_score was not set on search_text() call)"
            )

        return self._data["_text_score"]

    # Feature flag: when True, use the optimized to_mongo that replaces SON
    # with plain dict and caches per-field co_varnames introspection.
    _fast_to_mongo: bool = True

    def to_mongo(self, use_db_field: bool = True, fields: list[str] | None = None) -> Any:
        """
        Return as SON data ready for use with MongoDB.
        """
        if self._fast_to_mongo:
            return self._to_mongo_fast(use_db_field, fields)
        return self._to_mongo_legacy(use_db_field, fields)

    def _to_mongo_legacy(self, use_db_field: bool = True, fields: list[str] | None = None) -> Any:
        """Original to_mongo implementation using SON."""
        fields = fields or []

        data = SON()
        data["_id"] = None
        data["_cls"] = self._class_name

        # only root fields ['test1.a', 'test2'] => ['test1', 'test2']
        root_fields = {f.split(".")[0] for f in fields}

        for field_name in self:
            if root_fields and field_name not in root_fields:
                continue

            value = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if field is None and self._dynamic:
                field = self._dynamic_fields.get(field_name)

            if value is not None:
                f_inputs = field.to_mongo.__code__.co_varnames
                ex_vars: dict[str, Any] = {}
                if fields and "fields" in f_inputs:
                    key = f"{field_name}."
                    embedded_fields = [i.replace(key, "") for i in fields if i.startswith(key)]

                    ex_vars["fields"] = embedded_fields

                if "use_db_field" in f_inputs:
                    ex_vars["use_db_field"] = use_db_field

                value = field.to_mongo(value, **ex_vars)

            # Handle self generating fields.
            # Skip if generate() is a coroutine function (async) — those
            # must be pre-generated before to_mongo() is called (e.g. in
            # Document.save()).
            if value is None and field._auto_gen:
                import inspect

                if not inspect.iscoroutinefunction(field.generate):
                    value = field.generate()
                    self._data[field_name] = value

            if value is not None or field.null:
                if use_db_field:
                    data[field.db_field] = value
                else:
                    data[field.name] = value

        # Only add _cls if allow_inheritance is True
        if not self._meta.get("allow_inheritance"):
            data.pop("_cls")

        return data

    # Per-field cache: maps field class -> (accepts_use_db_field, accepts_fields)
    _to_mongo_sig_cache: dict[type, tuple[bool, bool]] = {}

    @staticmethod
    def _get_to_mongo_sig(field: Any) -> tuple[bool, bool]:
        """Return (accepts_use_db_field, accepts_fields) for a field's to_mongo."""
        cls = type(field)
        cache = BaseDocument._to_mongo_sig_cache
        sig = cache.get(cls)
        if sig is None:
            varnames = field.to_mongo.__code__.co_varnames
            sig = ("use_db_field" in varnames, "fields" in varnames)
            cache[cls] = sig
        return sig

    def _to_mongo_fast(self, use_db_field: bool = True, fields: list[str] | None = None) -> Any:
        """Optimized to_mongo: plain dict instead of SON, cached co_varnames."""
        _data = self._data
        _fields = self._fields
        _is_dynamic = self._dynamic
        _dynamic_fields = self._dynamic_fields if _is_dynamic else None
        _get_sig = BaseDocument._get_to_mongo_sig
        allow_inheritance = self._meta.get("allow_inheritance")

        # Build root_fields filter only when fields is provided
        if fields:
            root_fields = {f.split(".", 1)[0] for f in fields}
        else:
            root_fields = None

        # Use _MongoDict (plain dict + to_dict()) instead of SON.
        # Python 3.7+ dicts preserve insertion order, and PyMongo
        # accepts plain dicts.
        data: dict[str, Any] = _MongoDict(_id=None)

        # Only add _cls when inheritance is enabled
        if allow_inheritance:
            data["_cls"] = self._class_name

        for field_name in self._fields_ordered:
            if root_fields is not None and field_name not in root_fields:
                continue

            value = _data.get(field_name)
            field = _fields.get(field_name)

            if field is None and _is_dynamic:
                field = _dynamic_fields.get(field_name)

            if value is not None:
                accepts_db, accepts_fields = _get_sig(field)
                if fields and accepts_fields:
                    # Build sub-fields for this embedded field
                    key = field_name + "."
                    key_len = len(key)
                    embedded_fields = [f[key_len:] for f in fields if f.startswith(key)]
                    if accepts_db:
                        value = field.to_mongo(value, use_db_field=use_db_field, fields=embedded_fields)
                    else:
                        value = field.to_mongo(value, fields=embedded_fields)
                elif accepts_db:
                    value = field.to_mongo(value, use_db_field=use_db_field)
                else:
                    value = field.to_mongo(value)

            # Handle self-generating fields.
            # Skip if generate() is a coroutine function (async).
            if value is None and field._auto_gen:
                import inspect

                if not inspect.iscoroutinefunction(field.generate):
                    value = field.generate()
                    _data[field_name] = value

            if value is not None or field.null:
                if use_db_field:
                    data[field.db_field] = value
                else:
                    data[field.name] = value

        return data

    # Cached _import_class results.
    _validate_embedded_types: tuple[type, ...] | None = None
    _from_son_embedded_doc_type: type | None = None

    def validate(self, clean: bool = True) -> None:
        """Ensure that all fields' values are valid and that required fields
        are present.

        Raises :class:`ValidationError` if any of the fields' values are found
        to be invalid.
        """
        # Ensure that each field is matched to a valid value
        errors: dict[str, Any] = {}
        if clean:
            try:
                self.clean()
            except ValidationError as error:
                errors[NON_FIELD_ERRORS] = error

        # Cached import: resolve once, reuse across all validate() calls
        embedded_types = BaseDocument._validate_embedded_types
        if embedded_types is None:
            embedded_types = (
                _import_class("EmbeddedDocumentField"),
                _import_class("GenericEmbeddedDocumentField"),
            )
            BaseDocument._validate_embedded_types = embedded_types

        # Inline iteration: avoid intermediate list allocation
        _fields = self._fields
        _dynamic_fields = self._dynamic_fields
        _data = self._data
        for name in self._fields_ordered:
            field = _fields.get(name) or _dynamic_fields.get(name)
            value = _data.get(name)
            if value is not None:
                try:
                    if isinstance(field, embedded_types):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError) as error:
                    errors[field.name] = error
            elif field.required and not getattr(field, "_auto_gen", False):
                errors[field.name] = ValidationError("Field is required", field_name=field.name)

        if errors:
            pk = "None"
            if hasattr(self, "pk"):
                pk = self.pk
            elif self._instance and hasattr(self._instance, "pk"):
                pk = self._instance.pk
            message = f"ValidationError ({self._class_name}:{pk}) "
            raise ValidationError(message, errors=errors)

    def to_json(self, *args: Any, **kwargs: Any) -> str:
        """Convert this document to JSON.

        :param use_db_field: Serialize field names as they appear in
            MongoDB (as opposed to attribute names on this document).
            Defaults to True.
        """
        use_db_field: bool = kwargs.pop("use_db_field", True)
        if "json_options" not in kwargs:
            warnings.warn(
                "No 'json_options' are specified! Falling back to "
                "LEGACY_JSON_OPTIONS with uuid_representation=PYTHON_LEGACY. "
                "For use with other MongoDB drivers specify the UUID "
                "representation to use. This will be changed to "
                "uuid_representation=UNSPECIFIED in a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs["json_options"] = LEGACY_JSON_OPTIONS
        return json_util.dumps(self.to_mongo(use_db_field), *args, **kwargs)

    @classmethod
    def from_json(cls, json_data: str, created: bool = False, **kwargs: Any) -> Self:
        """Converts json data to a Document instance.

        :param str json_data: The json data to load into the Document.
        :param bool created: Boolean defining whether to consider the newly
            instantiated document as brand new or as persisted already:
            * If True, consider the document as brand new, no matter what data
              it's loaded with (i.e., even if an ID is loaded).
            * If False and an ID is NOT provided, consider the document as
              brand new.
            * If False and an ID is provided, assume that the object has
              already been persisted (this has an impact on the subsequent
              call to .save()).
            * Defaults to ``False``.
        """
        # TODO should `created` default to False? If the object already exists
        # in the DB, you would likely retrieve it from MongoDB itself through
        # a query, not load it from JSON data.
        if "json_options" not in kwargs:
            warnings.warn(
                "No 'json_options' are specified! Falling back to "
                "LEGACY_JSON_OPTIONS with uuid_representation=PYTHON_LEGACY. "
                "For use with other MongoDB drivers specify the UUID "
                "representation to use. This will be changed to "
                "uuid_representation=UNSPECIFIED in a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs["json_options"] = LEGACY_JSON_OPTIONS
        return cls._from_son(json_util.loads(json_data, **kwargs), created=created)

    def __expand_dynamic_values(self, name: str, value: Any) -> Any:
        """Expand any dynamic values to their correct types / values."""
        if not isinstance(value, (dict, list, tuple)):
            return value

        # If the value is a dict with '_cls' in it, turn it into a document
        is_dict = isinstance(value, dict)
        if is_dict and "_cls" in value:
            cls = _DocumentRegistry.get(value["_cls"])
            return cls(**value)

        if is_dict:
            value = {k: self.__expand_dynamic_values(k, v) for k, v in value.items()}
        else:
            value = [self.__expand_dynamic_values(name, v) for v in value]

        # Convert lists / values so we can watch for any changes on them
        EmbeddedDocumentListField = _import_class("EmbeddedDocumentListField")
        if isinstance(value, (list, tuple)) and not isinstance(value, BaseList):
            if issubclass(type(self), EmbeddedDocumentListField):
                value = EmbeddedDocumentList(value, self, name)
            else:
                value = BaseList(value, self, name)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, self, name)

        return value

    def _mark_as_changed(self, key: str) -> None:
        """Mark a key as explicitly changed by the user."""
        if not hasattr(self, "_changed_fields"):
            return

        if "." in key:
            key, rest = key.split(".", 1)
            key = self._db_field_map.get(key, key)
            key = f"{key}.{rest}"
        else:
            key = self._db_field_map.get(key, key)

        if key not in self._changed_fields:
            levels, idx = key.split("."), 1
            while idx <= len(levels):
                if ".".join(levels[:idx]) in self._changed_fields:
                    break
                idx += 1
            else:
                self._changed_fields.append(key)
                # remove lower level changed fields
                level = ".".join(levels[:idx]) + "."
                remove = self._changed_fields.remove
                for field in self._changed_fields[:]:
                    if field.startswith(level):
                        remove(field)

    def _clear_changed_fields(self) -> None:
        """Using _get_changed_fields iterate and remove any fields that
        are marked as changed.
        """
        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")

        for changed in self._get_changed_fields():
            parts = changed.split(".")
            data: Any = self
            for part in parts:
                if isinstance(data, list):
                    try:
                        data = data[int(part)]
                    except IndexError:
                        data = None
                elif isinstance(data, dict):
                    data = data.get(part, None)
                else:
                    field_name = data._reverse_db_field_map.get(part, part)
                    data = getattr(data, field_name, None)

                if not isinstance(data, LazyReference) and hasattr(data, "_changed_fields"):
                    if getattr(data, "_is_document", False):
                        continue

                    data._changed_fields = []
                elif isinstance(data, (list, tuple, dict)):
                    if hasattr(data, "field") and isinstance(data.field, (ReferenceField, GenericReferenceField)):
                        continue
                    BaseDocument._nestable_types_clear_changed_fields(data)

        self._changed_fields = []

    @staticmethod
    def _nestable_types_clear_changed_fields(data: list[Any] | tuple[Any, ...] | dict[str, Any]) -> None:
        """Inspect nested data for changed fields

        :param data: data to inspect for changes
        """
        Document = _import_class("Document")

        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, "items"):
            iterator = enumerate(data)
        else:
            iterator = data.items()

        for _index_or_key, value in iterator:
            if hasattr(value, "_get_changed_fields") and not isinstance(value, Document):  # don't follow references
                value._clear_changed_fields()
            elif isinstance(value, (list, tuple, dict)):
                BaseDocument._nestable_types_clear_changed_fields(value)

    @staticmethod
    def _nestable_types_changed_fields(
        changed_fields: list[str],
        base_key: str,
        data: list[Any] | tuple[Any, ...] | dict[str, Any],
    ) -> None:
        """Inspect nested data for changed fields

        :param changed_fields: Previously collected changed fields
        :param base_key: The base key that must be used to prepend changes to this data
        :param data: data to inspect for changes
        """
        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, "items"):
            iterator = enumerate(data)
        else:
            iterator = data.items()

        for index_or_key, value in iterator:
            item_key = f"{base_key}{index_or_key}."
            # don't check anything lower if this key is already marked
            # as changed.
            if item_key[:-1] in changed_fields:
                continue

            if hasattr(value, "_get_changed_fields"):
                changed = value._get_changed_fields()
                changed_fields += [f"{item_key}{k}" for k in changed if k]
            elif isinstance(value, (list, tuple, dict)):
                BaseDocument._nestable_types_changed_fields(changed_fields, item_key, value)

    def _get_changed_fields(self) -> list[str]:
        """Return a list of all fields that have explicitly been changed."""
        EmbeddedDocument = _import_class("EmbeddedDocument")
        LazyReferenceField = _import_class("LazyReferenceField")
        ReferenceField = _import_class("ReferenceField")
        GenericLazyReferenceField = _import_class("GenericLazyReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")
        SortedListField = _import_class("SortedListField")

        changed_fields: list[str] = []
        changed_fields += getattr(self, "_changed_fields", [])

        for field_name in self._fields_ordered:
            db_field_name = self._db_field_map.get(field_name, field_name)
            key = f"{db_field_name}."
            data = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if db_field_name in changed_fields:
                # Whole field already marked as changed, no need to go further
                continue

            if isinstance(field, ReferenceField):  # Don't follow referenced documents
                continue

            if isinstance(data, EmbeddedDocument):
                # Find all embedded fields that have been changed
                changed = data._get_changed_fields()
                changed_fields += [f"{key}{k}" for k in changed if k]
            elif isinstance(data, (list, tuple, dict)):
                if hasattr(field, "field") and isinstance(
                    field.field,
                    (
                        LazyReferenceField,
                        ReferenceField,
                        GenericLazyReferenceField,
                        GenericReferenceField,
                    ),
                ):
                    continue
                elif isinstance(field, SortedListField) and field._ordering:
                    # if ordering is affected whole list is changed
                    if any(field._ordering in d._changed_fields for d in data):
                        changed_fields.append(db_field_name)
                        continue

                self._nestable_types_changed_fields(changed_fields, key, data)
        return changed_fields

    def _delta(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """Returns the delta (set, unset) of the changes for a document.
        Gets any values that have been explicitly changed.
        """
        # Handles cases where not loaded from_son but has _id
        doc = self.to_mongo()

        set_fields = self._get_changed_fields()
        unset_data: dict[str, Any] = {}
        if hasattr(self, "_changed_fields"):
            set_data: dict[str, Any] = {}
            # Fetch each set item from its path
            for path in set_fields:
                parts = path.split(".")
                d: Any = doc
                new_path: list[str] = []
                for p in parts:
                    if isinstance(d, (ObjectId, DBRef)):
                        # Don't dig in the references
                        break
                    elif isinstance(d, list) and p.isdigit():
                        # An item of a list (identified by its index) is updated
                        d = d[int(p)]
                    elif hasattr(d, "get"):
                        # dict-like (dict, embedded document)
                        d = d.get(p)
                    new_path.append(p)
                path = ".".join(new_path)
                set_data[path] = d
        else:
            set_data = doc
            if "_id" in set_data:
                del set_data["_id"]

        # Determine if any changed items were actually unset.
        for path, value in list(set_data.items()):
            if value or isinstance(value, (numbers.Number, bool)):  # Account for 0 and True that are truthy
                continue

            parts = path.split(".")

            if self._dynamic and len(parts) and parts[0] in self._dynamic_fields:
                del set_data[path]
                unset_data[path] = 1
                continue

            # If we've set a value that ain't the default value don't unset it.
            default: Any = None
            if path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                db_field_name = parts.pop()
                for p in parts:
                    if isinstance(d, list) and p.isdigit():
                        d = d[int(p)]
                    elif hasattr(d, "__getattribute__") and not isinstance(d, dict):
                        real_path = d._reverse_db_field_map.get(p, p)
                        d = getattr(d, real_path)
                    else:
                        d = d.get(p)

                if hasattr(d, "_fields"):
                    field_name = d._reverse_db_field_map.get(db_field_name, db_field_name)
                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None

            if default is not None:
                default = default() if callable(default) else default

            if value != default:
                continue

            del set_data[path]
            unset_data[path] = 1
        return set_data, unset_data

    @classmethod
    def _get_collection_name(cls) -> str | None:
        """Return the collection name for this class. None for abstract
        class.
        """
        return cls._meta.get("collection", None)

    @classmethod
    def _from_son(cls, son: dict[str, Any], created: bool = False) -> Self:
        """Create an instance of a Document (subclass) from a PyMongo SON (dict).

        Bypasses ``__init__`` and writes directly to ``_data`` in a single
        pass, using ``_reverse_db_field_map`` for O(1) key translation.
        """
        if son and not isinstance(son, dict):
            raise ValueError(f"The source SON object needs to be of type 'dict' but a '{type(son)}' was found")

        # Get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get("_cls", cls._class_name)

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = _DocumentRegistry.get(class_name)

        # Fall back to __init__ path if pre_init/post_init signals have
        # receivers, since the fast path skips signal dispatch.
        if signals.pre_init.has_receivers_for(cls) or signals.post_init.has_receivers_for(cls):
            return cls._from_son_via_init(son, created)

        obj = cls.__new__(cls)  # type: ignore[arg-type]

        # Initialise slots that __init__ would normally set
        obj._initialised = False
        obj._created = created
        obj._dynamic_fields = SON()
        obj._auto_id_field = cls._meta.get("id_field")
        obj._db_field_map = cls._db_field_map

        fields = cls._fields
        reverse_map = cls._reverse_db_field_map  # {db_field: field_name}

        if cls.STRICT and not cls._dynamic:
            obj._data = StrictDict.create(allowed_keys=cls._fields_ordered)()
        else:
            obj._data = {}

        errors_dict: dict[str, Any] = {}

        # Single pass: translate db_field → field_name and call to_python
        for db_key, value in son.items():
            db_key = str(db_key)
            field_name = reverse_map.get(db_key, db_key)
            field = fields.get(field_name)

            if field is not None:
                if value is not None:
                    try:
                        value = field.to_python(value)
                    except (AttributeError, ValueError) as e:
                        errors_dict[field_name] = e
                        continue
                else:
                    # Replicate BaseField.__set__ null handling:
                    # replace None with default when null=False.
                    if not field.null and field.default is not None:
                        value = field.default
                        if callable(value):
                            value = value()
                obj._data[field_name] = value
            elif field_name in _KNOWN_EXTRA_KEYS:
                # Internal keys (_cls, _text_score) — store silently
                obj._data[field_name] = value
            else:
                # Extra key not in declared fields — store for dynamic /
                # non-strict docs; raise for strict non-dynamic docs.
                if not cls._dynamic and (cls._meta.get("strict", True) or created):
                    msg = f'The fields "{{{field_name}}}" do not exist on the document "{cls._class_name}"'
                    raise FieldDoesNotExist(msg)
                obj._data[field_name] = value

        if errors_dict:
            errors = "\n".join([f"Field '{k}' - {v}" for k, v in errors_dict.items()])
            msg = f"Invalid data to create a `{cls._class_name}` instance.\n{errors}"
            raise InvalidDocumentError(msg)

        # Set defaults for fields missing from son
        for field_name in cls._fields_ordered:
            if field_name not in obj._data:
                field = fields[field_name]
                default = field.default
                if callable(default):
                    default = default()
                obj._data[field_name] = default

        # Wire up _instance on embedded documents so change tracking
        # and nested access work correctly (replicates what the field
        # descriptor __set__ does during normal __init__).
        EmbeddedDocument = cls._from_son_embedded_doc_type
        if EmbeddedDocument is None:
            EmbeddedDocument = _import_class("EmbeddedDocument")
            BaseDocument._from_son_embedded_doc_type = EmbeddedDocument

        proxy = weakref.proxy(obj)

        def _set_instance(val: Any) -> None:
            """Recursively set _instance on embedded docs.

            Must recurse into lists (ListField/EmbeddedDocumentListField)
            and dicts (MapField, DictField) to reach nested embedded docs.
            """
            if isinstance(val, EmbeddedDocument):
                val._instance = proxy
            elif isinstance(val, (list, tuple)):
                for item in val:
                    _set_instance(item)
            elif isinstance(val, dict):
                for item in val.values():
                    _set_instance(item)

        for value in obj._data.values():
            _set_instance(value)

        # Set up get_<field>_display methods for fields with choices
        obj.__set_field_display()

        # For dynamic documents, unlock and use setattr for non-field
        # keys so that DynamicField descriptors are created properly.
        if cls._dynamic:
            obj._dynamic_lock = False  # pyright: ignore[reportGeneralTypeIssues]
            for key in list(obj._data):
                if key not in fields:
                    value = obj._data.pop(key)
                    setattr(obj, key, value)

        obj._changed_fields = []
        obj._initialised = True
        return obj

    @classmethod
    def _from_son_via_init(cls, son: dict[str, Any], created: bool = False) -> Self:
        """Fallback _from_son that goes through __init__.

        Used when pre_init / post_init signal receivers are registered,
        since the fast path skips signal dispatch.
        """
        # Convert SON to a data dict, making sure each key is a string
        # and corresponds to the right db field.
        data: dict[str, Any] = {}
        for key, value in son.items():
            key = str(key)
            key = cls._db_field_map.get(key, key)
            data[key] = value

        errors_dict: dict[str, Any] = {}
        fields = cls._fields

        # Apply field-name / db-field conversion and to_python
        for field_name, field in fields.items():
            if field.db_field in data:
                value = data[field.db_field]
                try:
                    data[field_name] = value if value is None else field.to_python(value)
                    if field_name != field.db_field:
                        del data[field.db_field]
                except (AttributeError, ValueError) as e:
                    errors_dict[field_name] = e

        if errors_dict:
            errors = "\n".join([f"Field '{k}' - {v}" for k, v in errors_dict.items()])
            msg = f"Invalid data to create a `{cls._class_name}` instance.\n{errors}"
            raise InvalidDocumentError(msg)

        # In STRICT documents, remove any keys that aren't in cls._fields
        if cls.STRICT:
            data = {k: v for k, v in data.items() if k in cls._fields}

        obj = cls(__auto_convert=False, _created=created, **data)
        obj._changed_fields = []
        return obj

    @classmethod
    def _build_index_specs(cls, meta_indexes: list[Any]) -> list[dict[str, Any]]:
        """Generate and merge the full index specs."""
        geo_indices = cls._geo_indices()
        unique_indices = cls._unique_with_indexes()
        index_specs = [cls._build_index_spec(spec) for spec in meta_indexes]

        def merge_index_specs(
            index_specs: list[dict[str, Any]],
            indices: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            """Helper method for merging index specs."""
            if not indices:
                return index_specs

            # Create a map of index fields to index spec. We're converting
            # the fields from a list to a tuple so that it's hashable.
            spec_fields = {tuple(index["fields"]): index for index in index_specs}

            # For each new index, if there's an existing index with the same
            # fields list, update the existing spec with all data from the
            # new spec.
            for new_index in indices:
                candidate = spec_fields.get(tuple(new_index["fields"]))
                if candidate is None:
                    index_specs.append(new_index)
                else:
                    candidate.update(new_index)

            return index_specs

        # Merge geo indexes and unique_with indexes into the meta index specs.
        index_specs = merge_index_specs(index_specs, geo_indices)
        index_specs = merge_index_specs(index_specs, unique_indices)
        return index_specs

    @classmethod
    def _build_index_spec(cls, spec: Any) -> dict[str, Any]:
        """Build a PyMongo index spec from a MongoEngine index spec."""
        if isinstance(spec, str):
            spec = {"fields": [spec]}
        elif isinstance(spec, (list, tuple)):
            spec = {"fields": list(spec)}
        elif isinstance(spec, dict):
            spec = dict(spec)

        index_list: list[tuple[str, int | str]] = []
        direction: int | str | None = None  # pyright: ignore[reportRedeclaration]

        # Check to see if we need to include _cls
        allow_inheritance = cls._meta.get("allow_inheritance")
        include_cls = (
            allow_inheritance
            and not spec.get("sparse", False)
            and spec.get("cls", True)
            and "_cls" not in spec["fields"]
        )

        # 733: don't include cls if index_cls is False unless there is an explicit cls with the index
        include_cls = include_cls and (spec.get("cls", False) or cls._meta.get("index_cls", True))
        if "cls" in spec:
            spec.pop("cls")
        for key in spec["fields"]:
            # If inherited spec continue
            if isinstance(key, (list, tuple)):
                continue

            # ASCENDING from +
            # DESCENDING from -
            # TEXT from $
            # HASHED from #
            # GEOSPHERE from (
            # GEOHAYSTACK from )
            # GEO2D from *
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            elif key.startswith("$"):
                direction = pymongo.TEXT
            elif key.startswith("#"):
                direction = pymongo.HASHED
            elif key.startswith("("):
                direction = pymongo.GEOSPHERE
            elif key.startswith(")"):
                try:
                    direction = pymongo.GEOHAYSTACK
                except AttributeError:
                    raise NotImplementedError
            elif key.startswith("*"):
                direction = pymongo.GEO2D
            if key.startswith(("+", "-", "*", "$", "#", "(", ")")):
                key = key[1:]

            # Use real field name, do it manually because we need field
            # objects for the next part (list field checking)
            parts = key.split(".")
            if parts in (["pk"], ["id"], ["_id"]):
                key = "_id"
            else:
                fields = cls._lookup_field(parts)
                parts = []
                for field in fields:
                    try:
                        if field != "_id":
                            field = field.db_field
                    except AttributeError:
                        pass
                    parts.append(field)
                key = ".".join(parts)
            index_list.append((key, direction))  # type: ignore[arg-type]

        # Don't add cls to a geo index
        if (
            include_cls
            and direction not in (pymongo.GEO2D, pymongo.GEOSPHERE)
            and (GEOHAYSTACK is None or direction != GEOHAYSTACK)
        ):
            index_list.insert(0, ("_cls", 1))

        if index_list:
            spec["fields"] = index_list

        return spec

    @classmethod
    def _unique_with_indexes(cls, namespace: str = "") -> list[dict[str, Any]]:
        """Find unique indexes in the document schema and return them."""
        unique_indexes: list[dict[str, Any]] = []
        for field_name, field in cls._fields.items():
            sparse = field.sparse

            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, str):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with: list[str] = []
                    for other_name in field.unique_with:
                        parts = other_name.split(".")

                        # Lookup real name
                        parts = cls._lookup_field(parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append(".".join(name_parts))

                        # Unique field should be required
                        parts[-1].required = True
                        sparse = not sparse and parts[-1].name not in cls.__dict__

                    unique_fields += unique_with

                # Add the new index to the list
                fields = [(f"{namespace}{f}", pymongo.ASCENDING) for f in unique_fields]
                index = {"fields": fields, "unique": True, "sparse": sparse}
                unique_indexes.append(index)

            if field.__class__.__name__ in {
                "EmbeddedDocumentListField",
                "ListField",
                "SortedListField",
            }:
                field = field.field

            # Grab any embedded document field unique indexes
            if field.__class__.__name__ == "EmbeddedDocumentField" and field.document_type != cls:
                field_namespace = f"{field_name}."
                doc_cls = field.document_type
                unique_indexes += doc_cls._unique_with_indexes(field_namespace)

        return unique_indexes

    @classmethod
    def _geo_indices(
        cls,
        inspected: list[Any] | None = None,
        parent_field: str | None = None,
    ) -> list[dict[str, Any]]:
        inspected = inspected or []
        geo_indices: list[dict[str, Any]] = []
        inspected.append(cls)

        geo_field_type_names = (
            "EmbeddedDocumentField",
            "GeoPointField",
            "PointField",
            "LineStringField",
            "PolygonField",
        )

        geo_field_types = tuple(_import_class(field) for field in geo_field_type_names)

        for field in cls._fields.values():
            if not isinstance(field, geo_field_types):
                continue

            if hasattr(field, "document_type"):
                field_cls = field.document_type
                if field_cls in inspected:
                    continue

                if hasattr(field_cls, "_geo_indices"):
                    geo_indices += field_cls._geo_indices(inspected, parent_field=field.db_field)
            elif field._geo_index:
                field_name = field.db_field
                if parent_field:
                    field_name = f"{parent_field}.{field_name}"
                geo_indices.append({"fields": [(field_name, field._geo_index)]})

        return geo_indices

    @classmethod
    def _lookup_field(cls, parts: list[str] | tuple[str, ...] | str) -> list[Any]:
        """Given the path to a given field, return a list containing
        the Field object associated with that field and all of its parent
        Field objects.

        Args:
            parts (str, list, or tuple) - path to the field. Should be a
            string for simple fields existing on this document or a list
            of strings for a field that exists deeper in embedded documents.

        Returns:
            A list of Field instances for fields that were found or
            strings for sub-fields that weren't.

        Example:
            >>> user._lookup_field('name')
            [<mongoengine.fields.StringField at 0x1119bff50>]

            >>> user._lookup_field('roles')
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>]

            >>> user._lookup_field(['roles', 'role'])
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>,
             <mongoengine.fields.StringField at 0x1119ec050>]

            >>> user._lookup_field('doesnt_exist')
            raises LookUpError

            >>> user._lookup_field(['roles', 'doesnt_exist'])
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>,
             'doesnt_exist']

        """
        # TODO this method is WAY too complicated. Simplify it.
        # TODO don't think returning a string for embedded non-existent fields is desired

        ListField = _import_class("ListField")
        DynamicField = _import_class("DynamicField")

        if not isinstance(parts, (list, tuple)):
            parts = [parts]

        fields: list[Any] = []
        field: Any = None

        for field_name in parts:
            # Handle ListField indexing:
            if field_name.isdigit() and isinstance(field, ListField):
                fields.append(field_name)
                continue

            # Look up first field from the document
            if field is None:
                if field_name == "pk":
                    # Deal with "primary key" alias
                    field_name = cls._meta["id_field"]

                if field_name in cls._fields:
                    field = cls._fields[field_name]
                elif cls._dynamic:
                    field = DynamicField(db_field=field_name)
                elif cls._meta.get("allow_inheritance") or cls._meta.get("abstract", False):
                    # 744: in case the field is defined in a subclass
                    for subcls in cls.__subclasses__():
                        try:
                            field = subcls._lookup_field([field_name])[0]
                        except LookUpError:
                            continue

                        if field is not None:
                            break
                    else:
                        raise LookUpError(f'Cannot resolve field "{field_name}"')
                else:
                    raise LookUpError(f'Cannot resolve field "{field_name}"')
            else:
                ReferenceField = _import_class("ReferenceField")
                GenericReferenceField = _import_class("GenericReferenceField")

                # If previous field was a reference, throw an error (we
                # cannot look up fields that are on references).
                if isinstance(field, (ReferenceField, GenericReferenceField)):
                    raise LookUpError("Cannot perform join in mongoDB: {}".format("__".join(parts)))

                # If the parent field has a "field" attribute which has a
                # lookup_member method, call it to find the field
                # corresponding to this iteration.
                if hasattr(getattr(field, "field", None), "lookup_member"):
                    new_field = field.field.lookup_member(field_name)

                # If the parent field is a DynamicField or if it's part of
                # a DynamicDocument, mark current field as a DynamicField
                # with db_name equal to the field name.
                elif cls._dynamic and (
                    isinstance(field, DynamicField) or getattr(getattr(field, "document_type", None), "_dynamic", None)
                ):
                    new_field = DynamicField(db_field=field_name)

                # Else, try to use the parent field's lookup_member method
                # to find the subfield.
                elif hasattr(field, "lookup_member"):
                    new_field = field.lookup_member(field_name)

                # Raise a LookUpError if all the other conditions failed.
                else:
                    raise LookUpError(f"Cannot resolve subfield or operator {field_name} on the field {field.name}")

                # If current field still wasn't found and the parent field
                # is a ComplexBaseField, add the name current field name and
                # move on.
                if not new_field and isinstance(field, ComplexBaseField):
                    fields.append(field_name)
                    continue
                elif not new_field:
                    raise LookUpError(f'Cannot resolve field "{field_name}"')

                field = new_field  # update field to the new field type

            fields.append(field)

        return fields

    @classmethod
    def _translate_field_name(cls, field: str, sep: str = ".") -> str:
        """Translate a field attribute name to a database field name."""
        parts = field.split(sep)
        parts = [f.db_field for f in cls._lookup_field(parts)]
        return ".".join(parts)

    def __set_field_display(self) -> None:
        """For each field that specifies choices, create a
        get_<field>_display method.
        """
        fields_with_choices = [(n, f) for n, f in self._fields.items() if f.choices]
        for attr_name, field in fields_with_choices:
            setattr(
                self,
                f"get_{attr_name}_display",
                partial(self.__get_field_display, field=field),
            )

    def __get_field_display(self, field: Any) -> Any:
        """Return the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            if value is None:
                return None
            sep = getattr(field, "display_sep", " ")
            values = value if field.__class__.__name__ in ("ListField", "SortedListField") else [value]
            return sep.join([str(dict(field.choices).get(val, val)) for val in values or []])
        return value
