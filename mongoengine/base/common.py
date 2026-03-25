from mongoengine.errors import NotRegistered

__all__ = ("UPDATE_OPERATORS", "_DocumentRegistry")


UPDATE_OPERATORS = {
    "set",
    "unset",
    "inc",
    "dec",
    "mul",
    "pop",
    "push",
    "push_all",
    "pull",
    "pull_all",
    "add_to_set",
    "set_on_insert",
    "min",
    "max",
    "rename",
}


# Primary index: module-qualified key -> DocCls (no collisions)
_document_registry = {}
# Secondary index: _class_name -> DocCls (last-write-wins, for DB _cls compat)
_class_name_registry = {}


def _registry_key(DocCls):
    """Build a module-qualified registry key, e.g. 'myapp.models.User'."""
    return f"{DocCls.__module__}.{DocCls._class_name}"


class _DocumentRegistry:
    """Wrapper for the document registry (providing a singleton pattern).
    This is part of MongoEngine's internals, not meant to be used directly by end-users
    """

    @staticmethod
    def get(name):
        # 1. Exact match on primary (module-qualified) registry
        doc = _document_registry.get(name, None)
        if doc:
            return doc

        # 2. Exact match on secondary (_class_name) registry
        doc = _class_name_registry.get(name, None)
        if doc:
            return doc

        # 3. Suffix fallback for old-style names (e.g. "Area" -> "Location.Area")
        single_end = name.split(".")[-1]
        compound_end = f".{single_end}"
        possible_match = [k for k in _class_name_registry if k.endswith(compound_end) or k == single_end]
        if len(possible_match) == 1:
            return _class_name_registry[possible_match[0]]

        if not possible_match:
            raise NotRegistered(
                """
                `%s` has not been registered in the document registry.
                Importing the document class automatically registers it, has it
                been imported?
            """.strip()
                % name
            )

        # Multiple matches: return the last registered one (preserves
        # the old overwrite-on-collision behaviour).
        return _class_name_registry[possible_match[-1]]

    @staticmethod
    def register(DocCls):
        _document_registry[_registry_key(DocCls)] = DocCls
        _class_name_registry[DocCls._class_name] = DocCls

    @staticmethod
    def unregister(doc_cls_name):
        """Unregister by _class_name or module-qualified key."""
        # Remove from secondary index
        _class_name_registry.pop(doc_cls_name, None)
        # Remove from primary index
        if doc_cls_name in _document_registry:
            _document_registry.pop(doc_cls_name)
        else:
            # Find by _class_name suffix in primary registry
            to_remove = [k for k in _document_registry if k.endswith(f".{doc_cls_name}")]
            for key in to_remove:
                _document_registry.pop(key)


def _get_documents_by_db(connection_alias, default_connection_alias):
    """Get all registered Documents class attached to a given database"""

    def get_doc_alias(doc_cls):
        return doc_cls._meta.get("db_alias", default_connection_alias)

    return [doc_cls for doc_cls in _document_registry.values() if get_doc_alias(doc_cls) == connection_alias]
