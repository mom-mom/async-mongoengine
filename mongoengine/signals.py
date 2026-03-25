from typing import Any, NoReturn

__all__ = (
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
)

signals_available = False
try:
    from blinker import Namespace as Namespace  # pyright: ignore[reportAssignmentType]  # noqa: PLC0414

    signals_available = True
except ImportError:

    class _FakeSignal:
        """If blinker is unavailable, create a fake class with the same
        interface that allows sending of signals but will fail with an
        error on anything else.  Instead of doing anything on send, it
        will just ignore the arguments and do nothing instead.
        """

        def __init__(self, name: str, doc: str | None = None) -> None:
            self.name = name
            self.__doc__ = doc

        def _fail(self, *args: Any, **kwargs: Any) -> NoReturn:
            raise RuntimeError("signalling support is unavailable because the blinker library is not installed.")

        send = lambda *a, **kw: None  # noqa

        async def send_async(self, *a: Any, **kw: Any) -> None:
            return None

        connect = disconnect = has_receivers_for = receivers_for = temporarily_connected_to = _fail
        del _fail

    class Namespace:  # type: ignore[no-redef]
        def signal(self, name: str, doc: str | None = None) -> _FakeSignal:
            return _FakeSignal(name, doc)


_signals = Namespace()

# ---------------------------------------------------------------------------
# Sync signals — use .send(), register sync handlers with .connect()
#
# pre_init / post_init are SYNC-ONLY because they are emitted from
# Document.__init__(), which is a regular (non-async) method.
# There are no async variants for these two signals.
# ---------------------------------------------------------------------------
pre_init = _signals.signal("pre_init")
post_init = _signals.signal("post_init")

# The following sync signals are emitted alongside their async counterparts
# in async contexts (save, delete, insert). Use these for sync handlers.
pre_save = _signals.signal("pre_save")
pre_save_post_validation = _signals.signal("pre_save_post_validation")
post_save = _signals.signal("post_save")
pre_delete = _signals.signal("pre_delete")
post_delete = _signals.signal("post_delete")
pre_bulk_insert = _signals.signal("pre_bulk_insert")
post_bulk_insert = _signals.signal("post_bulk_insert")

# ---------------------------------------------------------------------------
# Async signals — use await .send_async(), register async handlers with .connect()
#
# These are emitted right after the corresponding sync signal in async
# contexts (Document.save, Document.delete, QuerySet.insert).
# Register async handlers here when you need to await DB operations
# inside a signal handler.
#
# Example:
#     async def my_handler(sender, document, **kwargs):
#         await document.reload()
#
#     signals.post_save_async.connect(my_handler, sender=MyDoc)
# ---------------------------------------------------------------------------
pre_save_async = _signals.signal("pre_save_async")
pre_save_post_validation_async = _signals.signal("pre_save_post_validation_async")
post_save_async = _signals.signal("post_save_async")
pre_delete_async = _signals.signal("pre_delete_async")
post_delete_async = _signals.signal("post_delete_async")
pre_bulk_insert_async = _signals.signal("pre_bulk_insert_async")
post_bulk_insert_async = _signals.signal("post_bulk_insert_async")
