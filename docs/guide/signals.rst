.. _signals:

=======
Signals
=======

.. note::

  Signal support is provided by the `blinker`_ library. If you wish
  to enable signal support this library must be installed, though it is not
  required for async-mongoengine to function.

Overview
--------

Signals are found within the ``mongoengine.signals`` module.  Unless
specified, signals receive no additional arguments beyond the ``sender`` class and
``document`` instance.  Post-signals are only called if there were no exceptions
raised during the processing of their related function.

async-mongoengine provides two types of signals for each event:

- **Sync signals** — for synchronous handlers, emitted via ``.send()``
- **Async signals** — for asynchronous handlers, emitted via ``await .send_async()``

Both sync and async signals are emitted at each event point (except
``pre_init`` / ``post_init`` which are sync-only).

Available Signals
-----------------

Sync-only signals (emitted from ``__init__``, no async variant):

``pre_init``
  Called during the creation of a new :class:`~mongoengine.Document` or
  :class:`~mongoengine.EmbeddedDocument` instance, after the constructor
  arguments have been collected but before any additional processing has been
  done to them.  Handlers are passed the dictionary of arguments using the
  ``values`` keyword argument and may modify this dictionary prior to returning.

``post_init``
  Called after all processing of a new :class:`~mongoengine.Document` or
  :class:`~mongoengine.EmbeddedDocument` instance has been completed.

Dual signals (both sync and async variants are emitted):

``pre_save`` / ``pre_save_async``
  Called within :meth:`~mongoengine.Document.save` prior to performing
  any actions.

``pre_save_post_validation`` / ``pre_save_post_validation_async``
  Called within :meth:`~mongoengine.Document.save` after validation
  has taken place but before saving.

``post_save`` / ``post_save_async``
  Called within :meth:`~mongoengine.Document.save` after most actions
  (validation, insert/update, and cascades, but not clearing dirty flags) have
  completed successfully.  Passed the additional boolean keyword argument
  ``created`` to indicate if the save was an insert or an update.

``pre_delete`` / ``pre_delete_async``
  Called within :meth:`~mongoengine.Document.delete` prior to
  attempting the delete operation.

``post_delete`` / ``post_delete_async``
  Called within :meth:`~mongoengine.Document.delete` upon successful
  deletion of the record.

``pre_bulk_insert`` / ``pre_bulk_insert_async``
  Called after validation of the documents to insert, but prior to any data
  being written. The ``document`` argument is replaced by a
  ``documents`` argument representing the list of documents being inserted.

``post_bulk_insert`` / ``post_bulk_insert_async``
  Called after a successful bulk insert operation.  As per ``pre_bulk_insert``,
  the ``document`` argument is omitted and replaced with a ``documents`` argument.
  An additional boolean argument, ``loaded``, identifies the contents of
  ``documents`` as either :class:`~mongoengine.Document` instances when ``True`` or
  simply a list of primary key values for the inserted records if ``False``.

Attaching Events
----------------

Sync handlers
~~~~~~~~~~~~~

Use the sync signal for handlers that do not need to ``await``::

    import logging
    from mongoengine import signals

    class Author(Document):
        name = StringField()

        @classmethod
        def pre_save(cls, sender, document, **kwargs):
            logging.debug("Pre Save: %s" % document.name)

        @classmethod
        def post_save(cls, sender, document, **kwargs):
            logging.debug("Post Save: %s" % document.name)
            if kwargs.get('created'):
                logging.debug("Created")
            else:
                logging.debug("Updated")

    signals.pre_save.connect(Author.pre_save, sender=Author)
    signals.post_save.connect(Author.post_save, sender=Author)

Async handlers
~~~~~~~~~~~~~~

Use the async signal (``*_async``) for handlers that need to perform
database operations or other async work::

    from mongoengine import signals

    class BlogPost(Document):
        content = StringField()
        author = ReferenceField(Person, reverse_delete_rule=CASCADE)
        editor = ReferenceField(Editor)

        @classmethod
        async def pre_delete(cls, sender, document, **kwargs):
            editor = await Editor.objects.get(pk=document.editor.id)
            await editor.update(dec__review_queue=1)

    signals.pre_delete_async.connect(BlogPost.pre_delete, sender=BlogPost)

.. warning::

    Note that EmbeddedDocument only supports ``pre_init`` / ``post_init`` signals.
    ``pre_save``, ``post_save``, etc. should be attached to Document's class only.

.. warning::

    ``pre_init`` and ``post_init`` are **sync-only** because they are emitted
    from ``Document.__init__()``, which cannot be async. Do not register async
    handlers for these signals.


.. _blinker: http://pypi.python.org/pypi/blinker
