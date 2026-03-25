====================================
async-mongoengine User Documentation
====================================

**async-mongoengine** is an async Object-Document Mapper, written in Python for
working with MongoDB. It is built on PyMongo's native async support
(``AsyncMongoClient``) and provides ``async``/``await`` APIs for all database
operations. To install it, simply run

.. code-block:: console

    $ python -m pip install -U async-mongoengine

:doc:`tutorial`
  A quick tutorial building a tumblelog to get you up and running with
  async-mongoengine.

:doc:`guide/index`
  The Full guide to async-mongoengine --- from modeling documents to storing
  files, from querying for data to firing signals and *everything* between.

:doc:`apireference`
  The complete API documentation --- the innards of documents, querysets and fields.

:doc:`faq`
  Frequently Asked Questions

MongoDB and driver support
--------------------------

async-mongoengine uses PyMongo's built-in async support (``AsyncMongoClient``,
available in PyMongo 4.0+) and does **not** depend on Motor. It is tested
against multiple versions of MongoDB. For further details, please refer to the
`readme <https://github.com/MongoEngine/mongoengine>`_.

Community
---------

To get help with using async-mongoengine, use the `MongoEngine Users mailing list
<http://groups.google.com/group/mongoengine-users>`_ or the ever popular
`stackoverflow <http://www.stackoverflow.com>`_.

Contributing
------------

**Yes please!**  We are always looking for contributions, additions and improvements.

The source is available on `GitHub <https://github.com/MongoEngine/mongoengine>`_
and contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation, the website or the core.

To contribute, fork the project on
`GitHub <https://github.com/MongoEngine/mongoengine>`_ and send a
pull request.

Changes
-------

See the :doc:`changelog` for a full list of changes to async-mongoengine.

Offline Reading
---------------

Download the docs in `pdf <https://media.readthedocs.org/pdf/mongoengine-odm/latest/mongoengine-odm.pdf>`_
or `epub <https://media.readthedocs.org/epub/mongoengine-odm/latest/mongoengine-odm.epub>`_
formats for offline reading.


.. toctree::
    :maxdepth: 1
    :numbered:
    :hidden:

    tutorial
    guide/index
    apireference
    changelog
    faq

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
