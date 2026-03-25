==================
async-mongoengine
==================

.. warning::

   This project is a fork of `MongoEngine <https://github.com/MongoEngine/mongoengine>`_
   with native PyMongo async support (``AsyncMongoClient``). It does **not** use Motor.

   **Important limitations:**

   - Requires **Python 3.13+** and **MongoDB 7.0+**
   - ``FileField`` and ``ImageField`` (GridFS) are **not supported**
   - Auto-dereference for ``ReferenceField`` is **not supported**

   This project was written with the assistance of Claude (Anthropic). While it is
   publicly available, it was primarily built for internal use. **No guarantees are
   made regarding stability, backward compatibility, or long-term maintenance.**

:Info: async-mongoengine is an async ORM-like layer on top of PyMongo.
:Based on: `MongoEngine <https://github.com/MongoEngine/mongoengine>`_ by Harry Marr and Bastien Gerard

About
=====
async-mongoengine is a Python Object-Document Mapper for working with MongoDB
using native async/await. It is based on MongoEngine and uses PyMongo's built-in
``AsyncMongoClient`` — no Motor dependency required.

Supported Versions
==================
- **Python**: 3.13+
- **MongoDB**: 7.0+
- **PyMongo**: 4.10+

Installation
============
We recommend the use of `virtualenv <https://virtualenv.pypa.io/>`_ and of
`pip <https://pip.pypa.io/>`_.

.. code-block:: shell

    python -m pip install -U async-mongoengine

Dependencies
============
- pymongo>=4.10

Optional:

- dateutil>=2.1.0 (for flexible ``DateTimeField`` parsing)
- blinker>=1.3 (for signals support)

Examples
========
Some simple examples of what async-mongoengine code looks like:

.. code :: python

    import asyncio
    import datetime
    from mongoengine import *

    connect('mydb')

    class BlogPost(Document):
        title = StringField(required=True, max_length=200)
        posted = DateTimeField(default=lambda: datetime.datetime.now(datetime.timezone.utc))
        tags = ListField(StringField(max_length=50))
        meta = {'allow_inheritance': True}

    class TextPost(BlogPost):
        content = StringField(required=True)

    class LinkPost(BlogPost):
        url = StringField(required=True)

    async def main():
        # Create a text-based post
        post1 = TextPost(title='Using async-mongoengine', content='See the tutorial')
        post1.tags = ['mongodb', 'mongoengine']
        await post1.save()

        # Create a link-based post
        post2 = LinkPost(title='async-mongoengine Docs', url='https://github.com/mom-mom/async-mongoengine')
        post2.tags = ['mongoengine', 'documentation']
        await post2.save()

        # Iterate over all posts using the BlogPost superclass
        async for post in BlogPost.objects:
            print('===', post.title, '===')
            if isinstance(post, TextPost):
                print(post.content)
            elif isinstance(post, LinkPost):
                print('Link:', post.url)

        # Count all blog posts and its subtypes
        print(await BlogPost.objects.count())   # 2
        print(await TextPost.objects.count())   # 1
        print(await LinkPost.objects.count())   # 1

        # Count tagged posts
        print(await BlogPost.objects(tags='mongoengine').count())  # 2
        print(await BlogPost.objects(tags='mongodb').count())      # 1

    asyncio.run(main())

Tests
=====
To run the test suite, ensure you are running a local instance of MongoDB on
the standard port and have ``pytest`` and ``pytest-asyncio`` installed. Then:

.. code-block:: shell

    pytest tests/

Credits
=======
This project is based on `MongoEngine <https://github.com/MongoEngine/mongoengine>`_,
originally created by Harry Marr and maintained by Bastien Gerard.
