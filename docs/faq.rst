==========================
Frequently Asked Questions
==========================

Is PyMongo or Motor used under the hood?
-----------------------------------------

async-mongoengine uses **PyMongo's built-in async support** (``AsyncMongoClient``),
available in PyMongo 4.0 and later. It does **not** use Motor. PyMongo natively
supports ``async``/``await`` through ``AsyncMongoClient``, so there is no need
for a separate async driver. All database operations --- saves, queries,
deletes, updates, and more --- are ``async def`` methods that you call with
``await``.
