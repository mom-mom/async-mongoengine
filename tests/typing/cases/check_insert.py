"""Consumer type regression cases: ``QuerySet.insert()`` result shapes.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``: the result
follows the input shape (one document or a sequence) and ``load_bulk``
(documents by default, primary keys with ``load_bulk=False``); the key type
follows the model's ``Document[PK]`` parameter.
"""

from collections.abc import Sequence
from typing import assert_type

from bson import ObjectId

from mongoengine import Document, QuerySet, StringField


class Item(Document):
    name = StringField()

    meta = {"allow_inheritance": True}


class Sub(Item):
    extra = StringField()


class Coded(Document[str]):
    code = StringField(primary_key=True)


async def single_document(query: QuerySet[Item], item: Item, flag: bool) -> None:
    assert_type(await query.insert(item), Item)
    assert_type(await query.insert(item, load_bulk=True), Item)
    assert_type(await query.insert(item, True), Item)
    assert_type(await query.insert(item, True, {"w": 1}), Item)
    assert_type(await query.insert(item, write_concern={"w": 1}, signal_kwargs={"k": 1}), Item)
    assert_type(await query.insert(item, load_bulk=False), ObjectId)
    assert_type(await query.insert(item, False), ObjectId)
    assert_type(await query.insert(item, flag), Item | ObjectId)
    assert_type(await query.insert(item, load_bulk=flag), Item | ObjectId)

    inserted = await query.insert(item)
    assert_type(inserted.id, ObjectId | None)
    assert_type(inserted.name, str | None)
    inserted_id = await query.insert(item, load_bulk=False)
    assert_type(inserted_id.generation_time.year, int)


async def document_sequences(query: QuerySet[Item], item: Item, items: Sequence[Item], flag: bool) -> None:
    assert_type(await query.insert([item]), list[Item])
    assert_type(await query.insert([item], load_bulk=True), list[Item])
    assert_type(await query.insert((item, item)), list[Item])
    assert_type(await query.insert(items), list[Item])
    assert_type(await query.insert([item], load_bulk=False), list[ObjectId])
    assert_type(await query.insert(items, False), list[ObjectId])
    assert_type(await query.insert([item], flag), list[Item] | list[ObjectId])
    assert_type(await query.insert(items, load_bulk=flag), list[Item] | list[ObjectId])
    for doc in await query.insert([item]):
        assert_type(doc, Item)
    for oid in await query.insert([item], load_bulk=False):
        assert_type(oid, ObjectId)


async def through_the_manager(item: Item, sub: Sub) -> None:
    assert_type(await Item.objects.insert(item), Item)
    assert_type(await Item.objects.insert([item, sub]), list[Item])
    assert_type(await Item.objects.insert(item, load_bulk=False), ObjectId)
    assert_type(await Sub.objects.insert(sub), Sub)
    assert_type(await Sub.objects.insert([sub], load_bulk=False), list[ObjectId])


async def custom_primary_key(coded: Coded, flag: bool) -> None:
    assert_type(await Coded.objects.insert(coded), Coded)
    assert_type(await Coded.objects.insert([coded]), list[Coded])
    assert_type(await Coded.objects.insert(coded, load_bulk=False), str)
    assert_type(await Coded.objects.insert([coded], load_bulk=False), list[str])
    assert_type(await Coded.objects.insert(coded, load_bulk=flag), Coded | str)
    assert_type(await Coded.objects.insert([coded], load_bulk=flag), list[Coded] | list[str])


async def shape_mismatches(query: QuerySet[Item], item: Item, coded: Coded) -> None:
    single: Item = await query.insert([item])  # expect-error: reportAssignmentType
    _ = single
    batch: list[Item] = await query.insert(item)  # expect-error: reportAssignmentType
    _ = batch
    document: Item = await query.insert(item, load_bulk=False)  # expect-error: reportAssignmentType
    _ = document
    ids: list[ObjectId] = await query.insert([item])  # expect-error: reportAssignmentType
    _ = ids
    object_id: ObjectId = await Coded.objects.insert(coded, load_bulk=False)  # expect-error: reportAssignmentType
    _ = object_id
    inserted_id = await query.insert(item, load_bulk=False)
    inserted_id.name  # expect-error: reportAttributeAccessIssue "name"
    inserted = await query.insert(item)
    inserted.generation_time  # expect-error: reportAttributeAccessIssue "generation_time"


async def wrong_model(query: QuerySet[Item], coded: Coded) -> None:
    # Pyright reports both the failed overload resolution and the argument type.
    await query.insert(coded)  # expect-error: reportCallIssue  # expect-error: reportArgumentType
    await query.insert([coded])  # expect-error: reportCallIssue  # expect-error: reportArgumentType
