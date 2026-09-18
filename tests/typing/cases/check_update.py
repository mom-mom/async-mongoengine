"""Consumer type regression cases: ``update()`` / ``update_one()`` / ``Document.update()``.

Read by ``tests/typing/test_typing.py`` (Pyright, basic mode); never imported
or executed by pytest.  Every check lives in an ``async def`` that is never
awaited, so no coroutine runs and no connection is made.  Positive
expectations use ``typing.assert_type``; negative expectations use
``# expect-error: <rule> ["message substring"]`` markers.

The contract under test is documented in ``docs/typing.md``:
``full_result=True`` yields a ``pymongo.results.UpdateResult``, the default /
``full_result=False`` form yields the matched count as ``int`` and a runtime
``bool`` flag yields the union.
"""

from typing import Any, assert_type

from pymongo.results import UpdateResult

from mongoengine import Document, IntField, QuerySet, StringField


class Item(Document):
    name = StringField(required=True)
    count = IntField()


class Coded(Document[str]):
    code = StringField(primary_key=True)
    label = StringField()


async def queryset_update(query: QuerySet[Item], flag: bool) -> None:
    assert_type(await query.update(set__name="u"), int)
    assert_type(await query.update(full_result=False, set__name="u"), int)
    assert_type(await query.update(True, False, set__name="u"), int)
    assert_type(await query.update(upsert=True, multi=False, set__name="u"), int)
    assert_type(await query.update(inc__count=1, write_concern={"w": 1}), int)

    result = await query.update(full_result=True, set__name="u")
    assert_type(result, UpdateResult)
    assert_type(result.matched_count, int)
    assert_type(result.modified_count, int)
    assert_type(result.did_upsert, bool)
    assert_type(result.acknowledged, bool)
    _ = result.upserted_id
    assert_type(await query.update(upsert=True, full_result=True, set__name="u"), UpdateResult)
    assert_type(await query.update(True, False, None, None, full_result=True, set__name="u"), UpdateResult)

    assert_type(await query.update(full_result=flag, set__name="u"), int | UpdateResult)

    # The result form is independent of chaining and of no-match querysets.
    assert_type(await query.filter(name="x").limit(1).update(full_result=True, inc__count=1), UpdateResult)
    assert_type(await query.none().update(full_result=True, set__name="u"), UpdateResult)
    assert_type(await query[5:5].update(full_result=True, set__name="u"), UpdateResult)
    assert_type(await query.none().update(set__name="u"), int)
    assert_type(await Item.objects(name="x").update(set__name="u"), int)


async def queryset_update_one(query: QuerySet[Item], flag: bool) -> None:
    assert_type(await query.update_one(set__name="u"), int)
    assert_type(await query.update_one(full_result=False, set__name="u"), int)
    assert_type(await query.update_one(True, set__name="u"), int)
    assert_type(await query.update_one(full_result=True, set__name="u"), UpdateResult)
    assert_type(await query.update_one(True, {"w": 1}, full_result=True, set__name="u"), UpdateResult)
    assert_type(await query.update_one(full_result=flag, set__name="u"), int | UpdateResult)
    assert_type(await Item.objects(name="x").update_one(full_result=True, set__name="u"), UpdateResult)


async def document_update(item: Item, coded: Coded, flag: bool) -> None:
    assert_type(await item.update(set__name="u"), int)
    assert_type(await item.update(full_result=False, set__name="u"), int)
    assert_type(await item.update(full_result=True, set__name="u"), UpdateResult)
    assert_type(await item.update(upsert=True, full_result=True, set__name="u"), UpdateResult)
    assert_type(await item.update(full_result=flag, set__name="u"), int | UpdateResult)
    assert_type(await coded.update(set__label="u"), int)
    assert_type(await coded.update(full_result=True, set__label="u"), UpdateResult)


async def result_narrowing(query: QuerySet[Item], flag: bool) -> None:
    dynamic = await query.update(full_result=flag, set__name="u")
    if isinstance(dynamic, UpdateResult):
        assert_type(dynamic.matched_count, int)
    else:
        assert_type(dynamic, int)


async def count_form_has_no_result_attributes(query: QuerySet[Item], item: Item) -> None:
    count = await query.update(set__name="u")
    count.matched_count  # expect-error: reportAttributeAccessIssue "matched_count"
    one = await query.update_one(set__name="u")
    one.upserted_id  # expect-error: reportAttributeAccessIssue "upserted_id"
    doc_count = await item.update(set__name="u")
    doc_count.modified_count  # expect-error: reportAttributeAccessIssue "modified_count"


async def form_mismatches(query: QuerySet[Item], item: Item, flag: bool) -> None:
    wrong_result: UpdateResult = await query.update(set__name="u")  # expect-error: reportAssignmentType
    _ = wrong_result
    wrong_count: int = await query.update(full_result=True, set__name="u")  # expect-error: reportAssignmentType
    _ = wrong_count
    wrong_one: int = await query.update_one(full_result=True, set__name="u")  # expect-error: reportAssignmentType
    _ = wrong_one
    wrong_doc: UpdateResult = await item.update(set__name="u")  # expect-error: reportAssignmentType
    _ = wrong_doc
    # A runtime bool flag must be narrowed before using result attributes.
    dynamic = await query.update(full_result=flag, set__name="u")
    dynamic.matched_count  # expect-error: reportAttributeAccessIssue "matched_count"
    untyped: Any = dynamic
    _ = untyped
