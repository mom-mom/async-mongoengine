from enum import Enum

import pytest
from bson import InvalidDocument

from mongoengine import (
    DictField,
    Document,
    EnumField,
    ListField,
    ValidationError,
)
from tests.utils import MongoDBTestCase, get_as_pymongo


class Status(Enum):
    NEW = "new"
    DONE = "done"


class Color(Enum):
    RED = 1
    BLUE = 2


class ModelWithEnum(Document):
    status = EnumField(Status)


class ModelComplexEnum(Document):
    status = EnumField(Status)
    statuses = ListField(EnumField(Status))
    color_mapping = DictField(EnumField(Color))


class TestStringEnumField(MongoDBTestCase):
    async def test_storage(self):
        model = await ModelWithEnum(status=Status.NEW).save()
        assert await get_as_pymongo(model) == {"_id": model.id, "status": "new"}

    async def test_set_enum(self):
        await ModelWithEnum.drop_collection()
        await ModelWithEnum(status=Status.NEW).save()
        assert await ModelWithEnum.objects(status=Status.NEW).count() == 1
        assert (await ModelWithEnum.objects.first()).status == Status.NEW

    async def test_set_by_value(self):
        await ModelWithEnum.drop_collection()
        await ModelWithEnum(status="new").save()
        assert (await ModelWithEnum.objects.first()).status == Status.NEW

    async def test_filter(self):
        await ModelWithEnum.drop_collection()
        await ModelWithEnum(status="new").save()
        assert await ModelWithEnum.objects(status="new").count() == 1
        assert await ModelWithEnum.objects(status=Status.NEW).count() == 1
        assert await ModelWithEnum.objects(status=Status.DONE).count() == 0

    async def test_change_value(self):
        m = ModelWithEnum(status="new")
        m.status = Status.DONE
        await m.save()
        assert m.status == Status.DONE

        m.status = "wrong"
        assert m.status == "wrong"
        with pytest.raises(ValidationError):
            m.validate()

    async def test_set_default(self):
        class ModelWithDefault(Document):
            status = EnumField(Status, default=Status.DONE)

        m = await ModelWithDefault().save()
        assert m.status == Status.DONE

    async def test_enum_field_can_be_empty(self):
        await ModelWithEnum.drop_collection()
        m = await ModelWithEnum().save()
        assert m.status is None
        assert (await ModelWithEnum.objects.first()).status is None
        assert await ModelWithEnum.objects(status=None).count() == 1

    async def test_set_none_explicitly(self):
        await ModelWithEnum.drop_collection()
        await ModelWithEnum(status=None).save()
        assert (await ModelWithEnum.objects.first()).status is None

    def test_cannot_create_model_with_wrong_enum_value(self):
        m = ModelWithEnum(status="wrong_one")
        with pytest.raises(ValidationError):
            m.validate()

    def test_partial_choices(self):
        partial = [Status.DONE]
        enum_field = EnumField(Status, choices=partial)
        assert enum_field.choices == partial

        class FancyDoc(Document):
            z = enum_field

        FancyDoc(z=Status.DONE).validate()
        with pytest.raises(ValidationError, match=r"Value must be one of .*Status.DONE"):
            FancyDoc(z=Status.NEW).validate()

    def test_wrong_choices(self):
        with pytest.raises(ValueError, match="Invalid choices"):
            EnumField(Status, choices=["my", "custom", "options"])
        with pytest.raises(ValueError, match="Invalid choices"):
            EnumField(Status, choices=[Color.RED])
        with pytest.raises(ValueError, match="Invalid choices"):
            EnumField(Status, choices=[Status.DONE, Color.RED])

    async def test_embedding_in_complex_field(self):
        await ModelComplexEnum.drop_collection()
        model = await ModelComplexEnum(status="new", statuses=["new"], color_mapping={"red": 1}).save()
        assert model.status == Status.NEW
        assert model.statuses == [Status.NEW]
        assert model.color_mapping == {"red": Color.RED}

        await model.reload()
        assert model.status == Status.NEW
        assert model.statuses == [Status.NEW]
        assert model.color_mapping == {"red": Color.RED}

        model.status = "done"
        model.color_mapping = {"blue": 2}
        model.statuses = ["new", "done"]
        await model.save()
        assert model.status == Status.DONE
        assert model.statuses == [Status.NEW, Status.DONE]
        assert model.color_mapping == {"blue": Color.BLUE}

        await model.reload()
        assert model.status == Status.DONE
        assert model.color_mapping == {"blue": Color.BLUE}
        assert model.statuses == [Status.NEW, Status.DONE]

        with pytest.raises(ValidationError, match="must be one of ..Status"):
            model.statuses = [1]
            await model.save()

        model.statuses = ["done"]
        model.color_mapping = {"blue": "done"}
        with pytest.raises(ValidationError, match="must be one of ..Color"):
            await model.save()


class ModelWithColor(Document):
    color = EnumField(Color, default=Color.RED)


class TestIntEnumField(MongoDBTestCase):
    async def test_enum_with_int(self):
        await ModelWithColor.drop_collection()
        m = await ModelWithColor().save()
        assert m.color == Color.RED
        assert await ModelWithColor.objects(color=Color.RED).count() == 1
        assert await ModelWithColor.objects(color=1).count() == 1
        assert await ModelWithColor.objects(color=2).count() == 0

    async def test_create_int_enum_by_value(self):
        model = await ModelWithColor(color=2).save()
        assert model.color == Color.BLUE

    async def test_storage_enum_with_int(self):
        model = await ModelWithColor(color=Color.BLUE).save()
        assert await get_as_pymongo(model) == {"_id": model.id, "color": 2}

    def test_validate_model(self):
        with pytest.raises(ValidationError, match="must be one of ..Color"):
            ModelWithColor(color="wrong_type").validate()


class TestFunkyEnumField(MongoDBTestCase):
    async def test_enum_incompatible_bson_type_fails_during_save(self):
        class FunkyColor(Enum):
            YELLOW = object()

        class ModelWithFunkyColor(Document):
            color = EnumField(FunkyColor)

        m = ModelWithFunkyColor(color=FunkyColor.YELLOW)

        with pytest.raises(InvalidDocument, match="[cC]annot encode object"):
            await m.save()
