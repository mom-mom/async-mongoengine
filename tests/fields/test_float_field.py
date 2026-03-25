import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestFloatField(MongoDBTestCase):
    async def test_float_ne_operator(self):
        class TestDocument(Document):
            float_fld = FloatField()

        await TestDocument.drop_collection()

        await TestDocument(float_fld=None).save()
        await TestDocument(float_fld=1).save()

        assert 1 == await TestDocument.objects(float_fld__ne=None).count()
        assert 1 == await TestDocument.objects(float_fld__ne=1).count()

    def test_validation(self):
        """Ensure that invalid values cannot be assigned to float fields."""

        class Person(Document):
            height = FloatField(min_value=0.1, max_value=3.5)

        class BigPerson(Document):
            height = FloatField()

        person = Person()
        person.height = 1.89
        person.validate()

        person.height = "2.0"
        with pytest.raises(ValidationError):
            person.validate()

        person.height = 0.01
        with pytest.raises(ValidationError):
            person.validate()

        person.height = 4.0
        with pytest.raises(ValidationError):
            person.validate()

        person_2 = Person(height="something invalid")
        with pytest.raises(ValidationError):
            person_2.validate()

        big_person = BigPerson()

        big_person.height = 0
        big_person.validate()

        big_person.height = 2**500
        big_person.validate()

        big_person.height = 2**100000  # Too big for a float value
        with pytest.raises(ValidationError):
            big_person.validate()

    async def test_query_none_value_dont_raise(self):
        class BigPerson(Document):
            height = FloatField()

        _ = [d async for d in BigPerson.objects(height=None)]
