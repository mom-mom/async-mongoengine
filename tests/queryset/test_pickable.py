import pickle

from mongoengine import Document, IntField, StringField
from tests.utils import MongoDBTestCase


class Person(Document):
    name = StringField()
    age = IntField()


class TestQuerysetPickable(MongoDBTestCase):
    """
    Test for adding pickling support for QuerySet instances
    See issue https://github.com/MongoEngine/mongoengine/issues/442
    """

    async def setup_method(self):
        self.john = await Person.objects.create(name="John", age=21)

    async def test_picke_simple_qs(self):
        qs = Person.objects.all()
        pickle.dumps(qs)

    def _get_loaded(self, qs):
        s = pickle.dumps(qs)
        return pickle.loads(s)

    async def test_unpickle(self):
        qs = Person.objects.all()

        loadedQs = self._get_loaded(qs)

        assert await qs.count() == await loadedQs.count()

        # can update loadedQs
        await loadedQs.update(age=23)

        # check
        assert (await Person.objects.first()).age == 23

    async def test_pickle_support_filtration(self):
        await Person.objects.create(name="Alice", age=22)

        await Person.objects.create(name="Bob", age=23)

        qs = Person.objects.filter(age__gte=22)
        assert await qs.count() == 2

        loaded = self._get_loaded(qs)

        assert await loaded.count() == 2
        assert (await loaded.filter(name="Bob").first()).age == 23
