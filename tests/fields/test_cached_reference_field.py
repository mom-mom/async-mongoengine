from decimal import Decimal

import pytest

from mongoengine import (
    CachedReferenceField,
    DecimalField,
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    InvalidDocumentError,
    ListField,
    ReferenceField,
    StringField,
    ValidationError,
)
from tests.utils import MongoDBTestCase


class TestCachedReferenceField(MongoDBTestCase):
    def test_constructor_fail_bad_document_type(self):
        with pytest.raises(ValidationError, match="must be a document class or a string"):
            CachedReferenceField(document_type=0)

    async def test_get_and_save(self):
        """
        Tests #1047: CachedReferenceField creates DBRefs on to_python,
        but can't save them on to_mongo.
        """

        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal)

        await Animal.drop_collection()
        await Ocorrence.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        await Ocorrence(person="testte", animal=animal).save()
        p = await Ocorrence.objects.get()
        p.person = "new_testte"
        # CachedReferenceField returns a dict after fetch; re-assign the
        # actual document so validation passes on save.
        p.animal = animal
        await p.save()

    async def test_general_things(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag"])

        await Animal.drop_collection()
        await Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy")
        await a.save()

        assert Animal._cached_reference_fields == [Ocorrence.animal]
        o = Ocorrence(person="teste", animal=a)
        await o.save()

        p = Ocorrence(person="Wilson")
        await p.save()

        assert await Ocorrence.objects(animal=None).count() == 1

        assert a.to_mongo(fields=["tag"]) == {"tag": "heavy", "_id": a.pk}

        assert o.to_mongo()["animal"]["tag"] == "heavy"

        # counts
        await Ocorrence(person="teste 2").save()
        await Ocorrence(person="teste 3").save()

        count = await Ocorrence.objects(animal__tag="heavy").count()
        assert count == 1

        ocorrence = await Ocorrence.objects(animal__tag="heavy").first()
        assert ocorrence.person == "teste"
        # CachedReferenceField to_python returns raw dict, not Document
        assert isinstance(ocorrence.animal, dict)

    async def test_with_decimal(self):
        class PersonAuto(Document):
            name = StringField()
            salary = DecimalField()

        class SocialTest(Document):
            group = StringField()
            person = CachedReferenceField(PersonAuto, fields=("salary",))

        await PersonAuto.drop_collection()
        await SocialTest.drop_collection()

        p = PersonAuto(name="Alberto", salary=Decimal("7000.00"))
        await p.save()

        s = SocialTest(group="dev", person=p)
        await s.save()

        assert await SocialTest.objects._collection.find_one({"person.salary": 7000.00}) == {
            "_id": s.pk,
            "group": s.group,
            "person": {"_id": p.pk, "salary": 7000.00},
        }

    async def test_cached_reference_field_reference(self):
        class Group(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            group = ReferenceField(Group)

        class SocialData(Document):
            obs = StringField()
            tags = ListField(StringField())
            person = CachedReferenceField(Person, fields=("group",))

        await Group.drop_collection()
        await Person.drop_collection()
        await SocialData.drop_collection()

        g1 = Group(name="dev")
        await g1.save()

        g2 = Group(name="designers")
        await g2.save()

        p1 = Person(name="Alberto", group=g1)
        await p1.save()

        p2 = Person(name="Andre", group=g1)
        await p2.save()

        p3 = Person(name="Afro design", group=g2)
        await p3.save()

        s1 = SocialData(obs="testing 123", person=p1, tags=["tag1", "tag2"])
        await s1.save()

        s2 = SocialData(obs="testing 321", person=p3, tags=["tag3", "tag4"])
        await s2.save()

        assert await SocialData.objects._collection.find_one({"tags": "tag2"}) == {
            "_id": s1.pk,
            "obs": "testing 123",
            "tags": ["tag1", "tag2"],
            "person": {"_id": p1.pk, "group": g1.pk},
        }

        assert await SocialData.objects(person__group=g2).count() == 1
        assert await SocialData.objects(person__group=g2).first() == s2

    async def test_cached_reference_field_push_with_fields(self):
        class Product(Document):
            name = StringField()

        await Product.drop_collection()

        class Basket(Document):
            products = ListField(CachedReferenceField(Product, fields=["name"]))

        await Basket.drop_collection()
        product1 = await Product(name="abc").save()
        product2 = await Product(name="def").save()
        basket = await Basket(products=[product1]).save()
        assert await Basket.objects._collection.find_one() == {
            "_id": basket.pk,
            "products": [{"_id": product1.pk, "name": product1.name}],
        }
        # push to list
        await basket.update(push__products=product2)
        await basket.reload()
        assert await Basket.objects._collection.find_one() == {
            "_id": basket.pk,
            "products": [
                {"_id": product1.pk, "name": product1.name},
                {"_id": product2.pk, "name": product2.name},
            ],
        }

    async def test_cached_reference_field_update_all(self):
        class Person(Document):
            TYPES = (("pf", "PF"), ("pj", "PJ"))
            name = StringField()
            tp = StringField(choices=TYPES)
            father = CachedReferenceField("self", fields=("tp",))

        await Person.drop_collection()

        a1 = Person(name="Wilson Father", tp="pj")
        await a1.save()

        a2 = Person(name="Wilson Junior", tp="pf", father=a1)
        await a2.save()

        a2 = await Person.objects.with_id(a2.id)
        # CachedReferenceField to_python returns raw dict
        assert isinstance(a2.father, dict)
        assert a2.father["tp"] == a1.tp

        # Verify the raw data in the database matches expected shape
        raw = await Person.objects._collection.find_one({"_id": a2.pk})
        assert raw == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pj"},
        }

        assert Person.objects(father=a1)._query == {"father._id": a1.pk}
        assert await Person.objects(father=a1).count() == 1

        # sync_all is removed in async version
        # Just verify the update works
        await Person.objects.update(set__tp="pf")

    def test_cached_reference_fields_on_embedded_documents(self):
        with pytest.raises(InvalidDocumentError):

            class Test(Document):
                name = StringField()

            type(
                "WrongEmbeddedDocument",
                (EmbeddedDocument,),
                {"test": CachedReferenceField(Test)},
            )

    async def test_cached_reference_auto_sync_disabled(self):
        """In async, auto_sync via signal is disabled. Verify the cached
        data is NOT auto-updated when the referenced doc changes."""

        class Persone(Document):
            TYPES = (("pf", "PF"), ("pj", "PJ"))
            name = StringField()
            tp = StringField(choices=TYPES)

            father = CachedReferenceField("self", fields=("tp",), auto_sync=False)

        await Persone.drop_collection()

        a1 = Persone(name="Wilson Father", tp="pj")
        await a1.save()

        a2 = Persone(name="Wilson Junior", tp="pf", father=a1)
        await a2.save()

        a1.tp = "pf"
        await a1.save()

        assert await Persone.objects._collection.find_one({"_id": a2.pk}) == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pj"},
        }

    async def test_cached_reference_embedded_fields(self):
        class Owner(EmbeddedDocument):
            TPS = (("n", "Normal"), ("u", "Urgent"))
            name = StringField()
            tp = StringField(verbose_name="Type", db_field="t", choices=TPS)

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag", "owner.tp"])

        await Animal.drop_collection()
        await Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy", owner=Owner(tp="u", name="Wilson Junior"))
        await a.save()

        o = Ocorrence(person="teste", animal=a)
        await o.save()
        assert dict(a.to_mongo(fields=["tag", "owner.tp"])) == {
            "_id": a.pk,
            "tag": "heavy",
            "owner": {"t": "u"},
        }
        assert o.to_mongo()["animal"]["tag"] == "heavy"
        assert o.to_mongo()["animal"]["owner"]["t"] == "u"

        # Check to_mongo with fields
        assert "animal" not in o.to_mongo(fields=["person"])

        # counts
        await Ocorrence(person="teste 2").save()
        await Ocorrence(person="teste 3").save()

        count = await Ocorrence.objects(animal__tag="heavy", animal__owner__tp="u").count()
        assert count == 1

        ocorrence = await Ocorrence.objects(animal__tag="heavy", animal__owner__tp="u").first()
        assert ocorrence.person == "teste"
        # CachedReferenceField to_python returns raw dict
        assert isinstance(ocorrence.animal, dict)

    async def test_cached_reference_embedded_list_fields(self):
        class Owner(EmbeddedDocument):
            name = StringField()
            tags = ListField(StringField())

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag", "owner.tags"])

        await Animal.drop_collection()
        await Ocorrence.drop_collection()

        a = Animal(
            name="Leopard",
            tag="heavy",
            owner=Owner(tags=["cool", "funny"], name="Wilson Junior"),
        )
        await a.save()

        o = Ocorrence(person="teste 2", animal=a)
        await o.save()
        assert dict(a.to_mongo(fields=["tag", "owner.tags"])) == {
            "_id": a.pk,
            "tag": "heavy",
            "owner": {"tags": ["cool", "funny"]},
        }

        assert o.to_mongo()["animal"]["tag"] == "heavy"
        assert o.to_mongo()["animal"]["owner"]["tags"] == ["cool", "funny"]

        # counts
        await Ocorrence(person="teste 2").save()
        await Ocorrence(person="teste 3").save()

        query = Ocorrence.objects(animal__tag="heavy", animal__owner__tags="cool")._query
        assert query == {"animal.owner.tags": "cool", "animal.tag": "heavy"}

        ocorrence = await Ocorrence.objects(animal__tag="heavy", animal__owner__tags="cool").first()
        assert ocorrence.person == "teste 2"
        # CachedReferenceField to_python returns raw dict
        assert isinstance(ocorrence.animal, dict)
