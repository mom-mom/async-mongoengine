import pytest
from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine.base import LazyReference
from tests.utils import MongoDBTestCase


class TestLazyReferenceField(MongoDBTestCase):
    def test_lazy_reference_config(self):
        # Make sure ReferenceField only accepts a document class or a string
        # with a document class name.
        with pytest.raises(ValidationError):
            LazyReferenceField(EmbeddedDocument)

    async def test___repr__(self):
        class Animal(Document):
            pass

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = Animal()
        oc = Ocurrence(animal=animal)
        assert "LazyReference" in repr(oc.animal)

    async def test___getattr___unknown_attr_raises_attribute_error(self):
        class Animal(Document):
            pass

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal().save()
        oc = Ocurrence(animal=animal)
        with pytest.raises(AttributeError):
            oc.animal.not_exist

    async def test_lazy_reference_simple(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        await Ocurrence(person="test", animal=animal).save()
        p = await Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        fetched_animal = await p.animal.fetch()
        assert fetched_animal == animal
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        await animal.save()
        double_fetch = await p.animal.fetch()
        assert fetched_animal is double_fetch
        assert double_fetch.tag == "heavy"
        # ...unless specified otherwise
        fetch_force = await p.animal.fetch(force=True)
        assert fetch_force is not fetched_animal
        assert fetch_force.tag == "not so heavy"

    async def test_lazy_reference_fetch_invalid_ref(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        await Ocurrence(person="test", animal=animal).save()
        await animal.delete()
        p = await Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        with pytest.raises(DoesNotExist):
            await p.animal.fetch()

    async def test_lazy_reference_set(self):
        class Animal(Document):
            meta = {"allow_inheritance": True}

            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        class SubAnimal(Animal):
            nick = StringField()

        animal = await Animal(name="Leopard", tag="heavy").save()
        sub_animal = await SubAnimal(nick="doggo", name="dog").save()
        for ref in (
            animal,
            animal.pk,
            DBRef(animal._get_collection_name(), animal.pk),
            LazyReference(Animal, animal.pk),
            sub_animal,
            sub_animal.pk,
            DBRef(sub_animal._get_collection_name(), sub_animal.pk),
            LazyReference(SubAnimal, sub_animal.pk),
        ):
            p = await Ocurrence(person="test", animal=ref).save()
            await p.reload()
            assert isinstance(p.animal, LazyReference)
            await p.animal.fetch()

    async def test_lazy_reference_bad_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        class BadDoc(Document):
            pass

        animal = await Animal(name="Leopard", tag="heavy").save()
        baddoc = await BadDoc().save()
        for bad in (
            42,
            "foo",
            baddoc,
            DBRef(baddoc._get_collection_name(), animal.pk),
            LazyReference(BadDoc, animal.pk),
        ):
            with pytest.raises(ValidationError):
                await Ocurrence(person="test", animal=bad).save()

    async def test_lazy_reference_query_conversion(self):
        """Ensure that LazyReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """

        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = LazyReferenceField(Member, dbref=False)

        await Member.drop_collection()
        await BlogPost.drop_collection()

        m1 = Member(user_num=1)
        await m1.save()
        m2 = Member(user_num=2)
        await m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        await post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        await post2.save()

        post = await BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = await BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = await BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    async def test_lazy_reference_query_conversion_dbref(self):
        """Ensure that LazyReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """

        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = LazyReferenceField(Member, dbref=True)

        await Member.drop_collection()
        await BlogPost.drop_collection()

        m1 = Member(user_num=1)
        await m1.save()
        m2 = Member(user_num=2)
        await m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        await post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        await post2.save()

        post = await BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = await BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = await BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    async def test_lazy_reference_passthrough(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal, passthrough=False)
            animal_passthrough = LazyReferenceField(Animal, passthrough=True)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        await Ocurrence(animal=animal, animal_passthrough=animal).save()
        p = await Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        with pytest.raises(KeyError):
            p.animal["name"]
        with pytest.raises(AttributeError):
            p.animal.name
        assert p.animal.pk == animal.pk

        # Passthrough no longer works in async - must fetch explicitly
        # p.animal_passthrough.name would require fetch
        fetched = await p.animal_passthrough.fetch()
        assert fetched.name == "Leopard"

        # Should not be able to access referenced document's methods
        with pytest.raises(AttributeError):
            p.animal.save
        with pytest.raises(KeyError):
            p.animal["save"]

    async def test_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        await Ocurrence(person="foo").save()
        p = await Ocurrence.objects.get()
        assert p.animal is None

    async def test_lazy_reference_equality(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        await Animal.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        animalref = LazyReference(Animal, animal.pk)
        assert animal == animalref
        assert animalref == animal

        other_animalref = LazyReference(Animal, ObjectId("54495ad94c934721ede76f90"))
        assert animal != other_animalref
        assert other_animalref != animal

    async def test_lazy_reference_embedded(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class EmbeddedOcurrence(EmbeddedDocument):
            in_list = ListField(LazyReferenceField(Animal))
            direct = LazyReferenceField(Animal)

        class Ocurrence(Document):
            in_list = ListField(LazyReferenceField(Animal))
            in_embedded = EmbeddedDocumentField(EmbeddedOcurrence)
            direct = LazyReferenceField(Animal)

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal1 = await Animal(name="doggo").save()
        animal2 = await Animal(name="cheeta").save()

        def check_fields_type(occ):
            assert isinstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                assert isinstance(elem, LazyReference)
            assert isinstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                assert isinstance(elem, LazyReference)

        occ = await Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={"in_list": [animal1, animal2], "direct": animal1},
            direct=animal1,
        ).save()
        check_fields_type(occ)
        await occ.reload()
        check_fields_type(occ)
        occ.direct = animal1.id
        occ.in_list = [animal1.id, animal2.id]
        occ.in_embedded.direct = animal1.id
        occ.in_embedded.in_list = [animal1.id, animal2.id]
        check_fields_type(occ)

    async def test_lazy_reference_embedded_dereferencing(self):
        # Test case for #2375

        # -- Test documents

        class Author(Document):
            name = StringField()

        class AuthorReference(EmbeddedDocument):
            author = LazyReferenceField(Author)

        class Book(Document):
            authors = EmbeddedDocumentListField(AuthorReference)

        # -- Cleanup

        await Author.drop_collection()
        await Book.drop_collection()

        # -- Create test data

        author_1 = await Author(name="A1").save()
        author_2 = await Author(name="A2").save()
        author_3 = await Author(name="A3").save()
        await Book(
            authors=[
                AuthorReference(author=author_1),
                AuthorReference(author=author_2),
                AuthorReference(author=author_3),
            ]
        ).save()

        book = await Book.objects.first()
        # Accessing the list must not trigger dereferencing.
        book.authors

        for ref in book.authors:
            with pytest.raises(AttributeError):
                ref["author"].name
            assert isinstance(ref.author, LazyReference)
            assert isinstance(ref.author.id, ObjectId)

    async def test_lazy_reference_in_list_with_changed_element(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            in_list = ListField(LazyReferenceField(Animal))

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal1 = await Animal(name="doggo").save()

        animal1.tag = "blue"

        occ = await Ocurrence(in_list=[animal1]).save()
        await animal1.save()
        assert isinstance(occ.in_list[0], LazyReference)
        assert occ.in_list[0].pk == animal1.pk


class TestGenericLazyReferenceField(MongoDBTestCase):
    async def test_generic_lazy_reference_simple(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal(name="Leopard", tag="heavy").save()
        await Ocurrence(person="test", animal=animal).save()
        p = await Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        fetched_animal = await p.animal.fetch()
        assert fetched_animal == animal
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        await animal.save()
        double_fetch = await p.animal.fetch()
        assert fetched_animal is double_fetch
        assert double_fetch.tag == "heavy"
        # ...unless specified otherwise
        fetch_force = await p.animal.fetch(force=True)
        assert fetch_force is not fetched_animal
        assert fetch_force.tag == "not so heavy"

    async def test_generic_lazy_reference_choices(self):
        class Animal(Document):
            name = StringField()

        class Vegetal(Document):
            name = StringField()

        class Mineral(Document):
            name = StringField()

        class Ocurrence(Document):
            living_thing = GenericLazyReferenceField(choices=[Animal, Vegetal])
            thing = GenericLazyReferenceField()

        await Animal.drop_collection()
        await Vegetal.drop_collection()
        await Mineral.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal(name="Leopard").save()
        vegetal = await Vegetal(name="Oak").save()
        mineral = await Mineral(name="Granite").save()

        occ_animal = await Ocurrence(living_thing=animal, thing=animal).save()
        _ = await Ocurrence(living_thing=vegetal, thing=vegetal).save()
        with pytest.raises(ValidationError):
            await Ocurrence(living_thing=mineral).save()

        occ = await Ocurrence.objects.get(living_thing=animal)
        assert occ == occ_animal
        assert isinstance(occ.thing, LazyReference)
        assert isinstance(occ.living_thing, LazyReference)

        occ.thing = vegetal
        occ.living_thing = vegetal
        await occ.save()

        occ.thing = mineral
        occ.living_thing = mineral
        with pytest.raises(ValidationError):
            await occ.save()

    async def test_generic_lazy_reference_set(self):
        class Animal(Document):
            meta = {"allow_inheritance": True}

            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        class SubAnimal(Animal):
            nick = StringField()

        animal = await Animal(name="Leopard", tag="heavy").save()
        sub_animal = await SubAnimal(nick="doggo", name="dog").save()
        for ref in (
            animal,
            LazyReference(Animal, animal.pk),
            {"_cls": "Animal", "_ref": DBRef(animal._get_collection_name(), animal.pk)},
            sub_animal,
            LazyReference(SubAnimal, sub_animal.pk),
            {
                "_cls": "SubAnimal",
                "_ref": DBRef(sub_animal._get_collection_name(), sub_animal.pk),
            },
        ):
            p = await Ocurrence(person="test", animal=ref).save()
            await p.reload()
            assert isinstance(p.animal, (LazyReference, Document))
            await p.animal.fetch()

    async def test_generic_lazy_reference_bad_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField(choices=["Animal"])

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        class BadDoc(Document):
            pass

        animal = await Animal(name="Leopard", tag="heavy").save()
        baddoc = await BadDoc().save()
        for bad in (42, "foo", baddoc, LazyReference(BadDoc, animal.pk)):
            with pytest.raises(ValidationError):
                await Ocurrence(person="test", animal=bad).save()

    async def test_generic_lazy_reference_query_conversion(self):
        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = GenericLazyReferenceField()

        await Member.drop_collection()
        await BlogPost.drop_collection()

        m1 = Member(user_num=1)
        await m1.save()
        m2 = Member(user_num=2)
        await m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        await post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        await post2.save()

        post = await BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = await BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = await BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    async def test_generic_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        await Ocurrence(person="foo").save()
        p = await Ocurrence.objects.get()
        assert p.animal is None

    async def test_generic_lazy_reference_accepts_string_instead_of_class(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField("Animal")

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal = await Animal().save()
        await Ocurrence(animal=animal).save()
        p = await Ocurrence.objects.get()
        # GenericLazyReferenceField returns LazyReference
        assert isinstance(p.animal, LazyReference)
        fetched = await p.animal.fetch()
        assert fetched == animal

    async def test_generic_lazy_reference_embedded(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class EmbeddedOcurrence(EmbeddedDocument):
            in_list = ListField(GenericLazyReferenceField())
            direct = GenericLazyReferenceField()

        class Ocurrence(Document):
            in_list = ListField(GenericLazyReferenceField())
            in_embedded = EmbeddedDocumentField(EmbeddedOcurrence)
            direct = GenericLazyReferenceField()

        await Animal.drop_collection()
        await Ocurrence.drop_collection()

        animal1 = await Animal(name="doggo").save()
        animal2 = await Animal(name="cheeta").save()

        def check_fields_type(occ):
            assert isinstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                assert isinstance(elem, LazyReference)
            assert isinstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                assert isinstance(elem, LazyReference)

        occ = await Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={"in_list": [animal1, animal2], "direct": animal1},
            direct=animal1,
        ).save()
        check_fields_type(occ)
        await occ.reload()
        check_fields_type(occ)
        animal1_ref = {
            "_cls": "Animal",
            "_ref": DBRef(animal1._get_collection_name(), animal1.pk),
        }
        animal2_ref = {
            "_cls": "Animal",
            "_ref": DBRef(animal2._get_collection_name(), animal2.pk),
        }
        occ.direct = animal1_ref
        occ.in_list = [animal1_ref, animal2_ref]
        occ.in_embedded.direct = animal1_ref
        occ.in_embedded.in_list = [animal1_ref, animal2_ref]
        check_fields_type(occ)
