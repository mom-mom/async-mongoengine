import datetime

import pytest

from mongoengine import *
from mongoengine.queryset import (
    QuerySet,
    QuerySetManager,
    queryset_manager,
)
from mongoengine.queryset.base import BaseQuerySet
from tests.utils import (
    MongoDBTestCase,
)


class TestQueryset4(MongoDBTestCase):
    def setup_method(self, method=None):
        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {"allow_inheritance": True}

        self.PersonMeta = PersonMeta
        self.Person = Person

    async def assertSequence(self, qs, expected):
        qs = [d async for d in qs]
        expected = list(expected)
        assert len(qs) == len(expected)
        for i in range(len(qs)):
            assert qs[i] == expected[i]

    async def tearDown(self):
        await self.Person.drop_collection()

    async def test_item_frequencies(self):
        """Ensure that item frequencies are properly generated from lists."""

        class BlogPost(Document):
            hits = IntField()
            tags = ListField(StringField(), db_field="blogTags")

        await BlogPost.drop_collection()

        await BlogPost(hits=1, tags=["music", "film", "actors", "watch"]).save()
        await BlogPost(hits=2, tags=["music", "watch"]).save()
        await BlogPost(hits=2, tags=["music", "actors"]).save()

        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"music", "film", "actors", "watch"} == set(f.keys())
            assert f["music"] == 3
            assert f["actors"] == 2
            assert f["watch"] == 2
            assert f["film"] == 1

        freq = await BlogPost.objects.item_frequencies("tags")
        test_assertions(freq)

        # Ensure query is taken into account
        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"music", "actors", "watch"} == set(f.keys())
            assert f["music"] == 2
            assert f["actors"] == 1
            assert f["watch"] == 1

        freq = await BlogPost.objects(hits__gt=1).item_frequencies("tags")
        test_assertions(freq)

        # Check that normalization works
        def test_assertions(f):
            assert round(abs(f["music"] - 3.0 / 8.0), 7) == 0
            assert round(abs(f["actors"] - 2.0 / 8.0), 7) == 0
            assert round(abs(f["watch"] - 2.0 / 8.0), 7) == 0
            assert round(abs(f["film"] - 1.0 / 8.0), 7) == 0

        freq = await BlogPost.objects.item_frequencies("tags", normalize=True)
        test_assertions(freq)

        # Check item_frequencies works for non-list fields
        def test_assertions(f):
            assert {1, 2} == set(f.keys())
            assert f[1] == 1
            assert f[2] == 2

        freq = await BlogPost.objects.item_frequencies("hits")
        test_assertions(freq)

        await BlogPost.drop_collection()

    async def test_item_frequencies_on_embedded(self):
        """Ensure that item frequencies are properly generated from lists."""

        class Phone(EmbeddedDocument):
            number = StringField()

        class Person(Document):
            name = StringField()
            phone = EmbeddedDocumentField(Phone)

        await Person.drop_collection()

        doc = Person(name="Guido")
        doc.phone = Phone(number="62-3331-1656")
        await doc.save()

        doc = Person(name="Marr")
        doc.phone = Phone(number="62-3331-1656")
        await doc.save()

        doc = Person(name="WP Junior")
        doc.phone = Phone(number="62-3332-1656")
        await doc.save()

        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"62-3331-1656", "62-3332-1656"} == set(f.keys())
            assert f["62-3331-1656"] == 2
            assert f["62-3332-1656"] == 1

        freq = await Person.objects.item_frequencies("phone.number")
        test_assertions(freq)

        # Ensure query is taken into account
        def test_assertions(f):
            f = {key: int(val) for key, val in f.items()}
            assert {"62-3331-1656"} == set(f.keys())
            assert f["62-3331-1656"] == 2

        freq = await Person.objects(phone__number="62-3331-1656").item_frequencies("phone.number")
        test_assertions(freq)

        # Check that normalization works
        def test_assertions(f):
            assert f["62-3331-1656"] == 2.0 / 3.0
            assert f["62-3332-1656"] == 1.0 / 3.0

        freq = await Person.objects.item_frequencies("phone.number", normalize=True)
        test_assertions(freq)

    async def test_item_frequencies_null_values(self):
        class Person(Document):
            name = StringField()
            city = StringField()

        await Person.drop_collection()

        await Person(name="Wilson Snr", city="CRB").save()
        await Person(name="Wilson Jr").save()

        freq = await Person.objects.item_frequencies("city")
        assert freq == {"CRB": 1.0, None: 1.0}
        freq = await Person.objects.item_frequencies("city", normalize=True)
        assert freq == {"CRB": 0.5, None: 0.5}

    async def test_average(self):
        """Ensure that field can be averaged correctly."""
        await self.Person(name="person", age=0).save()
        assert int(await self.Person.objects.average("age")) == 0

        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            await self.Person(name=f"test{i}", age=age).save()

        avg = float(sum(ages)) / (len(ages) + 1)  # take into account the 0
        assert round(abs(int(await self.Person.objects.average("age")) - avg), 7) == 0

        await self.Person(name="ageless person").save()
        assert int(await self.Person.objects.average("age")) == avg

        # dot notation
        await self.Person(name="person meta", person_meta=self.PersonMeta(weight=0)).save()
        assert round(abs(int(await self.Person.objects.average("person_meta.weight")) - 0), 7) == 0

        for i, weight in enumerate(ages):
            await self.Person(name=f"test meta{i}", person_meta=self.PersonMeta(weight=weight)).save()

        assert round(abs(int(await self.Person.objects.average("person_meta.weight")) - avg), 7) == 0

        await self.Person(name="test meta none").save()
        assert int(await self.Person.objects.average("person_meta.weight")) == avg

        # test summing over a filtered queryset
        over_50 = [a for a in ages if a >= 50]
        avg = float(sum(over_50)) / len(over_50)
        assert await self.Person.objects.filter(age__gte=50).average("age") == avg

    async def test_sum(self):
        """Ensure that field can be summed over correctly."""
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            await self.Person(name=f"test{i}", age=age).save()

        assert await self.Person.objects.sum("age") == sum(ages)

        await self.Person(name="ageless person").save()
        assert await self.Person.objects.sum("age") == sum(ages)

        for i, age in enumerate(ages):
            await self.Person(name=f"test meta{i}", person_meta=self.PersonMeta(weight=age)).save()

        assert await self.Person.objects.sum("person_meta.weight") == sum(ages)

        await self.Person(name="weightless person").save()
        assert await self.Person.objects.sum("age") == sum(ages)

        # test summing over a filtered queryset
        assert await self.Person.objects.filter(age__gte=50).sum("age") == sum(a for a in ages if a >= 50)

    async def test_sum_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can be summed over correctly.
        """

        class UserVisit(Document):
            num_visits = IntField(db_field="visits")

        await UserVisit.drop_collection()

        await UserVisit.objects.create(num_visits=10)
        await UserVisit.objects.create(num_visits=5)

        assert await UserVisit.objects.sum("num_visits") == 15

    async def test_average_over_db_field(self):
        """Ensure that a field mapped to a db field with a different name
        can have its average computed correctly.
        """

        class UserVisit(Document):
            num_visits = IntField(db_field="visits")

        await UserVisit.drop_collection()

        await UserVisit.objects.create(num_visits=20)
        await UserVisit.objects.create(num_visits=10)

        assert await UserVisit.objects.average("num_visits") == 15

    async def test_embedded_average(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.drop_collection()

        await Doc(name="Wilson Junior", pay=Pay(value=150)).save()
        await Doc(name="Isabella Luanna", pay=Pay(value=530)).save()
        await Doc(name="Tayza mariana", pay=Pay(value=165)).save()
        await Doc(name="Eliana Costa", pay=Pay(value=115)).save()

        assert await Doc.objects.average("pay.value") == 240

    async def test_embedded_array_average(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.drop_collection()

        await Doc(name="Wilson Junior", pay=Pay(values=[150, 100])).save()
        await Doc(name="Isabella Luanna", pay=Pay(values=[530, 100])).save()
        await Doc(name="Tayza mariana", pay=Pay(values=[165, 100])).save()
        await Doc(name="Eliana Costa", pay=Pay(values=[115, 100])).save()

        assert await Doc.objects.average("pay.values") == 170

    async def test_array_average(self):
        class Doc(Document):
            values = ListField(DecimalField())

        await Doc.drop_collection()

        await Doc(values=[150, 100]).save()
        await Doc(values=[530, 100]).save()
        await Doc(values=[165, 100]).save()
        await Doc(values=[115, 100]).save()

        assert await Doc.objects.average("values") == 170

    async def test_embedded_sum(self):
        class Pay(EmbeddedDocument):
            value = DecimalField()

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.drop_collection()

        await Doc(name="Wilson Junior", pay=Pay(value=150)).save()
        await Doc(name="Isabella Luanna", pay=Pay(value=530)).save()
        await Doc(name="Tayza mariana", pay=Pay(value=165)).save()
        await Doc(name="Eliana Costa", pay=Pay(value=115)).save()

        assert await Doc.objects.sum("pay.value") == 960

    async def test_embedded_array_sum(self):
        class Pay(EmbeddedDocument):
            values = ListField(DecimalField())

        class Doc(Document):
            name = StringField()
            pay = EmbeddedDocumentField(Pay)

        await Doc.drop_collection()

        await Doc(name="Wilson Junior", pay=Pay(values=[150, 100])).save()
        await Doc(name="Isabella Luanna", pay=Pay(values=[530, 100])).save()
        await Doc(name="Tayza mariana", pay=Pay(values=[165, 100])).save()
        await Doc(name="Eliana Costa", pay=Pay(values=[115, 100])).save()

        assert await Doc.objects.sum("pay.values") == 1360

    async def test_array_sum(self):
        class Doc(Document):
            values = ListField(DecimalField())

        await Doc.drop_collection()

        await Doc(values=[150, 100]).save()
        await Doc(values=[530, 100]).save()
        await Doc(values=[165, 100]).save()
        await Doc(values=[115, 100]).save()

        assert await Doc.objects.sum("values") == 1360

    async def test_distinct(self):
        """Ensure that the QuerySet.distinct method works."""
        await self.Person(name="Mr Orange", age=20).save()
        await self.Person(name="Mr White", age=20).save()
        await self.Person(name="Mr Orange", age=30).save()
        await self.Person(name="Mr Pink", age=30).save()
        assert set(await self.Person.objects.distinct("name")) == {
            "Mr Orange",
            "Mr White",
            "Mr Pink",
        }
        assert set(await self.Person.objects.distinct("age")) == {20, 30}
        assert set(await self.Person.objects(age=30).distinct("name")) == {
            "Mr Orange",
            "Mr Pink",
        }

    async def test_distinct_handles_references(self):
        class Foo(Document):
            bar = ReferenceField("Bar")

        class Bar(Document):
            text = StringField()

        await Bar.drop_collection()
        await Foo.drop_collection()

        bar = Bar(text="hi")
        await bar.save()

        foo = Foo(bar=bar)
        await foo.save()

        assert await Foo.objects.distinct("bar") == [bar]

    async def test_base_queryset_iter_raise_not_implemented(self):
        class Tmp(Document):
            pass

        qs = BaseQuerySet(document=Tmp, collection=await Tmp._get_collection())
        with pytest.raises(NotImplementedError):
            _ = [d for d in qs]

    async def test_search_text_raise_if_called_2_times(self):
        class News(Document):
            title = StringField()
            content = StringField()
            is_active = BooleanField(default=True)

        await News.drop_collection()
        with pytest.raises(OperationError):
            News.objects.search_text("t1", language="portuguese").search_text("t2", language="french")

    async def test_search_text(self):
        class News(Document):
            title = StringField()
            content = StringField()
            is_active = BooleanField(default=True)

            meta = {
                "indexes": [
                    {
                        "fields": ["$title", "$content"],
                        "default_language": "portuguese",
                        "weights": {"title": 10, "content": 2},
                    }
                ]
            }

        await News.drop_collection()
        collection = await News._get_collection()
        info = await collection.index_information()
        assert "title_text_content_text" in info
        assert "textIndexVersion" in info["title_text_content_text"]

        await News(
            title="Neymar quebrou a vertebra",
            content="O Brasil sofre com a perda de Neymar",
        ).save()

        await News(
            title="Brasil passa para as quartas de finais",
            content="Com o brasil nas quartas de finais teremos um jogo complicado com a alemanha",
        ).save()

        count = await News.objects.search_text("neymar", language="portuguese").count()

        assert count == 1

        count = await News.objects.search_text("brasil -neymar").count()

        assert count == 1

        await News(
            title="As eleições no Brasil já estão em planejamento",
            content="A candidata dilma roussef já começa o teu planejamento",
            is_active=False,
        ).save()

        new = await News.objects(is_active=False).search_text("dilma", language="pt").first()

        query = News.objects(is_active=False).search_text("dilma", language="pt")._query

        assert query == {
            "$text": {"$search": "dilma", "$language": "pt"},
            "is_active": False,
        }

        assert not new.is_active
        assert "dilma" in new.content
        assert "planejamento" in new.title

        query = News.objects.search_text("candidata", text_score=True)
        assert query._search_text == "candidata"
        new = await query.first()

        assert isinstance(new.get_text_score(), float)

        # count
        query = News.objects.search_text("brasil", text_score=True).order_by("$text_score")
        assert query._search_text == "brasil"

        assert await query.count() == 3
        assert query._query == {"$text": {"$search": "brasil"}}
        cursor_args = query._cursor_args
        cursor_args_fields = cursor_args["projection"]
        assert cursor_args_fields == {"_text_score": {"$meta": "textScore"}}

        text_scores = [i.get_text_score() async for i in query]
        assert len(text_scores) == 3

        assert text_scores[0] > text_scores[1]
        assert text_scores[1] > text_scores[2]
        max_text_score = text_scores[0]

        # get item
        item = await News.objects.search_text("brasil").order_by("$text_score").first()
        assert item.get_text_score() == max_text_score

        # Verify query reproducibility when text_score is disabled
        # Following wouldn't work for text_score=True  #2759
        for i in range(10):
            qs1 = News.objects.search_text("brasil", text_score=False)
            qs2 = News.objects.search_text("brasil", text_score=False)
            assert [d async for d in qs1] == [d async for d in qs2]

    async def test_distinct_handles_references_to_alias(self):
        register_connection("testdb", "mongoenginetest2")

        class Foo(Document):
            bar = ReferenceField("Bar")
            meta = {"db_alias": "testdb"}

        class Bar(Document):
            text = StringField()
            meta = {"db_alias": "testdb"}

        await Bar.drop_collection()
        await Foo.drop_collection()

        bar = Bar(text="hi")
        await bar.save()

        foo = Foo(bar=bar)
        await foo.save()

        assert await Foo.objects.distinct("bar") == [bar]

    async def test_distinct_handles_db_field(self):
        """Ensure that distinct resolves field name to db_field as expected."""

        class Product(Document):
            product_id = IntField(db_field="pid")

        await Product.drop_collection()

        await Product(product_id=1).save()
        await Product(product_id=2).save()
        await Product(product_id=1).save()

        assert set(await Product.objects.distinct("product_id")) == {1, 2}
        assert set(await Product.objects.distinct("pid")) == {1, 2}

        await Product.drop_collection()

    async def test_distinct_ListField_EmbeddedDocumentField(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Book(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField(Author))

        await Book.drop_collection()

        mark_twain = Author(name="Mark Twain")
        john_tolkien = Author(name="John Ronald Reuel Tolkien")

        await Book.objects.create(title="Tom Sawyer", authors=[mark_twain])
        await Book.objects.create(title="The Lord of the Rings", authors=[john_tolkien])
        await Book.objects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        authors = await Book.objects.distinct("authors")
        authors_names = {author.name for author in authors}
        assert authors_names == {mark_twain.name, john_tolkien.name}

    async def test_distinct_ListField_EmbeddedDocumentField_EmbeddedDocumentField(self):
        class Continent(EmbeddedDocument):
            continent_name = StringField()

        class Country(EmbeddedDocument):
            country_name = StringField()
            continent = EmbeddedDocumentField(Continent)

        class Author(EmbeddedDocument):
            name = StringField()
            country = EmbeddedDocumentField(Country)

        class Book(Document):
            title = StringField()
            authors = ListField(EmbeddedDocumentField(Author))

        await Book.drop_collection()

        europe = Continent(continent_name="europe")
        asia = Continent(continent_name="asia")

        scotland = Country(country_name="Scotland", continent=europe)
        tibet = Country(country_name="Tibet", continent=asia)

        mark_twain = Author(name="Mark Twain", country=scotland)
        john_tolkien = Author(name="John Ronald Reuel Tolkien", country=tibet)

        await Book.objects.create(title="Tom Sawyer", authors=[mark_twain])
        await Book.objects.create(title="The Lord of the Rings", authors=[john_tolkien])
        await Book.objects.create(title="The Stories", authors=[mark_twain, john_tolkien])

        country_list = await Book.objects.distinct("authors.country")
        assert country_list == [scotland, tibet]

        continent_list = await Book.objects.distinct("authors.country.continent")
        continent_list_names = {c.continent_name for c in continent_list}
        assert continent_list_names == {europe.continent_name, asia.continent_name}

    async def test_distinct_ListField_ReferenceField(self):
        class Bar(Document):
            text = StringField()

        class Foo(Document):
            bar = ReferenceField("Bar")
            bar_lst = ListField(ReferenceField("Bar"))

        await Bar.drop_collection()
        await Foo.drop_collection()

        bar_1 = Bar(text="hi")
        await bar_1.save()

        bar_2 = Bar(text="bye")
        await bar_2.save()

        foo = Foo(bar=bar_1, bar_lst=[bar_1, bar_2])
        await foo.save()

        assert await Foo.objects.distinct("bar_lst") == [bar_1, bar_2]

    async def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected."""

        class BlogPost(Document):
            tags = ListField(StringField())
            deleted = BooleanField(default=False)
            date = DateTimeField(default=datetime.datetime.now)

            @queryset_manager
            def objects(cls, qryset):
                opts = {"deleted": False}
                return qryset(**opts)

            @queryset_manager
            def objects_1_arg(qryset):
                opts = {"deleted": False}
                return qryset(**opts)

            @queryset_manager
            def music_posts(doc_cls, queryset, deleted=False):
                return queryset(tags="music", deleted=deleted).order_by("date")

        await BlogPost.drop_collection()

        post1 = await BlogPost(tags=["music", "film"]).save()
        post2 = await BlogPost(tags=["music"]).save()
        post3 = await BlogPost(tags=["film", "actors"]).save()
        post4 = await BlogPost(tags=["film", "actors", "music"], deleted=True).save()

        assert [p.id async for p in BlogPost.objects()] == [post1.id, post2.id, post3.id]
        assert [p.id async for p in BlogPost.objects_1_arg()] == [
            post1.id,
            post2.id,
            post3.id,
        ]
        assert [p.id async for p in BlogPost.music_posts()] == [post1.id, post2.id]

        assert [p.id async for p in BlogPost.music_posts(True)] == [post4.id]

        await BlogPost.drop_collection()

    async def test_custom_manager_overriding_objects_works(self):
        class Foo(Document):
            bar = StringField(default="bar")
            active = BooleanField(default=False)

            @queryset_manager
            def objects(doc_cls, queryset):
                return queryset(active=True)

            @queryset_manager
            def with_inactive(doc_cls, queryset):
                return queryset(active=False)

        await Foo.drop_collection()

        await Foo(active=True).save()
        await Foo(active=False).save()

        assert 1 == await Foo.objects.count()
        assert 1 == await Foo.with_inactive.count()

        await (await Foo.with_inactive.first()).delete()
        assert 0 == await Foo.with_inactive.count()
        assert 1 == await Foo.objects.count()

    async def test_inherit_objects(self):
        class Foo(Document):
            meta = {"allow_inheritance": True}
            active = BooleanField(default=True)

            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):
            pass

        await Bar.drop_collection()
        await Bar.objects.create(active=False)
        assert 0 == await Bar.objects.count()

    async def test_inherit_objects_override(self):
        class Foo(Document):
            meta = {"allow_inheritance": True}
            active = BooleanField(default=True)

            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=True)

        class Bar(Foo):
            @queryset_manager
            def objects(klass, queryset):
                return queryset(active=False)

        await Bar.drop_collection()
        await Bar.objects.create(active=False)
        assert 0 == await Foo.objects.count()
        assert 1 == await Bar.objects.count()

    async def test_query_value_conversion(self):
        """Ensure that query values are properly converted when necessary."""

        class BlogPost(Document):
            author = ReferenceField(self.Person)

        await BlogPost.drop_collection()

        person = self.Person(name="test", age=30)
        await person.save()

        post = BlogPost(author=person)
        await post.save()

        # Test that query may be performed by providing a document as a value
        # while using a ReferenceField's name - the document should be
        # converted to an DBRef, which is legal, unlike a Document object
        post_obj = await BlogPost.objects(author=person).first()
        assert post.id == post_obj.id

        # Test that lists of values work when using the 'in', 'nin' and 'all'
        post_obj = await BlogPost.objects(author__in=[person]).first()
        assert post.id == post_obj.id

        await BlogPost.drop_collection()

    async def test_update_value_conversion(self):
        """Ensure that values used in updates are converted before use."""

        class Group(Document):
            members = ListField(ReferenceField(self.Person))

        await Group.drop_collection()

        user1 = self.Person(name="user1")
        await user1.save()
        user2 = self.Person(name="user2")
        await user2.save()

        group = Group()
        await group.save()

        await Group.objects(id=group.id).update(set__members=[user1, user2])
        await group.reload()

        assert len(group.members) == 2
        assert group.members[0].id == user1.id
        assert group.members[1].id == user2.id

        await Group.drop_collection()

    async def test_bulk(self):
        """Ensure bulk querying by object id returns a proper dict."""

        class BlogPost(Document):
            title = StringField()

        await BlogPost.drop_collection()

        post_1 = BlogPost(title="Post #1")
        post_2 = BlogPost(title="Post #2")
        post_3 = BlogPost(title="Post #3")
        post_4 = BlogPost(title="Post #4")
        post_5 = BlogPost(title="Post #5")

        await post_1.save()
        await post_2.save()
        await post_3.save()
        await post_4.save()
        await post_5.save()

        ids = [post_1.id, post_2.id, post_5.id]
        objects = await BlogPost.objects.in_bulk(ids)

        assert len(objects) == 3

        assert post_1.id in objects
        assert post_2.id in objects
        assert post_5.id in objects

        assert objects[post_1.id].title == post_1.title
        assert objects[post_2.id].title == post_2.title
        assert objects[post_5.id].title == post_5.title

        objects = await BlogPost.objects.as_pymongo().in_bulk(ids)
        assert len(objects) == 3
        assert isinstance(objects[post_1.id], dict)

        await BlogPost.drop_collection()

    async def test_custom_querysets(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(QuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class Post(Document):
            meta = {"queryset_class": CustomQuerySet}

        await Post.drop_collection()

        assert isinstance(Post.objects, CustomQuerySet)
        assert not await Post.objects.not_empty()

        await Post().save()
        assert await Post.objects.not_empty()

        await Post.drop_collection()

    async def test_custom_querysets_set_manager_directly(self):
        """Ensure that custom QuerySet classes may be used."""

        class CustomQuerySet(QuerySet):
            async def not_empty(self):
                return await self.count() > 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Post(Document):
            objects = CustomQuerySetManager()

        await Post.drop_collection()

        assert isinstance(Post.objects, CustomQuerySet)
        assert not await Post.objects.not_empty()

        await Post().save()
        assert await Post.objects.not_empty()

        await Post.drop_collection()

    async def test_custom_querysets_set_manager_methods(self):
        """Ensure that custom QuerySet classes methods may be used."""

        class CustomQuerySet(QuerySet):
            async def delete(self, *args, **kwargs):
                """Example of method when one want to change default behaviour of it"""
                return 0

        class CustomQuerySetManager(QuerySetManager):
            queryset_class = CustomQuerySet

        class Post(Document):
            objects = CustomQuerySetManager()

        await Post.drop_collection()

        assert isinstance(Post.objects, CustomQuerySet)
        assert await Post.objects.delete() == 0

        post = Post()
        await post.save()
        assert await Post.objects.count() == 1
        await post.delete()
        assert await Post.objects.count() == 1

        await Post.drop_collection()
