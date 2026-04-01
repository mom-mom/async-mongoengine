import weakref

import pytest

from mongoengine import *
from mongoengine.errors import InvalidDocumentError
from tests.utils import MongoDBTestCase


class TestInstanceEmbedded(MongoDBTestCase):
    def setup_method(self, method=None):
        class Job(EmbeddedDocument):
            name = StringField()
            years = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = EmbeddedDocumentField(Job)

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person
        self.Job = Job

    def _assert_has_instance(self, field, instance):
        assert hasattr(field, "_instance")
        assert field._instance is not None
        if isinstance(field._instance, weakref.ProxyType):
            assert field._instance.__eq__(instance)
        else:
            assert field._instance == instance

    async def test_embedded_document_to_mongo(self):
        class Person(EmbeddedDocument):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        assert sorted(Person(name="Bob", age=35).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
        ]
        assert sorted(Employee(name="Bob", age=35, salary=0).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
            "salary",
        ]

    async def test_embedded_document_to_mongo_id(self):
        class SubDoc(EmbeddedDocument):
            id = StringField(required=True)

        sub_doc = SubDoc(id="abc")
        assert list(sub_doc.to_mongo().keys()) == ["id"]

    async def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly."""

        class Comment(EmbeddedDocument):
            content = StringField()

        assert "content" in Comment._fields
        assert "id" not in Comment._fields

    async def test_embedded_document_instance(self):
        """Ensure that embedded documents can reference parent instance."""

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.drop_collection()

        doc = Doc(embedded_field=Embedded(string="Hi"))
        self._assert_has_instance(doc.embedded_field, doc)

        await doc.save()
        doc = await Doc.objects.get()
        self._assert_has_instance(doc.embedded_field, doc)

    async def test_embedded_document_complex_instance(self):
        """Ensure that embedded documents in complex fields can reference
        parent instance.
        """

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        await Doc.drop_collection()
        doc = Doc(embedded_field=[Embedded(string="Hi")])
        self._assert_has_instance(doc.embedded_field[0], doc)

        await doc.save()
        doc = await Doc.objects.get()
        self._assert_has_instance(doc.embedded_field[0], doc)

    async def test_embedded_document_complex_instance_no_use_db_field(self):
        """Ensure that use_db_field is propagated to list of Emb Docs."""

        class Embedded(EmbeddedDocument):
            string = StringField(db_field="s")

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        d = Doc(embedded_field=[Embedded(string="Hi")]).to_mongo(use_db_field=False).to_dict()
        assert d["embedded_field"] == [{"string": "Hi"}]

    async def test_instance_is_set_on_setattr(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            email = EmbeddedDocumentField(Email)

        await Account.drop_collection()

        acc = Account()
        acc.email = Email(email="test@example.com")
        self._assert_has_instance(acc._data["email"], acc)
        await acc.save()

        acc1 = await Account.objects.first()
        self._assert_has_instance(acc1._data["email"], acc1)

    async def test_instance_is_set_on_setattr_on_embedded_document_list(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            emails = EmbeddedDocumentListField(Email)

        await Account.drop_collection()
        acc = Account()
        acc.emails = [Email(email="test@example.com")]
        self._assert_has_instance(acc._data["emails"][0], acc)
        await acc.save()

        acc1 = await Account.objects.first()
        self._assert_has_instance(acc1._data["emails"][0], acc1)

    async def test_document_embedded_clean(self):
        class TestEmbeddedDocument(EmbeddedDocument):
            x = IntField(required=True)
            y = IntField(required=True)
            z = IntField(required=True)

            meta = {"allow_inheritance": False}

            def clean(self):
                if self.z:
                    if self.z != self.x + self.y:
                        raise ValidationError("Value of z != x + y")
                else:
                    self.z = self.x + self.y

        class TestDocument(Document):
            doc = EmbeddedDocumentField(TestEmbeddedDocument)
            status = StringField()

        await TestDocument.drop_collection()

        t = TestDocument(doc=TestEmbeddedDocument(x=10, y=25, z=15))

        with pytest.raises(ValidationError) as exc_info:
            await t.save()

        expected_msg = "Value of z != x + y"
        assert expected_msg in str(exc_info.value)
        assert exc_info.value.to_dict() == {"doc": {"__all__": expected_msg}}

        t = await TestDocument(doc=TestEmbeddedDocument(x=10, y=25)).save()
        assert t.doc.z == 35

        # Asserts not raises
        t = TestDocument(doc=TestEmbeddedDocument(x=15, y=35, z=5))
        await t.save(clean=False)

    async def test_list_search_by_embedded(self):
        class User(Document):
            username = StringField(required=True)

            meta = {"allow_inheritance": False}

        class Comment(EmbeddedDocument):
            comment = StringField()
            user = ReferenceField(User, required=True)

            meta = {"allow_inheritance": False}

        class Page(Document):
            comments = ListField(EmbeddedDocumentField(Comment))
            meta = {
                "allow_inheritance": False,
                "indexes": [{"fields": ["comments.user"]}],
            }

        await User.drop_collection()
        await Page.drop_collection()

        u1 = User(username="wilson")
        await u1.save()

        u2 = User(username="rozza")
        await u2.save()

        u3 = User(username="hmarr")
        await u3.save()

        p1 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
                Comment(user=u3, comment="Ping Pong"),
                Comment(user=u1, comment="I like a beer"),
            ]
        )
        await p1.save()

        p2 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
            ]
        )
        await p2.save()

        p3 = Page(comments=[Comment(user=u3, comment="Its very good")])
        await p3.save()

        p4 = Page(comments=[Comment(user=u2, comment="Heavy Metal song")])
        await p4.save()

        assert [p1, p2] == [doc async for doc in Page.objects.filter(comments__user=u1)]
        assert [p1, p2, p4] == [doc async for doc in Page.objects.filter(comments__user=u2)]
        assert [p1, p3] == [doc async for doc in Page.objects.filter(comments__user=u3)]

    async def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.save()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        employee_obj = await collection.find_one({"name": "Test Employee"})
        assert employee_obj["name"] == "Test Employee"
        assert employee_obj["age"] == 50

        # Ensure that the 'details' embedded object saved correctly
        assert employee_obj["details"]["position"] == "Developer"

    async def test_embedded_update_after_save(self):
        """Test update of `EmbeddedDocumentField` attached to a newly
        saved document.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.save()

        # Update
        site.page.log_message = "Error: Dummy message"
        await site.save()

        site = await Site.objects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.save()

        # Test updating an embedded document
        promoted_employee = await Employee.objects.get(name="Test Employee")
        promoted_employee.details.position = "Senior Developer"
        await promoted_employee.save()

        await promoted_employee.reload()
        assert promoted_employee.name == "Test Employee"
        assert promoted_employee.age == 50

        # Ensure that the 'details' embedded object saved correctly
        assert promoted_employee.details.position == "Senior Developer"

        # Test removal
        promoted_employee.details = None
        await promoted_employee.save()

        await promoted_employee.reload()
        assert promoted_employee.details is None

    async def test_embedded_document_equality(self):
        class Test(Document):
            field = StringField(required=True)

        class Embedded(EmbeddedDocument):
            ref = ReferenceField(Test)

        await Test.drop_collection()
        test = await Test(field="123").save()  # has id

        e = Embedded(ref=test)
        f1 = Embedded._from_son(e.to_mongo())
        f2 = Embedded._from_son(e.to_mongo())

        assert f1 == f2
        f1.ref  # Dereferences lazily
        assert f1 == f2

    async def test_embedded_document_equality_with_lazy_ref(self):
        class Job(EmbeddedDocument):
            boss = LazyReferenceField("Person")
            boss_dbref = LazyReferenceField("Person", dbref=True)

        class Person(Document):
            job = EmbeddedDocumentField(Job)

        await Person.drop_collection()

        boss = Person()
        worker = Person(job=Job(boss=boss, boss_dbref=boss))
        await boss.save()
        await worker.save()

        worker1 = await Person.objects.get(id=worker.id)

        # worker1.job should be equal to the job used originally to create the
        # document.
        assert worker1.job == worker.job

        # worker1.job should be equal to a newly created Job EmbeddedDocument
        # using either the Boss object or his ID.
        assert worker1.job == Job(boss=boss, boss_dbref=boss)
        assert worker1.job == Job(boss=boss.id, boss_dbref=boss.id)

        # The above equalities should also hold after worker1.job.boss has been
        # fetch()ed.
        await worker1.job.boss.fetch()
        assert worker1.job == worker.job
        assert worker1.job == Job(boss=boss, boss_dbref=boss)
        assert worker1.job == Job(boss=boss.id, boss_dbref=boss.id)

    async def test_embedded_document_failed_while_loading_instance_when_it_is_not_a_dict(
        self,
    ):
        class LightSaber(EmbeddedDocument):
            color = StringField()

        class Jedi(Document):
            light_saber = EmbeddedDocumentField(LightSaber)

        coll = await Jedi._get_collection()
        await Jedi(light_saber=LightSaber(color="red")).save()
        _ = [doc async for doc in Jedi.objects]  # Ensure a proper document loads without errors

        # Forces a document with a wrong shape (may occur in case of migration)
        value = "I_should_be_a_dict"
        await coll.insert_one({"light_saber": value})

        with pytest.raises(InvalidDocumentError) as exc_info:
            [doc async for doc in Jedi.objects]

        assert (
            str(exc_info.value)
            == f"Invalid data to create a `Jedi` instance.\nField 'light_saber' - The source SON object needs to be of type 'dict' but a '{type(value)}' was found"
        )
