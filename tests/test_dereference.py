from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine.context_managers import query_counter
from tests.utils import MongoDBTestCase


class TestDereference(MongoDBTestCase):
    async def test_list_item_dereference(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 51):
            user = User(name=f"user {i}")
            await user.save()

        group = Group(members=[u async for u in User.objects])
        await group.save()

        group = Group(members=[u async for u in User.objects])
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            len(group_obj._data["members"])
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            len(group_obj.members)
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 2
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0
            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0
            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

        await User.drop_collection()
        await Group.drop_collection()

    async def test_list_item_dereference_dref_false(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 51):
            user = User(name=f"user {i}")
            await user.save()

        group = Group(members=[u async for u in User.objects])
        await group.save()
        await group.reload()  # Confirm reload works

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            # verifies that no additional queries gets executed
            # if we re-iterate over the ListField
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()

            assert await q.get_count() == 2
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0
            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0
            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

    async def test_list_item_dereference_orphan_dbref(self):
        """Ensure that orphan DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 51):
            user = User(name=f"user {i}")
            await user.save()

        group = Group(members=[u async for u in User.objects])
        await group.save()
        await group.reload()  # Confirm reload works

        # Delete one User so one of the references in the
        # Group.members list is an orphan DBRef
        await (await User.objects.first()).delete()
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            # verifies that no additional queries gets executed
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        await User.drop_collection()
        await Group.drop_collection()

    async def test_list_item_dereference_dref_false_stores_as_type(self):
        """Ensure that DBRef items are stored as their type"""

        class User(Document):
            my_id = IntField(primary_key=True)
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.drop_collection()
        await Group.drop_collection()

        user = await User(my_id=1, name="user 1").save()

        await Group(members=[u async for u in User.objects]).save()
        group = await Group.objects.first()

        assert (await (await Group._get_collection()).find_one())["members"] == [1]
        assert group.members == [user]

    async def test_handle_old_style_references(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=True))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 26):
            user = User(name=f"user {i}")
            await user.save()

        group = Group(members=[u async for u in User.objects])
        await group.save()

        group = await (await Group._get_collection()).find_one()

        # Update the model to change the reference
        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        group = await Group.objects.first()
        await group.select_related()
        group.members.append(await User(name="String!").save())
        await group.save()

        group = await Group.objects.first()
        await group.select_related()
        assert group.members[0].name == "user 1"
        assert group.members[-1].name == "String!"

    async def test_migrate_references(self):
        """Example of migrating ReferenceField storage"""

        # Create some sample data
        class User(Document):
            name = StringField()

        class Group(Document):
            author = ReferenceField(User, dbref=True)
            members = ListField(ReferenceField(User, dbref=True))

        await User.drop_collection()
        await Group.drop_collection()

        user = await User(name="Ross").save()
        group = await Group(author=user, members=[user]).save()

        raw_data = await (await Group._get_collection()).find_one()
        assert isinstance(raw_data["author"], DBRef)
        assert isinstance(raw_data["members"][0], DBRef)
        group = await Group.objects.first()

        assert group.author == user
        assert group.members == [user]

        # Migrate the model definition
        class Group(Document):
            author = ReferenceField(User, dbref=False)
            members = ListField(ReferenceField(User, dbref=False))

        # Migrate the data
        async for g in Group.objects():
            # Explicitly mark as changed so resets
            g._mark_as_changed("author")
            g._mark_as_changed("members")
            await g.save()

        group = await Group.objects.first()
        assert group.author == user
        assert group.members == [user]

        raw_data = await (await Group._get_collection()).find_one()
        assert isinstance(raw_data["author"], ObjectId)
        assert isinstance(raw_data["members"][0], ObjectId)

    async def test_recursive_reference(self):
        """Ensure that ReferenceFields can reference their own documents."""

        class Employee(Document):
            name = StringField()
            boss = ReferenceField("self")
            friends = ListField(ReferenceField("self"))

        await Employee.drop_collection()

        bill = Employee(name="Bill Lumbergh")
        await bill.save()

        michael = Employee(name="Michael Bolton")
        await michael.save()

        samir = Employee(name="Samir Nagheenanajar")
        await samir.save()

        friends = [michael, samir]
        peter = Employee(name="Peter Gibbons", boss=bill, friends=friends)
        await peter.save()

        await Employee(name="Funky Gibbon", boss=bill, friends=friends).save()
        await Employee(name="Funky Gibbon", boss=bill, friends=friends).save()
        await Employee(name="Funky Gibbon", boss=bill, friends=friends).save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            peter = await Employee.objects.with_id(peter.id)
            assert await q.get_count() == 1

            # No auto-dereference — accessing boss/friends does not trigger extra queries
            peter.boss
            assert await q.get_count() == 1

            peter.friends
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            peter = await Employee.objects.select_related().with_id(peter.id)
            assert await q.get_count() == 2

            assert peter.boss == bill
            assert await q.get_count() == 2

            assert peter.friends == friends
            assert await q.get_count() == 2

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            employees = Employee.objects(boss=bill).select_related()
            assert await q.get_count() == 0

            async for employee in employees:
                assert employee.boss == bill
                assert employee.friends == friends
            assert await q.get_count() == 2

    async def test_list_of_lists_of_references(self):
        class User(Document):
            name = StringField()

        class Post(Document):
            user_lists = ListField(ListField(ReferenceField(User)))

        class SimpleList(Document):
            users = ListField(ReferenceField(User))

        await User.drop_collection()
        await Post.drop_collection()
        await SimpleList.drop_collection()

        u1 = await User.objects.create(name="u1")
        u2 = await User.objects.create(name="u2")
        u3 = await User.objects.create(name="u3")

        await SimpleList.objects.create(users=[u1, u2, u3])
        assert (await SimpleList.objects.all().first()).users == [u1, u2, u3]

        await Post.objects.create(user_lists=[[u1, u2], [u3]])
        assert (await Post.objects.all().first()).user_lists == [[u1, u2], [u3]]

    async def test_circular_reference(self):
        """Ensure you can handle circular references"""

        class Relation(EmbeddedDocument):
            name = StringField()
            person = ReferenceField("Person")

        class Person(Document):
            name = StringField()
            relations = ListField(EmbeddedDocumentField("Relation"))

            def __repr__(self):
                return f"<Person: {self.name}>"

        await Person.drop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        await mother.save()
        await daughter.save()

        daughter_rel = Relation(name="Daughter", person=daughter)
        mother.relations.append(daughter_rel)
        await mother.save()

        mother_rel = Relation(name="Daughter", person=mother)
        self_rel = Relation(name="Self", person=daughter)
        daughter.relations.append(mother_rel)
        daughter.relations.append(self_rel)
        await daughter.save()

        assert "[<Person: Mother>, <Person: Daughter>]" == "%s" % [doc async for doc in Person.objects()]

    async def test_circular_reference_on_self(self):
        """Ensure you can handle circular references"""

        class Person(Document):
            name = StringField()
            relations = ListField(ReferenceField("self"))

            def __repr__(self):
                return f"<Person: {self.name}>"

        await Person.drop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        await mother.save()
        await daughter.save()

        mother.relations.append(daughter)
        await mother.save()

        daughter.relations.append(mother)
        daughter.relations.append(daughter)
        assert daughter._get_changed_fields() == ["relations"]
        await daughter.save()

        assert "[<Person: Mother>, <Person: Daughter>]" == "%s" % [doc async for doc in Person.objects()]

    async def test_circular_tree_reference(self):
        """Ensure you can handle circular references with more than one level"""

        class Other(EmbeddedDocument):
            name = StringField()
            friends = ListField(ReferenceField("Person"))

        class Person(Document):
            name = StringField()
            other = EmbeddedDocumentField(Other, default=lambda: Other())

            def __repr__(self):
                return f"<Person: {self.name}>"

        await Person.drop_collection()
        paul = await Person(name="Paul").save()
        maria = await Person(name="Maria").save()
        julia = await Person(name="Julia").save()
        anna = await Person(name="Anna").save()

        paul.other.friends = [maria, julia, anna]
        paul.other.name = "Paul's friends"
        await paul.save()

        maria.other.friends = [paul, julia, anna]
        maria.other.name = "Maria's friends"
        await maria.save()

        julia.other.friends = [paul, maria, anna]
        julia.other.name = "Julia's friends"
        await julia.save()

        anna.other.friends = [paul, maria, julia]
        anna.other.name = "Anna's friends"
        await anna.save()

        assert "[<Person: Paul>, <Person: Maria>, <Person: Julia>, <Person: Anna>]" == "%s" % [
            doc async for doc in Person.objects()
        ]

    async def test_generic_reference(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(GenericReferenceField())

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            b = UserB(name=f"User B {i}")
            await b.save()

            c = UserC(name=f"User C {i}")
            await c.save()

            members += [a, b, c]

        group = Group(members=members)
        await group.save()

        group = Group(members=members)
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            for m in group_obj.members:
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                _ = [m for m in group_obj.members]

                for m in group_obj.members:
                    assert "User" in m.__class__.__name__
            assert await q.get_count() == 4

    async def test_generic_reference_orphan_dbref(self):
        """Ensure that generic orphan DBRef items in ListFields are dereferenced."""

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(GenericReferenceField())

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            b = UserB(name=f"User B {i}")
            await b.save()

            c = UserC(name=f"User C {i}")
            await c.save()

            members += [a, b, c]

        group = Group(members=members)
        await group.save()

        # Delete one UserA instance so that there is
        # an orphan DBRef in the GenericReference ListField
        await (await UserA.objects.first()).delete()
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

    async def test_list_field_complex(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField()

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            b = UserB(name=f"User B {i}")
            await b.save()

            c = UserC(name=f"User C {i}")
            await c.save()

            members += [a, b, c]

        group = Group(members=members)
        await group.save()

        group = Group(members=members)
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            for m in group_obj.members:
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                _ = [m for m in group_obj.members]

                for m in group_obj.members:
                    assert "User" in m.__class__.__name__
            assert await q.get_count() == 4

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

    async def test_map_field_reference(self):
        class User(Document):
            name = StringField()

        class Group(Document):
            members = MapField(ReferenceField(User))

        await User.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            user = User(name=f"user {i}")
            await user.save()
            members.append(user)

        group = Group(members={str(u.id): u for u in members})
        await group.save()

        group = Group(members={str(u.id): u for u in members})
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 2

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

            for k, m in group_obj.members.items():
                assert isinstance(m, User)

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                for k, m in group_obj.members.items():
                    assert isinstance(m, User)
            assert await q.get_count() == 2

        await User.drop_collection()
        await Group.drop_collection()

    async def test_dict_field(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = DictField()

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            b = UserB(name=f"User B {i}")
            await b.save()

            c = UserC(name=f"User C {i}")
            await c.save()

            members += [a, b, c]

        group = Group(members={str(u.id): u for u in members})
        await group.save()
        group = Group(members={str(u.id): u for u in members})
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            for k, m in group_obj.members.items():
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                _ = [m for m in group_obj.members]

                for k, m in group_obj.members.items():
                    assert "User" in m.__class__.__name__
            assert await q.get_count() == 4

        await Group.objects.delete()
        await Group().save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1
            assert group_obj.members == {}

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

    async def test_dict_field_no_field_inheritance(self):
        class UserA(Document):
            name = StringField()
            meta = {"allow_inheritance": False}

        class Group(Document):
            members = DictField()

        await UserA.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            members += [a]

        group = Group(members={str(u.id): u for u in members})
        await group.save()

        group = Group(members={str(u.id): u for u in members})
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 2

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 2

            for k, m in group_obj.members.items():
                assert isinstance(m, UserA)

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                _ = [m for m in group_obj.members]

                for _, m in group_obj.members.items():
                    assert isinstance(m, UserA)
            assert await q.get_count() == 2

        await UserA.drop_collection()
        await Group.drop_collection()

    async def test_generic_reference_map_field(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = MapField(GenericReferenceField())

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name=f"User A {i}")
            await a.save()

            b = UserB(name=f"User B {i}")
            await b.save()

            c = UserC(name=f"User C {i}")
            await c.save()

            members += [a, b, c]

        group = Group(members={str(u.id): u for u in members})
        await group.save()
        group = Group(members={str(u.id): u for u in members})
        await group.save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            # No auto-dereference — accessing members does not trigger extra queries
            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        # Document select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.select_related().first()
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 4

            for _, m in group_obj.members.items():
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with query_counter() as q:
            assert await q.get_count() == 0

            group_objs = Group.objects.select_related()
            assert await q.get_count() == 0

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]

                _ = [m for m in group_obj.members]

                for _, m in group_obj.members.items():
                    assert "User" in m.__class__.__name__
            assert await q.get_count() == 4

        await Group.objects.delete()
        await Group().save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            _ = [m for m in group_obj.members]
            assert await q.get_count() == 1

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

    async def test_multidirectional_lists(self):
        class Asset(Document):
            name = StringField(max_length=250, required=True)
            path = StringField()
            title = StringField()
            parent = GenericReferenceField(default=None)
            parents = ListField(GenericReferenceField())
            children = ListField(GenericReferenceField())

        await Asset.drop_collection()

        root = Asset(name="", path="/", title="Site Root")
        await root.save()

        company = Asset(name="company", title="Company", parent=root, parents=[root])
        await company.save()

        root.children = [company]
        await root.save()

        root = await root.reload()
        await root.select_related()
        assert root.children == [company]
        assert company.parents == [root]

    async def test_dict_in_dbref_instance(self):
        class Person(Document):
            name = StringField(max_length=250, required=True)

        class Room(Document):
            number = StringField(max_length=250, required=True)
            staffs_with_position = ListField(DictField())

        await Person.drop_collection()
        await Room.drop_collection()

        bob = await Person.objects.create(name="Bob")
        await bob.save()
        sarah = await Person.objects.create(name="Sarah")
        await sarah.save()

        room_101 = await Room.objects.create(number="101")
        room_101.staffs_with_position = [
            {"position_key": "window", "staff": sarah},
            {"position_key": "door", "staff": bob.to_dbref()},
        ]
        await room_101.save()

        room = await Room.objects.select_related().first()
        assert room.staffs_with_position[0]["staff"] == sarah
        assert room.staffs_with_position[1]["staff"] == bob

    async def test_document_reload_no_inheritance(self):
        class Foo(Document):
            meta = {"allow_inheritance": False}
            bar = ReferenceField("Bar")
            baz = ReferenceField("Baz")

        class Bar(Document):
            meta = {"allow_inheritance": False}
            msg = StringField(required=True, default="Blammo!")

        class Baz(Document):
            meta = {"allow_inheritance": False}
            msg = StringField(required=True, default="Kaboom!")

        await Foo.drop_collection()
        await Bar.drop_collection()
        await Baz.drop_collection()

        bar = Bar()
        await bar.save()
        baz = Baz()
        await baz.save()
        foo = Foo()
        foo.bar = bar
        foo.baz = baz
        await foo.save()
        await foo.reload()
        await foo.select_related()

        assert isinstance(foo.bar, Bar)
        assert isinstance(foo.baz, Baz)

    async def test_document_reload_reference_integrity(self):
        """
        Ensure reloading a document with multiple similar id
        in different collections doesn't mix them.
        """

        class Topic(Document):
            id = IntField(primary_key=True)

        class User(Document):
            id = IntField(primary_key=True)
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            topic = ReferenceField(Topic)
            author = ReferenceField(User)

        await Topic.drop_collection()
        await User.drop_collection()
        await Message.drop_collection()

        # All objects share the same id, but each in a different collection
        topic = await Topic(id=1).save()
        user = await User(id=1, name="user-name").save()
        await Message(id=1, topic=topic, author=user).save()

        concurrent_change_user = await User.objects.get(id=1)
        concurrent_change_user.name = "new-name"
        await concurrent_change_user.save()
        assert user.name != "new-name"

        msg = await Message.objects.get(id=1)
        await msg.reload()
        await msg.select_related()
        assert msg.topic == topic
        assert msg.author == user
        assert msg.author.name == "new-name"

    async def test_list_lookup_not_checked_in_map(self):
        """Ensure we dereference list data correctly"""

        class Comment(Document):
            id = IntField(primary_key=True)
            text = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            comments = ListField(ReferenceField(Comment))

        await Comment.drop_collection()
        await Message.drop_collection()

        c1 = await Comment(id=0, text="zero").save()
        c2 = await Comment(id=1, text="one").save()
        await Message(id=1, comments=[c1, c2]).save()

        msg = await Message.objects.get(id=1)
        await msg.select_related()
        assert 0 == msg.comments[0].id
        assert 1 == msg.comments[1].id

    async def test_list_item_dereference_dref_false_save_doesnt_cause_extra_queries(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(ReferenceField(User, dbref=False))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 51):
            await User(name=f"user {i}").save()

        await Group(name="Test", members=[u async for u in User.objects]).save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            group_obj.name = "new test"
            await group_obj.save()

            assert await q.get_count() == 2

    async def test_list_item_dereference_dref_true_save_doesnt_cause_extra_queries(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(ReferenceField(User, dbref=True))

        await User.drop_collection()
        await Group.drop_collection()

        for i in range(1, 51):
            await User(name=f"user {i}").save()

        await Group(name="Test", members=[u async for u in User.objects]).save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            group_obj.name = "new test"
            await group_obj.save()

            assert await q.get_count() == 2

    async def test_generic_reference_save_doesnt_cause_extra_queries(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(GenericReferenceField())

        await UserA.drop_collection()
        await UserB.drop_collection()
        await UserC.drop_collection()
        await Group.drop_collection()

        members = []
        for i in range(1, 51):
            a = await UserA(name=f"User A {i}").save()
            b = await UserB(name=f"User B {i}").save()
            c = await UserC(name=f"User C {i}").save()

            members += [a, b, c]

        await Group(name="test", members=members).save()

        async with query_counter() as q:
            assert await q.get_count() == 0

            group_obj = await Group.objects.first()
            assert await q.get_count() == 1

            group_obj.name = "new test"
            await group_obj.save()

            assert await q.get_count() == 2

    async def test_objectid_reference_across_databases(self):
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        register_connection("testdb-1", "mongoenginetest2")

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)

        # Drops
        await User.drop_collection()
        await Book.drop_collection()

        user = await User(name="Ross").save()
        await Book(name="MongoEngine for pros", author=user).save()

        # Can't use query_counter across databases - so test the _data object
        book = await Book.objects.first()
        assert not isinstance(book._data["author"], User)

        await book.select_related()
        assert isinstance(book._data["author"], User)

    async def test_non_ascii_pk(self):
        """
        Ensure that dbref conversion to string does not fail when
        non-ascii characters are used in primary key
        """

        class Brand(Document):
            title = StringField(max_length=255, primary_key=True)

        class BrandGroup(Document):
            title = StringField(max_length=255, primary_key=True)
            brands = ListField(ReferenceField("Brand", dbref=True))

        await Brand.drop_collection()
        await BrandGroup.drop_collection()

        brand1 = await Brand(title="Moschino").save()
        brand2 = await Brand(title="Денис Симачёв").save()

        await BrandGroup(title="top_brands", brands=[brand1, brand2]).save()
        brand_groups = BrandGroup.objects().all()

        assert 2 == len([brand async for bg in brand_groups for brand in bg.brands])

    async def test_dereferencing_embedded_listfield_referencefield(self):
        class Tag(Document):
            meta = {"collection": "tags"}
            name = StringField()

        class Post(EmbeddedDocument):
            body = StringField()
            tags = ListField(ReferenceField("Tag", dbref=True))

        class Page(Document):
            meta = {"collection": "pages"}
            tags = ListField(ReferenceField("Tag", dbref=True))
            posts = ListField(EmbeddedDocumentField(Post))

        await Tag.drop_collection()
        await Page.drop_collection()

        tag = await Tag(name="test").save()
        post = Post(body="test body", tags=[tag])
        await Page(tags=[tag], posts=[post]).save()

        page = await Page.objects.first()
        assert page.tags[0] == page.posts[0].tags[0]

    async def test_select_related_follows_embedded_referencefields(self):
        class Song(Document):
            title = StringField()

        class PlaylistItem(EmbeddedDocument):
            song = ReferenceField("Song")

        class Playlist(Document):
            items = ListField(EmbeddedDocumentField("PlaylistItem"))

        await Playlist.drop_collection()
        await Song.drop_collection()

        songs = [await Song.objects.create(title=f"song {i}") for i in range(3)]
        items = [PlaylistItem(song=song) for song in songs]
        playlist = await Playlist.objects.create(items=items)

        async with query_counter() as q:
            assert await q.get_count() == 0

            playlist = await Playlist.objects.select_related().first()
            songs = [item.song for item in playlist.items]

            assert await q.get_count() == 2

    async def test_select_related_with_get(self):
        """Ensure select_related works with get()."""

        class User(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            owner = ReferenceField(User)

        await User.drop_collection()
        await Group.drop_collection()

        user = await User(name="Alice").save()
        group = await Group(name="Admins", owner=user).save()

        # select_related().get() should dereference
        fetched = await Group.objects.select_related().get(id=group.id)
        assert isinstance(fetched.owner, User)
        assert fetched.owner.name == "Alice"

        # select_related().to_list() should dereference
        groups = await Group.objects.select_related().to_list()
        assert len(groups) == 1
        assert isinstance(groups[0].owner, User)
        assert groups[0].owner.name == "Alice"
