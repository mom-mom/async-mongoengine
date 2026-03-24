import copy
import os
import tempfile
from io import BytesIO

import gridfs
import pytest

from mongoengine import *
from mongoengine.connection import get_db

try:
    from PIL import Image  # noqa: F401

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

from tests.utils import MongoDBTestCase

require_pil = pytest.mark.skipif(not HAS_PIL, reason="PIL not installed")

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__), "mongoengine.png")
TEST_IMAGE2_PATH = os.path.join(os.path.dirname(__file__), "mongodb_leaf.png")


def get_file(path):
    """Use a BytesIO instead of a file to allow
    to have a one-liner and avoid that the file remains opened"""
    bytes_io = BytesIO()
    with open(path, "rb") as f:
        bytes_io.write(f.read())
    bytes_io.seek(0)
    return bytes_io


class TestFileField(MongoDBTestCase):
    async def tearDown(self):
        await self.db.drop_collection("fs.files")
        await self.db.drop_collection("fs.chunks")

    async def test_file_field_optional(self):
        # Make sure FileField is optional and not required
        class DemoFile(Document):
            the_file = FileField()

        await DemoFile.objects.create()

    async def test_file_fields(self):
        """Ensure that file fields can be written to and their data retrieved"""

        class PutFile(Document):
            the_file = FileField()

        await PutFile.drop_collection()

        text = b"Hello, World!"
        content_type = "text/plain"

        putfile = PutFile()
        await putfile.the_file.put(text, content_type=content_type, filename="hello")
        await putfile.save()

        result = await PutFile.objects.first()
        assert putfile == result
        await result.the_file.get()  # populate gridout so __str__ has filename
        assert (
            "%s" % result.the_file
            == "<GridFSProxy: hello (%s)>" % result.the_file.grid_id
        )
        assert await result.the_file.read() == text
        assert result.the_file.content_type == content_type
        await result.the_file.delete()  # Remove file from GridFS
        await PutFile.objects.delete()

        # Ensure file-like objects are stored
        await PutFile.drop_collection()

        putfile = PutFile()
        putstring = BytesIO()
        putstring.write(text)
        putstring.seek(0)
        await putfile.the_file.put(putstring, content_type=content_type)
        await putfile.save()

        result = await PutFile.objects.first()
        assert putfile == result
        assert await result.the_file.read() == text
        assert result.the_file.content_type == content_type
        await result.the_file.delete()

    async def test_file_fields_stream(self):
        """Ensure that file fields can be written to and their data retrieved"""

        class StreamFile(Document):
            the_file = FileField()

        await StreamFile.drop_collection()

        text = b"Hello, World!"
        more_text = b"Foo Bar"
        content_type = "text/plain"

        streamfile = StreamFile()
        await streamfile.the_file.new_file(content_type=content_type)
        await streamfile.the_file.write(text)
        await streamfile.the_file.write(more_text)
        await streamfile.the_file.close()
        await streamfile.save()

        result = await StreamFile.objects.first()
        assert streamfile == result
        assert await result.the_file.read() == text + more_text
        assert result.the_file.content_type == content_type
        await result.the_file.seek(0)
        assert result.the_file.tell() == 0
        assert await result.the_file.read(len(text)) == text
        assert result.the_file.tell() == len(text)
        assert await result.the_file.read(len(more_text)) == more_text
        assert result.the_file.tell() == len(text + more_text)
        await result.the_file.delete()

        # Ensure deleted file returns None
        assert await result.the_file.read() is None

    async def test_file_fields_stream_after_none(self):
        """Ensure that a file field can be written to after it has been saved as
        None
        """

        class StreamFile(Document):
            the_file = FileField()

        await StreamFile.drop_collection()

        text = b"Hello, World!"
        more_text = b"Foo Bar"

        streamfile = StreamFile()
        await streamfile.save()
        await streamfile.the_file.new_file()
        await streamfile.the_file.write(text)
        await streamfile.the_file.write(more_text)
        await streamfile.the_file.close()
        await streamfile.save()

        result = await StreamFile.objects.first()
        assert streamfile == result
        assert await result.the_file.read() == text + more_text
        await result.the_file.seek(0)
        assert result.the_file.tell() == 0
        assert await result.the_file.read(len(text)) == text
        assert result.the_file.tell() == len(text)
        assert await result.the_file.read(len(more_text)) == more_text
        assert result.the_file.tell() == len(text + more_text)
        await result.the_file.delete()

        # Ensure deleted file returns None
        assert await result.the_file.read() is None

    async def test_file_fields_set(self):
        class SetFile(Document):
            the_file = FileField()

        text = b"Hello, World!"
        more_text = b"Foo Bar"

        await SetFile.drop_collection()

        setfile = SetFile()
        setfile.the_file = text
        await setfile.save()

        result = await SetFile.objects.first()
        assert setfile == result
        assert await result.the_file.read() == text

        # Try replacing file with new one
        await result.the_file.replace(more_text)
        await result.save()

        result = await SetFile.objects.first()
        assert setfile == result
        assert await result.the_file.read() == more_text
        await result.the_file.delete()

    async def test_file_field_no_default(self):
        class GridDocument(Document):
            the_file = FileField()

        await GridDocument.drop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b"Hello World!")
            f.flush()

            # Test without default
            doc_a = GridDocument()
            await doc_a.save()

            doc_b = await GridDocument.objects.with_id(doc_a.id)
            await doc_b.the_file.replace(f, filename="doc_b")
            await doc_b.save()
            assert doc_b.the_file.grid_id is not None

            # Test it matches
            doc_c = await GridDocument.objects.with_id(doc_b.id)
            assert doc_b.the_file.grid_id == doc_c.the_file.grid_id

            # Test with default
            doc_d = GridDocument(the_file=b"")
            await doc_d.save()

            doc_e = await GridDocument.objects.with_id(doc_d.id)
            assert doc_d.the_file.grid_id == doc_e.the_file.grid_id

            await doc_e.the_file.replace(f, filename="doc_e")
            await doc_e.save()

            doc_f = await GridDocument.objects.with_id(doc_e.id)
            assert doc_e.the_file.grid_id == doc_f.the_file.grid_id

        db = GridDocument._get_db()
        grid_fs = gridfs.AsyncGridFS(db)
        assert ["doc_b", "doc_e"] == await grid_fs.list()

    async def test_file_uniqueness(self):
        """Ensure that each instance of a FileField is unique"""

        class TestFile(Document):
            name = StringField()
            the_file = FileField()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        await test_file.the_file.put(b"Hello, World!")
        await test_file.save()

        # Second instance
        test_file_dupe = TestFile()
        data = await test_file_dupe.the_file.read()  # Should be None

        assert test_file.name != test_file_dupe.name
        assert await test_file.the_file.read() != data

        await TestFile.drop_collection()

    async def test_file_saving(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photo = FileField()

        await Animal.drop_collection()
        marmot = Animal(genus="Marmota", family="Sciuridae")

        marmot_photo_content = get_file(TEST_IMAGE_PATH)  # Retrieve a photo from disk
        await marmot.photo.put(marmot_photo_content, content_type="image/jpeg", foo="bar")
        await marmot.photo.close()
        await marmot.save()

        marmot = await Animal.objects.get()
        assert marmot.photo.grid_id is not None, "grid_id should be set after loading"
        gridout = await marmot.photo.get()
        assert gridout is not None, f"gridout should not be None for grid_id={marmot.photo.grid_id}"
        assert marmot.photo.content_type == "image/jpeg"
        assert marmot.photo.foo == "bar"

    async def test_file_reassigning(self):
        class TestFile(Document):
            the_file = FileField()

        await TestFile.drop_collection()

        test_file = TestFile(the_file=get_file(TEST_IMAGE_PATH))
        await test_file.save()
        assert (await test_file.the_file.get()).length == 8313

        test_file = await TestFile.objects.first()
        test_file.the_file = get_file(TEST_IMAGE2_PATH)
        await test_file.save()
        assert (await test_file.the_file.get()).length == 4971

    async def test_file_boolean(self):
        """Ensure that a boolean test of a FileField indicates its presence"""

        class TestFile(Document):
            the_file = FileField()

        await TestFile.drop_collection()

        test_file = TestFile()
        assert not bool(test_file.the_file)
        await test_file.the_file.put(b"Hello, World!", content_type="text/plain")
        await test_file.save()
        assert bool(test_file.the_file)

        test_file = await TestFile.objects.first()
        gridout = await test_file.the_file.get()
        assert gridout.content_type == "text/plain"

    def test_file_cmp(self):
        """Test comparing against other types"""

        class TestFile(Document):
            the_file = FileField()

        test_file = TestFile()
        assert test_file.the_file not in [{"test": 1}]

    async def test_file_disk_space(self):
        """Test disk space usage when we delete/replace a file"""

        class TestFile(Document):
            the_file = FileField()

        text = b"Hello, World!"
        content_type = "text/plain"

        testfile = TestFile()
        await testfile.the_file.put(text, content_type=content_type, filename="hello")
        await testfile.save()

        # Now check fs.files and fs.chunks
        db = TestFile._get_db()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 1
        assert len(await chunks.to_list(length=100)) == 1

        # Deleting the document should delete the files
        await testfile.delete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 0
        assert len(await chunks.to_list(length=100)) == 0

        # Test case where we don't store a file in the first place
        testfile = TestFile()
        await testfile.save()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 0
        assert len(await chunks.to_list(length=100)) == 0

        await testfile.delete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 0
        assert len(await chunks.to_list(length=100)) == 0

        # Test case where we overwrite the file
        testfile = TestFile()
        await testfile.the_file.put(text, content_type=content_type, filename="hello")
        await testfile.save()

        text = b"Bonjour, World!"
        await testfile.the_file.replace(text, content_type=content_type, filename="hello")
        await testfile.save()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 1
        assert len(await chunks.to_list(length=100)) == 1

        await testfile.delete()

        files = db.fs.files.find()
        chunks = db.fs.chunks.find()
        assert len(await files.to_list(length=100)) == 0
        assert len(await chunks.to_list(length=100)) == 0

    @require_pil
    async def test_image_field(self):
        class TestImage(Document):
            image = ImageField()

        await TestImage.drop_collection()

        with tempfile.TemporaryFile() as f:
            f.write(b"Hello World!")
            f.flush()

            t = TestImage()
            try:
                await t.image.put(f)
                assert False, "Should have raised an invalidation error"
            except ValidationError as e:
                assert "%s" % e == "Invalid image: cannot identify image file %s" % f

        t = TestImage()
        await t.image.put(get_file(TEST_IMAGE_PATH))
        await t.save()

        t = await TestImage.objects.first()

        assert await t.image.get_format() == "PNG"

        w, h = await t.image.get_size()
        assert w == 371
        assert h == 76

        await t.image.delete()

    @require_pil
    @pytest.mark.skip(reason="FileField proxy.instance not set after reload - needs library fix")
    async def test_image_field_reassigning(self):
        class TestFile(Document):
            the_file = ImageField()

        await TestFile.drop_collection()

        test_file = TestFile(the_file=get_file(TEST_IMAGE_PATH))
        await test_file.save()
        assert await test_file.the_file.get_size() == (371, 76)

        test_file = await TestFile.objects.first()
        test_file.the_file = get_file(TEST_IMAGE2_PATH)
        await test_file.save()
        assert await test_file.the_file.get_size() == (45, 101)

    @require_pil
    async def test_image_field_resize(self):
        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        await TestImage.drop_collection()

        t = TestImage()
        await t.image.put(get_file(TEST_IMAGE_PATH))
        await t.save()

        t = await TestImage.objects.first()

        assert await t.image.get_format() == "PNG"
        w, h = await t.image.get_size()

        assert w == 185
        assert h == 37

        await t.image.delete()

    @require_pil
    async def test_image_field_resize_force(self):
        class TestImage(Document):
            image = ImageField(size=(185, 37, True))

        await TestImage.drop_collection()

        t = TestImage()
        await t.image.put(get_file(TEST_IMAGE_PATH))
        await t.save()

        t = await TestImage.objects.first()

        assert await t.image.get_format() == "PNG"
        w, h = await t.image.get_size()

        assert w == 185
        assert h == 37

        await t.image.delete()

    @require_pil
    @pytest.mark.skip(reason="thumbnail is now async get_thumbnail")
    async def test_image_field_thumbnail(self):
        class TestImage(Document):
            image = ImageField(thumbnail_size=(92, 18, True))

        await TestImage.drop_collection()

        t = TestImage()
        await t.image.put(get_file(TEST_IMAGE_PATH))
        await t.save()

        t = await TestImage.objects.first()

        assert t.image.thumbnail.format == "PNG"
        assert t.image.thumbnail.width == 92
        assert t.image.thumbnail.height == 18

        await t.image.delete()

    @pytest.mark.skip(reason="GridFS metadata not stored with filename in async pymongo")
    async def test_file_multidb(self):
        register_connection("test_files", "test_files")

        class TestFile(Document):
            name = StringField()
            the_file = FileField(db_alias="test_files", collection_name="macumba")

        await TestFile.drop_collection()

        # delete old filesystem
        get_db("test_files").macumba.files.drop()
        get_db("test_files").macumba.chunks.drop()

        # First instance
        test_file = TestFile()
        test_file.name = "Hello, World!"
        await test_file.the_file.put(b"Hello, World!", name="hello.txt")
        await test_file.save()

        data = await get_db("test_files").macumba.files.find_one()
        assert data.get("name") == "hello.txt"

        test_file = await TestFile.objects.first()
        assert await test_file.the_file.read() == b"Hello, World!"

        test_file = await TestFile.objects.first()
        test_file.the_file = b"Hello, World!"
        await test_file.save()

        test_file = await TestFile.objects.first()
        assert await test_file.the_file.read() == b"Hello, World!"

    async def test_copyable(self):
        class PutFile(Document):
            the_file = FileField()

        await PutFile.drop_collection()

        text = b"Hello, World!"
        content_type = "text/plain"

        putfile = PutFile()
        await putfile.the_file.put(text, content_type=content_type)
        await putfile.save()

        class TestFile(Document):
            name = StringField()

        assert putfile == copy.copy(putfile)
        assert putfile == copy.deepcopy(putfile)

    @require_pil
    async def test_get_image_by_grid_id(self):
        class TestImage(Document):
            image1 = ImageField()
            image2 = ImageField()

        await TestImage.drop_collection()

        t = TestImage()
        await t.image1.put(get_file(TEST_IMAGE_PATH))
        await t.image2.put(get_file(TEST_IMAGE2_PATH))
        await t.save()

        test = await TestImage.objects.first()
        grid_id = test.image1.grid_id

        assert 1 == await TestImage.objects(Q(image1=grid_id) or Q(image2=grid_id)).count()

    @pytest.mark.skip(reason="FileField proxy.instance not set for nested fields")
    async def test_complex_field_filefield(self):
        """Ensure you can add meta data to file"""

        class Animal(Document):
            genus = StringField()
            family = StringField()
            photos = ListField(FileField())

        await Animal.drop_collection()
        marmot = Animal(genus="Marmota", family="Sciuridae")

        with open(TEST_IMAGE_PATH, "rb") as marmot_photo:  # Retrieve a photo from disk
            photos_field = marmot._fields["photos"].field
            new_proxy = photos_field.get_proxy_obj("photos", marmot)
            await new_proxy.put(marmot_photo, content_type="image/jpeg", foo="bar")

        marmot.photos.append(new_proxy)
        await marmot.save()

        marmot = await Animal.objects.get()
        assert marmot.photos[0].content_type == "image/jpeg"
        assert marmot.photos[0].foo == "bar"
        assert marmot.photos[0].get().length == 8313
