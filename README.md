# async-mongoengine

> [!WARNING]
> This project is a fork of [MongoEngine](https://github.com/MongoEngine/mongoengine)
> with native PyMongo async support (`AsyncMongoClient`). It does **not** use Motor.
>
> **Important limitations:**
> - Requires **Python 3.13+** and **MongoDB 7.0+**
> - `FileField` and `ImageField` (GridFS) are **not supported**
> - Auto-dereference for `ReferenceField` is **not supported**
>
> This project was written with the assistance of Claude (Anthropic). While it is
> publicly available, it was primarily built for internal use. **No guarantees are
> made regarding stability, backward compatibility, or long-term maintenance.**

async-mongoengine is a Python Object-Document Mapper for working with MongoDB
using native async/await. It is based on MongoEngine and uses PyMongo's built-in
`AsyncMongoClient` — no Motor dependency required.

## Supported Versions

- **Python**: 3.13+
- **MongoDB**: 7.0+
- **PyMongo**: 4.10+

## Installation

```shell
# pip
pip install git+https://github.com/mom-mom/async-mongoengine.git

# uv
uv pip install git+https://github.com/mom-mom/async-mongoengine.git

# uv (add to project dependencies)
uv add git+https://github.com/mom-mom/async-mongoengine.git
```

## Usage

```python
# Import — the package name is "mongoengine", not "async-mongoengine"
from mongoengine import connect, Document, StringField
```

## Dependencies

- `pymongo>=4.10`

Optional:

- `dateutil>=2.1.0` (for flexible `DateTimeField` parsing)
- `blinker>=1.6` (for signals support — `send_async` requires 1.6+)

```shell
# Install with signals support
pip install "async-mongoengine[signals] @ git+https://github.com/mom-mom/async-mongoengine.git"
uv pip install "async-mongoengine[signals] @ git+https://github.com/mom-mom/async-mongoengine.git"
```

## Type Hints

This package ships with `py.typed` and comprehensive type annotations.
`QuerySet` is generic, so type checkers (pyright, mypy) can infer document
types from query results:

```python
user = await User.objects.first()       # User | None
user = await User.objects.get(name="x") # User
async for u in User.objects:            # User
```

For better field-level inference, add inline annotations to your models:

```python
class User(Document):
    name: str = StringField(required=True)
    age: int | None = IntField()
```

## Examples

```python
import asyncio
import datetime
from mongoengine import *

connect('mydb')

class BlogPost(Document):
    title = StringField(required=True, max_length=200)
    posted = DateTimeField(default=lambda: datetime.datetime.now(datetime.timezone.utc))
    tags = ListField(StringField(max_length=50))
    meta = {'allow_inheritance': True}

class TextPost(BlogPost):
    content = StringField(required=True)

class LinkPost(BlogPost):
    url = StringField(required=True)

async def main():
    # Create a text-based post
    post1 = TextPost(title='Using async-mongoengine', content='See the tutorial')
    post1.tags = ['mongodb', 'mongoengine']
    await post1.save()

    # Create a link-based post
    post2 = LinkPost(title='async-mongoengine Docs', url='https://github.com/mom-mom/async-mongoengine')
    post2.tags = ['mongoengine', 'documentation']
    await post2.save()

    # Iterate over all posts using the BlogPost superclass
    async for post in BlogPost.objects:
        print('===', post.title, '===')
        if isinstance(post, TextPost):
            print(post.content)
        elif isinstance(post, LinkPost):
            print('Link:', post.url)

    # Count all blog posts and its subtypes
    print(await BlogPost.objects.count())   # 2
    print(await TextPost.objects.count())   # 1
    print(await LinkPost.objects.count())   # 1

    # Count tagged posts
    print(await BlogPost.objects(tags='mongoengine').count())  # 2
    print(await BlogPost.objects(tags='mongodb').count())      # 1

asyncio.run(main())
```

## Development

```shell
# Clone and set up
git clone https://github.com/mom-mom/async-mongoengine.git
cd async-mongoengine
uv sync --group dev

# Run tests (requires local MongoDB)
uv run pytest tests/

# Lint
uv run ruff check .
uv run ruff format .
```

## Credits

This project is based on [MongoEngine](https://github.com/MongoEngine/mongoengine),
originally created by Harry Marr and maintained by Bastien Gerard.
