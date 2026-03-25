# CLAUDE.md

async-mongoengine: MongoEngine fork with native PyMongo async support (`AsyncMongoClient`).
See [docs/async/README.md](docs/async/README.md) for full API migration reference.

## Requirements

- Python 3.13+, MongoDB 7.0+, PyMongo 4.10+

## Commands

```shell
# Python runtime (always use .venv)
.venv/bin/python

# Run tests
.venv/bin/python -m pytest tests/

# Run specific test
.venv/bin/python -m pytest tests/path/to/test.py -k "test_name"

# Lint
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format .
```
