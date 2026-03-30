# CLAUDE.md

async-mongoengine: MongoEngine fork with native PyMongo async support (`AsyncMongoClient`).
See [docs/async/README.md](docs/async/README.md) for full API migration reference.

## Language

- All code, comments, commit messages, PR titles/descriptions, and documentation must be written in English — even when the user communicates in another language.

## Requirements

- Python 3.13+, MongoDB 7.0+, PyMongo 4.10+

## Commands

- If `uv run` fails with missing packages, run `uv sync --group dev` first.

```shell
# Run tests
uv run pytest tests/

# Run specific test
uv run pytest tests/path/to/test.py -k "test_name"

# Lint
uv run ruff check .
uv run ruff format .

# Type check
uv run pyright

# Sync dependencies (after changing pyproject.toml)
uv sync --group dev

# Benchmarks
uv run python benchmarks/run_all.py              # pure Python, compare vs main (via git)
uv run python benchmarks/run_all.py --mongodb    # include MongoDB I/O benchmarks
uv run python benchmarks/run_all.py --no-compare # current branch only, no git comparison
```

## Performance Optimization

See [docs/optimizations/README.md](docs/optimizations/README.md) for the
optimization workflow, benchmark conventions, and a list of past attempts.
