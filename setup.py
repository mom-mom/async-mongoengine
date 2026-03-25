from setuptools import find_packages, setup

setup(
    name="async-mongoengine",
    version="0.1.0",
    description="Async MongoEngine is a Python Object-Document Mapper for working with MongoDB with async support.",
    license="MIT",
    python_requires=">=3.13",
    install_requires=["pymongo>=4.10"],
    extras_require={
        "signals": [
            "blinker>=1.6",
        ],
        "test": [
            "pytest",
            "pytest-asyncio",
            "pytest-cov",
            "coverage",
            "blinker>=1.6",
        ],
    },
    packages=find_packages(exclude=["tests", "tests.*"]),
)
