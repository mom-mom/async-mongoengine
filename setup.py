import os

from setuptools import find_packages, setup

DESCRIPTION = "Async MongoEngine is a Python Object-Document Mapper for working with MongoDB with async support."

try:
    with open("README.rst") as fin:
        LONG_DESCRIPTION = fin.read()
except Exception:
    LONG_DESCRIPTION = None


def get_version(version_tuple):
    """Return the version tuple as a string, e.g. for (0, 10, 7),
    return '0.10.7'.
    """
    return ".".join(map(str, version_tuple))


# Dirty hack to get version number from monogengine/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), "mongoengine", "__init__.py")
version_line = list(filter(lambda line: line.startswith("VERSION"), open(init)))[0]

VERSION = get_version(eval(version_line.split("=")[-1]))

CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: Implementation :: CPython",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

install_require = ["pymongo>=4.10"]
tests_require = [
    "pytest",
    "pytest-cov",
    "coverage",
    "blinker",
    "Pillow>=10.0.0",
]

setup(
    name="async-mongoengine",
    version=VERSION,
    author="Harry Marr",
    author_email="harry.marr@gmail.com",
    maintainer="Bastien Gerard",
    maintainer_email="bast.gerard@gmail.com",
    url="http://mongoengine.org/",
    download_url="https://github.com/MongoEngine/mongoengine/tarball/master",
    license="MIT",
    include_package_data=True,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    platforms=["any"],
    classifiers=CLASSIFIERS,
    python_requires=">=3.13",
    install_requires=install_require,
    extras_require={
        "test": tests_require,
    },
    packages=find_packages(exclude=["tests", "tests.*"]),
)
