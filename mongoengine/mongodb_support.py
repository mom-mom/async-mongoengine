"""
Helper functions, constants, and types to aid with MongoDB version support
"""

from mongoengine.connection import get_connection

# Constant that can be used to compare the version retrieved with
# get_mongodb_version()
MONGODB_70 = (7, 0)
MONGODB_80 = (8, 0)


async def get_mongodb_version():
    """Return the version of the default connected mongoDB (first 2 digits)

    :return: tuple(int, int)
    """
    info = await get_connection().server_info()
    version_list = info["versionArray"][:2]  # e.g: (3, 2)
    return tuple(version_list)
