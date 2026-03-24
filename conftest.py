import pytest


def pytest_collection_modifyitems(items):
    """Automatically add asyncio marker to all async test functions."""
    for item in items:
        if item.get_closest_marker("asyncio") is None:
            if hasattr(item, "function") and hasattr(item.function, "__wrapped__"):
                # Handle decorated functions
                import asyncio

                if asyncio.iscoroutinefunction(item.function.__wrapped__):
                    item.add_marker(pytest.mark.asyncio)
            elif hasattr(item, "function"):
                import asyncio

                if asyncio.iscoroutinefunction(item.function):
                    item.add_marker(pytest.mark.asyncio)
