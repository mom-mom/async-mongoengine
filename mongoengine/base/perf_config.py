"""Performance feature flags.

Usage::

    import mongoengine
    mongoengine.PERF_FLAGS["fast_from_son"] = True
"""

from typing import Any

PERF_FLAGS: dict[str, Any] = {
    # Use an optimised _from_son implementation that bypasses __init__,
    # avoids intermediate dict copies, and performs a single-pass key
    # translation.  ~2x faster for _from_son / from_json.
    "fast_from_son": False,
}
