"""Debug endpoints for cache inspection."""
from __future__ import annotations

import logging
import sys
import traceback

from fastapi import APIRouter

from api.services.cache_helpers import (
    ACTIVE_CACHE,
    GLOBAL_CACHE,
    OLD_CACHE,
    RECENT_CACHE,
    _REVISION_CACHE,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_deep_size(obj, seen=None) -> int:
    """Recursively calculate the deep memory size of an object in bytes."""
    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return 0

    seen.add(obj_id)
    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        size += sum(_get_deep_size(k, seen) + _get_deep_size(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(_get_deep_size(item, seen) for item in obj)
    elif hasattr(obj, "__dict__"):
        size += _get_deep_size(obj.__dict__, seen)
    elif hasattr(obj, "__slots__"):
        size += sum(
            _get_deep_size(getattr(obj, attr, None), seen)
            for attr in obj.__slots__
            if hasattr(obj, attr)
        )

    return size


def _estimate_cache_size(cache) -> int:
    """Estimate memory usage of a cache in bytes using deep size calculation."""
    try:
        seen = set()  # Shared across all entries to avoid double-counting shared references
        total = 0
        for key, entry in cache._entries.items():
            total += _get_deep_size(key, seen)
            total += _get_deep_size(entry, seen)
            total += _get_deep_size(entry.value, seen)
        return total
    except Exception as e:
        logger.error(f"Error estimating cache size: {e}", exc_info=True)
        return 0


@router.get("/cache-stats")
async def cache_stats():
    """Return cache statistics including entry counts and estimated memory usage."""
    caches = {
        "active": ACTIVE_CACHE,
        "recent": RECENT_CACHE,
        "old": OLD_CACHE,
        "global": GLOBAL_CACHE,
        "revision": _REVISION_CACHE,
    }

    entries = {name: len(cache._entries) for name, cache in caches.items()}
    memory_bytes = {name: _estimate_cache_size(cache) for name, cache in caches.items()}
    memory_mb = {name: round(bytes_used / 1024 / 1024, 2) for name, bytes_used in memory_bytes.items()}

    total_entries = sum(entries.values())
    total_bytes = sum(memory_bytes.values())
    total_mb = round(total_bytes / 1024 / 1024, 2)

    return {
        "entries": {**entries, "total": total_entries},
        "memory": {
            "bytes": {**memory_bytes, "total": total_bytes},
            "mb": {**memory_mb, "total": total_mb},
        },
    }
