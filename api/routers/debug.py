"""Debug endpoints for cache inspection."""
from __future__ import annotations

import hmac
import logging
import os
import sys

from fastapi import APIRouter, Depends, Header, HTTPException

from api.services.cache_helpers import (
    ACTIVE_CACHE,
    GLOBAL_CACHE,
    OLD_CACHE,
    RECENT_CACHE,
    _REVISION_CACHE,
)

logger = logging.getLogger(__name__)


def _require_debug_token(x_debug_token: str | None = Header(default=None, alias="X-Debug-Token")) -> None:
    expected = (os.getenv("DEBUG_API_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Debug routes are not configured")
    provided = (x_debug_token or "").strip()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=403, detail="Forbidden")


router = APIRouter(dependencies=[Depends(_require_debug_token)])


def _format_ttl(seconds: float) -> str:
    """Format seconds into human-readable TTL string."""
    if seconds <= 0:
        return "expired"
    
    seconds = int(seconds)
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


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


def _get_ttl_stats(cache) -> dict:
    """Calculate TTL statistics for cache entries."""
    import time
    
    if not cache._entries:
        return {"min": "0s", "max": "0s", "avg": "0s", "min_seconds": 0, "max_seconds": 0, "avg_seconds": 0}
    
    now = time.monotonic()
    remaining_ttls = [max(0, entry.expires_at - now) for entry in cache._entries.values()]
    
    min_ttl = min(remaining_ttls)
    max_ttl = max(remaining_ttls)
    avg_ttl = sum(remaining_ttls) / len(remaining_ttls)
    
    return {
        "min": _format_ttl(min_ttl),
        "max": _format_ttl(max_ttl),
        "avg": _format_ttl(avg_ttl),
        "min_seconds": round(min_ttl, 1),
        "max_seconds": round(max_ttl, 1),
        "avg_seconds": round(avg_ttl, 1),
    }


def _get_entries_detail(cache) -> list:
    """Get detailed list of cache entries with their remaining TTL."""
    import time
    
    now = time.monotonic()
    details = []
    
    for key, entry in cache._entries.items():
        remaining = max(0, entry.expires_at - now)
        details.append({
            "key": str(key),
            "ttl_remaining": _format_ttl(remaining),
            "ttl_seconds": round(remaining, 1),
        })
    
    # Sort by TTL remaining (ascending)
    details.sort(key=lambda x: x["ttl_seconds"])
    return details


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
    
    # Calculate memory with explicit error handling
    memory_bytes = {}
    for name, cache in caches.items():
        try:
            size = _estimate_cache_size(cache)
            memory_bytes[name] = size
            logger.info(f"Cache {name}: {len(cache._entries)} entries, {size} bytes")
        except Exception as e:
            logger.error(f"Error calculating size for {name}: {e}", exc_info=True)
            memory_bytes[name] = 0
    
    memory_mb = {name: round(bytes_used / 1024 / 1024, 2) for name, bytes_used in memory_bytes.items()}

    total_entries = sum(entries.values())
    total_bytes = sum(memory_bytes.values())
    total_mb = round(total_bytes / 1024 / 1024, 2)

    # Collect hit/miss statistics
    stats = {name: cache.get_stats() for name, cache in caches.items()}
    
    # Get TTL stats
    ttl_stats = {name: _get_ttl_stats(cache) for name, cache in caches.items()}
    
    # Get detailed entries list
    entries_detail = {name: _get_entries_detail(cache) for name, cache in caches.items()}
    
    # Calculate totals
    total_hits = sum(s["hits"] for s in stats.values())
    total_misses = sum(s["misses"] for s in stats.values())
    total_sets = sum(s["sets"] for s in stats.values())
    total_requests = total_hits + total_misses
    total_hit_rate = (total_hits / total_requests * 100) if total_requests > 0 else 0.0

    result = {
        "info": {
            "description": "In-memory cache statistics for Pappaliiga API",
            "tiers": {
                "active": "Current season data (highest priority)",
                "recent": "Recent seasons within RECENT_SEASON_WINDOW",
                "old": "Older seasons beyond recent window",
                "global": "Cross-season aggregates and metadata",
                "revision": "Database revision tokens for cache invalidation",
            },
            "metrics": {
                "entries": "Number of cached items per tier",
                "memory": "Estimated deep memory usage including nested objects",
                "hits": "Successful cache retrievals (served without recalculation)",
                "misses": "Cache lookups requiring recalculation",
                "sets": "Number of times data was written to cache",
                "hit_rate": "Percentage of requests served from cache (hits / (hits + misses))",
            },
            "ttl": {
                "unit": "seconds",
                "min": "Shortest remaining TTL across all entries in tier",
                "max": "Longest remaining TTL across all entries in tier",
                "avg": "Average remaining TTL across all entries in tier",
            },
        },
        "entries": {**entries, "total": total_entries},
        "memory": {
            "bytes": {**memory_bytes, "total": total_bytes},
            "mb": {**memory_mb, "total": total_mb},
        },
        "ttl_remaining_seconds": ttl_stats,
        "stats": {
            **stats,
            "total": {
                "hits": total_hits,
                "misses": total_misses,
                "sets": total_sets,
                "hit_rate": round(total_hit_rate, 2),
            },
        },
        "entries_detail": entries_detail,
    }
    
    return result
