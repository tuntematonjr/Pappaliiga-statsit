"""Operational debug endpoints for cache/sync visibility."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from api.services import cache_helpers, matches_service
from api.services.sync_event_queue import get_sync_event_queue

router = APIRouter()


def _iso_utc(epoch_seconds: float | int | None) -> str | None:
    if not epoch_seconds:
        return None
    try:
        return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).isoformat()
    except Exception:
        return None


async def _cache_snapshot(cache: Any) -> dict[str, Any]:
    return {
        "size": await cache.size(),
        "stats": cache.get_stats(),
    }


@router.get("/status")
async def debug_status() -> dict[str, Any]:
    queue = get_sync_event_queue()
    queue_stats = queue.stats()

    global_revision = await cache_helpers.get_global_revision()
    active_cache = await _cache_snapshot(cache_helpers.ACTIVE_CACHE)
    recent_cache = await _cache_snapshot(cache_helpers.RECENT_CACHE)
    old_cache = await _cache_snapshot(cache_helpers.OLD_CACHE)
    global_cache = await _cache_snapshot(cache_helpers.GLOBAL_CACHE)
    revision_cache = await _cache_snapshot(cache_helpers._REVISION_CACHE)
    match_list_cache = await _cache_snapshot(matches_service._MATCH_LIST_CACHE)
    upcoming_match_cache = await _cache_snapshot(matches_service._UPCOMING_MATCH_CACHE)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "global_revision": global_revision,
        },
        "sync_queue": {
            **queue_stats,
            "last_job_started_at_iso": _iso_utc(queue_stats.get("last_job_started_at")),
            "last_job_finished_at_iso": _iso_utc(queue_stats.get("last_job_finished_at")),
        },
        "cache": {
            "active": active_cache,
            "recent": recent_cache,
            "old": old_cache,
            "global": global_cache,
            "revision": revision_cache,
            "match_list": match_list_cache,
            "upcoming_matches": upcoming_match_cache,
        },
    }
