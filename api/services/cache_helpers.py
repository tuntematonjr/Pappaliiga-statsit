from __future__ import annotations

import os
from typing import Optional, Tuple

from db_async import query_async
import faceit_config

from api.utils.cache import AsyncTTLCache


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


ACTIVE_TTL_SECONDS = _int_env("PL_CACHE_ACTIVE_TTL", 24 * 60 * 60)
RECENT_TTL_SECONDS = _int_env("PL_CACHE_RECENT_TTL", 24 * 60 * 60)
OLD_TTL_SECONDS = _int_env("PL_CACHE_OLD_TTL", 24 * 60 * 60)
GLOBAL_TTL_SECONDS = _int_env("PL_CACHE_GLOBAL_TTL", 24 * 60 * 60)
REVISION_TTL_SECONDS = _int_env("PL_CACHE_REVISION_TTL", 10)
RECENT_SEASON_WINDOW = _int_env("PL_CACHE_RECENT_WINDOW", 2)
SKIP_OLD_AFTER = _int_env("PL_CACHE_SKIP_OLD_AFTER", 5)

ACTIVE_CACHE = AsyncTTLCache(ttl_seconds=ACTIVE_TTL_SECONDS, maxsize=_int_env("PL_CACHE_ACTIVE_MAXSIZE", 512))
RECENT_CACHE = AsyncTTLCache(ttl_seconds=RECENT_TTL_SECONDS, maxsize=_int_env("PL_CACHE_RECENT_MAXSIZE", 384))
OLD_CACHE = AsyncTTLCache(ttl_seconds=OLD_TTL_SECONDS, maxsize=_int_env("PL_CACHE_OLD_MAXSIZE", 128))
GLOBAL_CACHE = AsyncTTLCache(ttl_seconds=GLOBAL_TTL_SECONDS, maxsize=_int_env("PL_CACHE_GLOBAL_MAXSIZE", 128))
_REVISION_CACHE = AsyncTTLCache(ttl_seconds=REVISION_TTL_SECONDS, maxsize=_int_env("PL_CACHE_REVISION_MAXSIZE", 256))


def select_season_cache(season: Optional[int]) -> Tuple[Optional[AsyncTTLCache], Optional[int]]:
    if season is None:
        return None, None

    current_season = faceit_config.CURRENT_SEASON

    if season == current_season:
        return ACTIVE_CACHE, ACTIVE_TTL_SECONDS

    if season >= current_season - RECENT_SEASON_WINDOW:
        return RECENT_CACHE, RECENT_TTL_SECONDS

    if season <= current_season - SKIP_OLD_AFTER:
        return None, None

    return OLD_CACHE, OLD_TTL_SECONDS


async def get_season_revision(season: int) -> Optional[str]:
    async def _fetch() -> Optional[str]:
        rows = await query_async(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(updated_at) FROM matches WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM maps WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM team_season_totals WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM player_season_totals WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM team_map_season_totals WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM player_map_season_totals WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM championships WHERE season = :season), '1970-01-01 00:00:00'),
                COALESCE((
                    SELECT MAX(cts.updated_at)
                    FROM championship_team_statuses cts
                    JOIN championships c ON c.championship_id = cts.championship_id
                    WHERE c.season = :season
                ), '1970-01-01 00:00:00')
            ) AS revision
            """,
            {"season": season},
        )
        if not rows:
            return None
        revision = rows[0].get("revision")
        return str(revision) if revision is not None else None

    cache_key = ("season-revision", season)
    revision, _ = await _REVISION_CACHE.get_or_set(cache_key, _fetch)
    return revision


async def get_championship_revision(championship_id: str) -> Optional[str]:
    async def _fetch() -> Optional[str]:
        rows = await query_async(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(updated_at) FROM matches WHERE championship_id = :champ_id), '1970-01-01 00:00:00'),
                COALESCE((
                    SELECT MAX(mp.updated_at)
                    FROM maps mp
                    JOIN matches m ON m.match_id = mp.match_id
                    WHERE m.championship_id = :champ_id
                ), '1970-01-01 00:00:00'),
                COALESCE((
                    SELECT MAX(updated_at)
                    FROM championship_team_statuses
                    WHERE championship_id = :champ_id
                ), '1970-01-01 00:00:00')
            ) AS revision
            """,
            {"champ_id": championship_id},
        )
        if not rows:
            return None
        revision = rows[0].get("revision")
        return str(revision) if revision is not None else None

    cache_key = ("championship-revision", championship_id)
    revision, _ = await _REVISION_CACHE.get_or_set(cache_key, _fetch)
    return revision


async def get_global_revision() -> Optional[str]:
    async def _fetch() -> Optional[str]:
        rows = await query_async(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(updated_at) FROM matches), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM maps), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM team_season_totals), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM player_season_totals), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM team_map_season_totals), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM player_map_season_totals), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM championships), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM teams), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM players), '1970-01-01 00:00:00'),
                COALESCE((SELECT MAX(updated_at) FROM championship_team_statuses), '1970-01-01 00:00:00')
            ) AS revision
            """
        )
        if not rows:
            return None
        revision = rows[0].get("revision")
        return str(revision) if revision is not None else None

    cache_key = ("global-revision",)
    revision, _ = await _REVISION_CACHE.get_or_set(cache_key, _fetch)
    return revision


async def clear_api_response_caches(*, clear_revision_cache: bool = True) -> dict[str, int]:
    """Clear all in-process API caches used by service-layer endpoints."""
    caches = [
        ("active", ACTIVE_CACHE),
        ("recent", RECENT_CACHE),
        ("old", OLD_CACHE),
        ("global", GLOBAL_CACHE),
    ]
    if clear_revision_cache:
        caches.append(("revision", _REVISION_CACHE))

    cleared: dict[str, int] = {}
    for cache_name, cache in caches:
        removed = await cache.size()
        await cache.clear()
        cleared[cache_name] = removed
    return cleared
