"""Utilities for aggregating unique player counts."""
from __future__ import annotations

from typing import Any, Dict, Optional

from db_async import query_async
from api.services.cache_helpers import (
    GLOBAL_CACHE,
    get_global_revision,
    get_season_revision,
    select_season_cache,
)


async def get_player_counts(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    include_all_time: bool = True,
) -> Dict[str, int | None]:
    """Return unique player counts for the requested scope.

    Args:
        season: Limit the counts to a specific season.
        division: When provided with ``season``, limit the division count.
        include_all_time: When ``True`` also include the all-time unique player count.

    Returns:
        Dict with keys ``division_players``, ``season_players`` and ``all_time_players``.
        Missing scopes are returned as ``None``.

    Raises:
        ValueError: If ``division`` is provided without ``season``.
    """
    # Build cache key based on parameters
    if season is not None:
        revision = await get_season_revision(season)
        cache_key = ("get_player_counts", season, division, include_all_time, revision)
        cache, ttl_seconds = select_season_cache(season)
    else:
        revision = await get_global_revision()
        cache_key = ("get_player_counts", None, None, include_all_time, revision)
        cache = GLOBAL_CACHE
        ttl_seconds = None

    async def _compute():
        return await _compute_player_counts(season=season, division=division, include_all_time=include_all_time)

    if ttl_seconds:
        cached_value, _ = await cache.get_or_set(cache_key, _compute, ttl_seconds=ttl_seconds)
    else:
        cached_value, _ = await cache.get_or_set(cache_key, _compute)
    return cached_value


async def _compute_player_counts(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    include_all_time: bool = True,
) -> Dict[str, int | None]:
    counts: Dict[str, int | None] = {
        "division_players": None,
        "season_players": None,
        "all_time_players": None,
    }

    if season is not None:
        params: Dict[str, Any] = {"season": season}
        if division is not None:
            params["division"] = division
            query = """
                SELECT
                    COUNT(DISTINCT CASE WHEN pst.division_num = :division THEN pst.player_id END) AS division_players,
                    COUNT(DISTINCT pst.player_id) AS season_players
                FROM player_season_totals pst
                WHERE pst.season = :season
            """
        else:
            query = """
                SELECT
                    COUNT(DISTINCT pst.player_id) AS season_players
                FROM player_season_totals pst
                WHERE pst.season = :season
            """
        rows = await query_async(query, params)
        row = rows[0] if rows else {}

        if "division_players" in row:
            counts["division_players"] = int(row.get("division_players") or 0)
        counts["season_players"] = int(row.get("season_players") or 0)
    elif division is not None:
        raise ValueError("Season must be provided when computing division player counts.")

    if include_all_time:
        all_time_rows = await query_async(
            "SELECT COUNT(DISTINCT player_id) AS cnt FROM players"
        )
        counts["all_time_players"] = (
            int(all_time_rows[0].get("cnt") or 0) if all_time_rows else 0
        )

    return counts

