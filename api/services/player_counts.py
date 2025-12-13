"""Utilities for aggregating unique player counts."""
from __future__ import annotations

from typing import Any, Dict, Optional

from db_async import query_async


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

