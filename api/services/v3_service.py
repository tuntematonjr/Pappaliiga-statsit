"""API v3 service layer."""
from __future__ import annotations

from typing import Any, Dict, List

import db_ops_async as db
from api.services import seasons_service
from db_async import get_pool


def _as_int(value: Any, default: int = 0) -> int:
    """Best effort integer conversion to avoid Pydantic validation errors."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    """Best effort float conversion to avoid Pydantic validation errors."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def get_season_summary_v3(season_id: int) -> Dict[str, Any]:
    """
    Return aggregated season statistics for the v3 API.

    Reuses the existing season summary service to ensure the numbers used by v1/v2
    endpoints stay in sync with the new frontend.
    """
    summary = await seasons_service.get_season_summary(season_id)
    if not summary:
        return {}

    progress = summary.get("progress") or {}

    return {
        "season_id": _as_int(summary.get("season_id", season_id), season_id),
        "divisions_total": _as_int(progress.get("divisions_total") or summary.get("divisions_total")),
        "divisions_finished": _as_int(progress.get("divisions_finished") or summary.get("divisions_finished")),
        "teams": _as_int(summary.get("teams")),
        "players": _as_int(summary.get("players")),
        "matches": _as_int(summary.get("matches")),
        "rounds": _as_int(summary.get("rounds")),
        "kills": _as_int(summary.get("kills")),
        "deaths": _as_int(summary.get("deaths")),
        "adr_avg": _as_float(summary.get("adr_avg")),
        "kd_avg": _as_float(summary.get("kd_ratio") or summary.get("kd_avg")),
        "win_rate": _as_float(summary.get("win_rate")),
    }


async def get_divisions_v3(season_id: int) -> List[Dict[str, Any]]:
    """Gets all divisions for a season for v3."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Query divisions directly by season
        divisions = await db.get_all_base_divisions_for_season(conn, season_id)

        result = []
        for div in divisions:
            stats = await db.get_division_stats_for_v3(conn, div["division_id"])

            season_stats = stats.get("season", {})
            playoff_stats = stats.get("playoffs", {})

            def get_status(matches_played, matches_total, db_status):
                if matches_played == 0:
                    return "waiting"
                if matches_played >= matches_total and matches_total > 0:
                    return "finished"
                if db_status == "finished":  # last match finished
                    return "finished"
                return "active"

            season_status = get_status(
                season_stats.get("matches_played", 0),
                season_stats.get("matches_total", 0),
                season_stats.get("division_status"),
            )

            playoff_matches_played = playoff_stats.get("matches_played", 0)
            playoff_matches_total = 7  # Hardcoded as per spec for now

            playoff_status = get_status(playoff_matches_played, playoff_matches_total, None)

            result.append(
                {
                    "division_id": div["division_id"],
                    "tier": div["tier"],
                    "name": div["name"],
                    "status": season_status,
                    "season": {
                        "teams": season_stats.get("teams", 0),
                        "matches_played": season_stats.get("matches_played", 0),
                        "matches_total": season_stats.get("matches_total", 0),
                    },
                    "playoffs": {
                        "status": playoff_status,
                        "teams": 8,
                        "matches_played": playoff_matches_played,
                        "matches_total": playoff_matches_total,
                        "winner_team": playoff_stats.get("winner_team"),
                    },
                    "meta": {
                        "winner_team": None,  # Placeholder
                        "mvp_player": None,
                    },
                }
            )
        return result
