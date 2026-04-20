"""Shared helpers for computing season-level summary totals."""
from __future__ import annotations

from typing import Any, Dict

from db_async import count_played_matches, query_async

from api.services.cache_helpers import get_season_revision, select_season_cache


def dedupe_team_total(value: float | int | None) -> int:
    """Team-level aggregates count each match/map twice (once per team)."""
    if not value:
        return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if numeric <= 0:
        return 0
    return int(round(numeric / 2))


async def get_season_summary_totals(season: int) -> Dict[str, Any]:
    """Return the canonical totals for the eight summary cards for a single season."""
    cache, ttl_seconds = select_season_cache(season)
    if cache is not None:
        revision = await get_season_revision(season)
        cache_key = ("season-summary-totals", season, revision)
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_season_summary_totals(season),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_season_summary_totals(season)


async def _compute_season_summary_totals(season: int) -> Dict[str, Any]:
    """Compute season summary totals without caching."""
    team_rows, player_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT tst.team_id) AS total_teams,
            COALESCE(SUM(tst.matches_played), 0) AS matches_played_total,
            COALESCE(SUM(tst.matches_won), 0) AS matches_won_total,
            COALESCE(SUM(tst.maps_played), 0) AS maps_played_total,
            COALESCE(SUM(tst.rounds_won + tst.rounds_lost), 0) AS rounds_played_total,
            COALESCE(SUM(tst.rounds_won), 0) AS rounds_won_total,
            COALESCE(SUM(tst.rounds_lost), 0) AS rounds_lost_total
        FROM team_season_totals tst
        WHERE tst.season = :season
        """,
        {"season": season},
    ), await query_async(
        """
        SELECT
            COUNT(DISTINCT pst.player_id) AS total_players,
            COALESCE(SUM(pst.kills), 0) AS total_kills,
            COALESCE(SUM(pst.deaths), 0) AS total_deaths,
            COALESCE(AVG(NULLIF(pst.adr, 0)), 0) AS avg_adr,
            COALESCE(SUM(pst.utility_damage), 0) AS total_utility_damage,
            COALESCE(SUM(pst.cl_1v1_wins + pst.cl_1v2_wins), 0) AS total_clutch_wins,
            COALESCE(SUM(CAST(pst.entry_wins AS SIGNED) * 2 - CAST(pst.entry_count AS SIGNED)), 0) AS total_entry_diff
        FROM player_season_totals pst
        WHERE pst.season = :season
        """,
        {"season": season},
    )
    team_totals_row = team_rows[0] if team_rows else {}
    player_totals_row = player_rows[0] if player_rows else {}

    team_totals = {
        "total_teams": int(team_totals_row.get("total_teams") or 0),
        "matches_played_total": int(team_totals_row.get("matches_played_total") or 0),
        "matches_won_total": int(team_totals_row.get("matches_won_total") or 0),
        "maps_played_total": int(team_totals_row.get("maps_played_total") or 0),
        "rounds_played_total": int(team_totals_row.get("rounds_played_total") or 0),
        "rounds_won_total": int(team_totals_row.get("rounds_won_total") or 0),
        "rounds_lost_total": int(team_totals_row.get("rounds_lost_total") or 0),
    }
    player_totals = {
        "total_players": int(player_totals_row.get("total_players") or 0),
        "total_kills": int(player_totals_row.get("total_kills") or 0),
        "total_deaths": int(player_totals_row.get("total_deaths") or 0),
        "avg_adr": float(player_totals_row.get("avg_adr") or 0.0),
        "total_utility_damage": int(player_totals_row.get("total_utility_damage") or 0),
        "total_clutch_wins": int(player_totals_row.get("total_clutch_wins") or 0),
        "total_entry_diff": int(player_totals_row.get("total_entry_diff") or 0),
    }

    division_rows = await query_async(
        """
        SELECT COUNT(*) AS cnt
        FROM championships
        WHERE season = :season
          AND is_playoffs = 0
        """,
        {"season": season},
    )
    if not division_rows or not division_rows[0].get("cnt"):
        division_rows = await query_async(
            """
            SELECT COUNT(DISTINCT division_num) AS cnt
            FROM team_season_totals
            WHERE season = :season
            """,
            {"season": season},
        )

    matches_played = await count_played_matches(
        season=season,
        include_forfeits=False,
        include_ignored=False,
    )
    map_round_rows = await query_async(
        """
        SELECT
            COUNT(*) AS total_maps,
            COALESCE(SUM(CASE WHEN is_forfeit = 0 THEN (COALESCE(score_team1,0) + COALESCE(score_team2,0)) ELSE 0 END),0) AS total_rounds
        FROM maps
        WHERE season = :season
                    AND match_id IN (
                            SELECT match_id
                            FROM matches
                            WHERE season = :season
                                AND COALESCE(ignored_due_ban, 0) = 0
                    )
        """,
        {"season": season},
    )

    map_round_row = map_round_rows[0] if map_round_rows else {}

    maps_total = int(map_round_row.get("total_maps") or 0)
    fallback_maps = dedupe_team_total(team_totals["maps_played_total"])
    if maps_total == 0:
        maps_total = fallback_maps

    rounds_total = int(map_round_row.get("total_rounds") or 0)
    fallback_rounds = dedupe_team_total(team_totals["rounds_played_total"])
    if rounds_total == 0:
        rounds_total = fallback_rounds

    kills_total = int(player_totals["total_kills"] or 0)
    deaths_total = int(player_totals["total_deaths"] or 0)

    summary_totals = {
        "divisions": int((division_rows[0] or {}).get("cnt") or 0),
        "teams": team_totals["total_teams"],
        "players": player_totals["total_players"],
        # Matches, maps, rounds, kills and deaths include played games from the regular season and playoffs.
        "matches": int(matches_played or 0),
        "maps": maps_total,
        "rounds": rounds_total,
        "kills": kills_total,
        "deaths": deaths_total,
    }
    return {
        "summary_totals": summary_totals,
        "team_totals": team_totals,
        "player_totals": player_totals,
    }
