from __future__ import annotations

from typing import Any, Dict, Optional

from async_db import query_async

from api.exceptions import BadRequestError, NotFoundError
from api.services.player_counts import get_player_counts
from division_overrides import combined_status_teams


def _get_excluded_team_ids(championship_id: int) -> set[str]:
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}


async def get_overview_stats() -> dict[str, int]:
    season_rows, div_rows, team_rows, match_rows, map_rows, totals_rows = await query_async(
        "SELECT COUNT(DISTINCT season) AS cnt FROM championships"
    ), await query_async(
        "SELECT COUNT(*) AS cnt FROM championships"
    ), await query_async(
        "SELECT COUNT(DISTINCT team_id) AS cnt FROM teams"
    ), await query_async(
        "SELECT COUNT(*) AS cnt FROM matches WHERE is_forfeit = 0"
    ), await query_async(
        "SELECT COUNT(*) AS cnt FROM maps"
    ), await query_async(
        """
        SELECT
          COALESCE(SUM(CASE WHEN ps.is_forfeit_map = 0 THEN (COALESCE(m.score_team1,0)+COALESCE(m.score_team2,0)) ELSE 0 END),0) AS total_rounds,
          COALESCE(SUM(ps.kills),0) AS total_kills,
          COALESCE(SUM(ps.deaths),0) AS total_deaths
        FROM player_stats ps
        LEFT JOIN maps m ON m.match_id = ps.match_id AND m.round_index = ps.round_index
        JOIN matches mt ON mt.match_id = ps.match_id
        WHERE mt.ignored_due_ban = 0
        """
    )
    player_counts = await get_player_counts(include_all_time=True)
    totals = totals_rows[0] if totals_rows else {"total_rounds": 0, "total_kills": 0, "total_deaths": 0}
    return {
        "total_seasons": int(season_rows[0]["cnt"] if season_rows else 0),
        "total_divisions": int(div_rows[0]["cnt"] if div_rows else 0),
        "total_teams": int(team_rows[0]["cnt"] if team_rows else 0),
        "total_players": int(player_counts.get("all_time_players") or 0),
        "total_matches": int(match_rows[0]["cnt"] if match_rows else 0),
        "total_maps_played": int(map_rows[0]["cnt"] if map_rows else 0),
        "total_rounds": int(totals.get("total_rounds") or 0),
        "total_kills": int(totals.get("total_kills") or 0),
        "total_deaths": int(totals.get("total_deaths") or 0),
    }


_STAT_COLUMN_MAP = {
    "kd": "kd",
    "adr": "adr",
    "rating": "rating",
    "hs_pct": "hs_pct",
    "kills": "kills",
    "mvps": "mvps",
    "kr": "kr",
}


async def get_top_players(
    stat: str,
    *,
    season: Optional[int],
    division: Optional[int],
    limit: int,
    min_maps: int,
) -> list[dict[str, Any]]:
    column = _STAT_COLUMN_MAP.get(stat)
    if column is None:
        raise BadRequestError(f"Invalid stat '{stat}'")

    where_clauses = ["pst.maps_played >= :min_maps"]
    params: Dict[str, Any] = {"min_maps": min_maps, "limit": limit}

    if season is not None:
        where_clauses.append("pst.season = :season")
        params["season"] = season

    if division is not None:
        where_clauses.append("pst.division_num = :division")
        params["division"] = division

    where_clause = " AND ".join(where_clauses)

    rows = await query_async(
        f"""
        SELECT pst.player_id, p.nickname, t.name AS team_name,
               pst.{column} AS stat_value, pst.maps_played,
               pst.season, pst.division_num
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE {where_clause}
        ORDER BY pst.{column} DESC
        LIMIT :limit
        """,
        params,
    )
    return rows


async def get_division_averages(championship_id: int) -> dict[str, float]:
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    season = champ_rows[0]["season"]
    division_num = champ_rows[0]["division_num"]
    excluded = _get_excluded_team_ids(championship_id)

    where_clause = "pst.season = :season AND pst.division_num = :division"
    params: Dict[str, Any] = {"season": season, "division": division_num}
    if excluded:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded)))
        where_clause += f" AND pst.team_id NOT IN ({placeholders})"
        for i, tid in enumerate(excluded):
            params[f"ex{i}"] = tid

    rows = await query_async(
        f"""
        SELECT AVG(pst.kd) AS avg_kd, AVG(pst.adr) AS avg_adr,
               AVG(pst.rating) AS avg_rating, AVG(pst.hs_pct) AS avg_hs_pct,
               AVG(pst.kr) AS avg_kr
        FROM player_season_totals pst
        WHERE {where_clause} AND pst.maps_played >= 3
        """,
        params,
    )
    if not rows:
        return {
            "avg_kd": 0.0,
            "avg_adr": 0.0,
            "avg_rating": 0.0,
            "avg_hs_pct": 0.0,
            "avg_kr": 0.0,
        }
    row = rows[0]
    return {
        "avg_kd": float(row.get("avg_kd") or 0.0),
        "avg_adr": float(row.get("avg_adr") or 0.0),
        "avg_rating": float(row.get("avg_rating") or 0.0),
        "avg_hs_pct": float(row.get("avg_hs_pct") or 0.0),
        "avg_kr": float(row.get("avg_kr") or 0.0),
    }


async def get_season_stats(season: int) -> dict[str, Any]:
    """Return aggregated statistics for an entire season.

    The payload mirrors the legacy shape used by the frontend Home view.
    """

    team_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT team_id) AS total_teams,
            COALESCE(SUM(matches_played), 0) AS matches_played_total,
            COALESCE(SUM(maps_played), 0) AS maps_played_total,
            COALESCE(SUM(rounds_won + rounds_lost), 0) AS rounds_played_total
        FROM team_season_totals
        WHERE season = :season
        """,
        {"season": season},
    )
    team_totals = team_rows[0] if team_rows else {}

    player_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT player_id) AS total_players,
            COALESCE(SUM(kills), 0) AS total_kills,
            COALESCE(SUM(deaths), 0) AS total_deaths,
            COALESCE(AVG(NULLIF(adr, 0)), 0) AS avg_adr
        FROM player_season_totals
        WHERE season = :season
        """,
        {"season": season},
    )
    player_totals = player_rows[0] if player_rows else {}

    # Progress tracking by championship (regular vs playoffs)
    progress_rows = await query_async(
        """
        SELECT
            CASE WHEN c.slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff,
            COUNT(DISTINCT m.match_id) AS total_matches,
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches
        FROM championships c
        LEFT JOIN matches m ON m.championship_id = c.championship_id
        WHERE c.season = :season
        GROUP BY is_playoff
        """,
        {"season": season},
    )

    overall_played = 0
    overall_total = 0
    regular = {"played": 0, "total": 0}
    playoffs = {"played": 0, "total": 0}

    for row in progress_rows:
        total = int(row.get("total_matches") or 0)
        played = int(row.get("played_matches") or 0)
        overall_played += played
        overall_total += total
        target = playoffs if row.get("is_playoff") else regular
        target["played"] += played
        target["total"] += total

    progress = {
        "overall": {"played": overall_played, "total": overall_total},
        "regular": regular,
        "playoffs": playoffs,
    }

    # Aggregate map stats across teams for the season
    map_rows = await query_async(
        """
        SELECT
            map_name,
            COALESCE(SUM(games), 0) AS games_played,
            COALESCE(SUM(wins), 0) AS wins,
            COALESCE(SUM(played), 0) AS picks,
            COALESCE(SUM(total_own_ban + opp_ban), 0) AS bans,
            COALESCE(SUM(kills), 0) AS kills,
            COALESCE(SUM(deaths), 0) AS deaths,
            COALESCE(SUM(adr * NULLIF(games,0)), 0) AS adr_weighted,
            COALESCE(SUM(CASE WHEN games > 0 THEN games ELSE 0 END), 0) AS adr_weight
        FROM team_map_season_totals
        WHERE season = :season
        GROUP BY map_name
        ORDER BY games_played DESC, map_name ASC
        """,
        {"season": season},
    )

    map_stats: list[dict[str, Any]] = []
    for row in map_rows:
        games_played = int(row.get("games_played") or 0)
        wins = int(row.get("wins") or 0)
        kills = float(row.get("kills") or 0)
        deaths = float(row.get("deaths") or 0)
        adr_weight = float(row.get("adr_weight") or 0)
        adr_weighted = float(row.get("adr_weighted") or 0)
        map_stats.append(
            {
                "map_name": row.get("map_name"),
                "maps_played": games_played,
                "wins": wins,
                "win_rate": float(wins) / games_played * 100 if games_played else 0.0,
                "banned": int(row.get("bans") or 0),
                "kills": int(kills),
                "deaths": int(deaths),
                "kd": (kills / deaths) if deaths else (kills if kills else 0.0),
                "adr": adr_weighted / adr_weight if adr_weight else 0.0,
                "rounds_played": 0,
                "clutches": 0,
                "sniper_kills": 0,
                "pistol_kills": 0,
            }
        )

    aggregates = {
        "season": season,
        "total_teams": int(team_totals.get("total_teams") or 0),
        "matches_played_total": int(team_totals.get("matches_played_total") or 0),
        "maps_played_total": int(team_totals.get("maps_played_total") or 0),
        "rounds_played_total": int(team_totals.get("rounds_played_total") or 0),
        "total_players": int(player_totals.get("total_players") or 0),
        "total_kills": int(player_totals.get("total_kills") or 0),
        "total_deaths": int(player_totals.get("total_deaths") or 0),
        "median_adr": float(player_totals.get("avg_adr") or 0.0),
        "median_survival": 0.0,
    }

    return {
        "season": season,
        "aggregates": aggregates,
        "map_stats": map_stats,
        "leaders": [],
        "progress": progress,
    }
