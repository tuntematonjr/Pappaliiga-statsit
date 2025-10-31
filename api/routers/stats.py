"""Stats and overview API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from async_db import query_async
from api.services.player_counts import get_player_counts
from division_overrides import combined_status_teams


def get_excluded_team_ids(championship_id: int) -> set[str]:
    """Get set of team IDs to exclude (banned + quit)."""
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}

router = APIRouter()


class TopPlayer(BaseModel):
    """Top player stats."""
    player_id: str
    nickname: str
    team_name: Optional[str]
    stat_value: float
    maps_played: int
    season: int
    division_num: int


class StatsOverview(BaseModel):
    """Overview stats for index page."""
    total_seasons: int
    total_divisions: int
    total_teams: int
    total_players: int
    total_matches: int
    total_maps_played: int
    total_rounds: int
    total_kills: int
    total_deaths: int


@router.get("/overview", response_model=StatsOverview)
async def get_stats_overview():
    """Get high-level overview stats for index page."""
    # Count distinct entities across all data
    season_rows = await query_async("SELECT COUNT(DISTINCT season) AS cnt FROM championships")
    div_rows = await query_async("SELECT COUNT(*) AS cnt FROM championships")
    team_rows = await query_async("SELECT COUNT(DISTINCT team_id) AS cnt FROM teams")
    match_rows = await query_async("SELECT COUNT(*) AS cnt FROM matches WHERE is_forfeit = 0")
    map_rows = await query_async("SELECT COUNT(*) AS cnt FROM maps")
    player_counts = await get_player_counts(include_all_time=True)
    # Aggregate rounds/kills/deaths across all seasons from raw player_stats
    # Use maps join to compute rounds per map and exclude matches marked ignored_due_ban
    totals_rows = await query_async(
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
    totals = totals_rows[0] if totals_rows else {"total_rounds": 0, "total_kills": 0, "total_deaths": 0}

    return {
        "total_seasons": season_rows[0]["cnt"] if season_rows else 0,
        "total_divisions": div_rows[0]["cnt"] if div_rows else 0,
        "total_teams": team_rows[0]["cnt"] if team_rows else 0,
        "total_players": player_counts.get("all_time_players") or 0,
        "total_matches": match_rows[0]["cnt"] if match_rows else 0,
        "total_maps_played": map_rows[0]["cnt"] if map_rows else 0,
        "total_rounds": int(totals.get("total_rounds") or 0),
        "total_kills": int(totals.get("total_kills") or 0),
        "total_deaths": int(totals.get("total_deaths") or 0),
    }


@router.get("/top-players/{stat}", response_model=List[TopPlayer])
async def get_top_players(
    stat: str,
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(10, ge=1, le=100),
    min_maps: int = Query(5, ge=0, description="Minimum maps played"),
):
    """Get top players by a specific stat (kd, adr, rating, hs_pct, etc.)."""
    # Map stat parameter to database column
    stat_column_map = {
        "kd": "kd",
        "adr": "adr",
        "rating": "rating",
        "hs_pct": "hs_pct",
        "kills": "kills",
        "mvps": "mvps",
        "kr": "kr",
    }
    
    if stat not in stat_column_map:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid stat '{stat}'")
    
    column = stat_column_map[stat]
    
    # Build query with optional filters
    where_clauses = ["pst.maps_played >= :min_maps"]
    params: Dict[str, Any] = {"min_maps": min_maps, "limit": limit}
    
    if season:
        where_clauses.append("pst.season = :season")
        params["season"] = season
    
    if division:
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
        params
    )
    
    return [
        {
            "player_id": r["player_id"],
            "nickname": r["nickname"],
            "team_name": r.get("team_name"),
            "stat_value": float(r["stat_value"] or 0.0),
            "maps_played": r["maps_played"] or 0,
            "season": r["season"],
            "division_num": r["division_num"],
        }
        for r in rows
    ]


@router.get("/division/{championship_id}/averages")
async def get_division_averages(championship_id: int):
    """Get division-wide average stats."""
    # Get season/division
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id}
    )
    
    if not champ_rows:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Championship {championship_id} not found")
    
    season = champ_rows[0]["season"]
    division_num = champ_rows[0]["division_num"]
    
    # Get excluded teams
    excluded = get_excluded_team_ids(championship_id)
    
    # Build exclusion clause
    where_clause = "pst.season = :season AND pst.division_num = :division"
    params: Dict[str, Any] = {"season": season, "division": division_num}
    
    if excluded:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded)))
        where_clause += f" AND pst.team_id NOT IN ({placeholders})"
        for i, tid in enumerate(excluded):
            params[f"ex{i}"] = tid
    
    # Calculate averages
    rows = await query_async(
        f"""
        SELECT AVG(pst.kd) AS avg_kd, AVG(pst.adr) AS avg_adr,
               AVG(pst.rating) AS avg_rating, AVG(pst.hs_pct) AS avg_hs_pct,
               AVG(pst.kr) AS avg_kr
        FROM player_season_totals pst
        WHERE {where_clause} AND pst.maps_played >= 3
        """,
        params
    )
    
    if not rows:
        return {
            "avg_kd": 0.0,
            "avg_adr": 0.0,
            "avg_rating": 0.0,
            "avg_hs_pct": 0.0,
            "avg_kr": 0.0,
        }
    
    avg = rows[0]
    return {
        "avg_kd": float(avg.get("avg_kd") or 0.0),
        "avg_adr": float(avg.get("avg_adr") or 0.0),
        "avg_rating": float(avg.get("avg_rating") or 0.0),
        "avg_hs_pct": float(avg.get("avg_hs_pct") or 0.0),
        "avg_kr": float(avg.get("avg_kr") or 0.0),
    }


@router.get("/seasons/{season}/stats")
async def get_season_stats(season: int):
    """Aggregate statistics for a given season used on the home page.

    Returns counts of divisions, teams, players, matches, maps and breakdowns
    for regular vs playoff matches (scheduled + played).
    """
    # Divisions (championships) - exclude playoffs so playoffs are not counted as separate divisions
    div_rows = await query_async(
        "SELECT COUNT(*) AS cnt FROM championships WHERE season = :season AND (is_playoffs = 0 OR is_playoffs IS NULL)",
        {"season": season}
    )

    # Teams participating in season (from team_season_totals)
    # Exclude banned/quit teams which are encoded in division_overrides (combined_status_teams)
    # First collect all championship_ids for this season
    champ_rows = await query_async(
        "SELECT championship_id FROM championships WHERE season = :season",
        {"season": season}
    )

    excluded_ids: set[str] = set()
    for cr in champ_rows:
        try:
            excl = combined_status_teams(str(cr["championship_id"]))
            for t in excl:
                if t and isinstance(t, dict) and t.get("team_id"):
                    excluded_ids.add(t["team_id"])
        except Exception:
            # Ignore per-championship errors and continue
            continue

    # Get distinct team_ids for the season and filter excluded ones in Python (simpler than dynamic SQL)
    team_id_rows = await query_async(
        "SELECT DISTINCT team_id FROM team_season_totals WHERE season = :season",
        {"season": season}
    )
    team_ids = [r["team_id"] for r in team_id_rows if r and r.get("team_id")]
    filtered_team_ids = [tid for tid in team_ids if tid not in excluded_ids]
    team_count = len(filtered_team_ids)

    # Players participating (from player_season_totals)
    player_counts = await get_player_counts(season=season, include_all_time=False)

    # Matches (scheduled) and maps
    match_rows = await query_async(
        "SELECT COUNT(*) AS cnt FROM matches WHERE season = :season",
        {"season": season}
    )

    map_rows = await query_async(
        "SELECT COUNT(*) AS cnt FROM maps WHERE season = :season",
        {"season": season}
    )

    # Rounds, kills, deaths aggregated from player season totals
    rounds_rows = await query_async(
        "SELECT COALESCE(SUM(rounds_played),0) AS rounds_played, COALESCE(SUM(kills),0) AS total_kills, COALESCE(SUM(deaths),0) AS total_deaths FROM player_season_totals WHERE season = :season",
        {"season": season}
    )

    rounds_data = rounds_rows[0] if rounds_rows else {"rounds_played": 0, "total_kills": 0, "total_deaths": 0}

    # If the season-level totals table is empty or zero (for example after a fresh sync
    # where per-season upserts didn't run), fall back to aggregating directly from
    # player_stats so the UI can still show meaningful totals.
    if (not rounds_data or
            (int(rounds_data.get("rounds_played", 0)) == 0 and int(rounds_data.get("total_kills", 0)) == 0 and int(rounds_data.get("total_deaths", 0)) == 0)):
        fallback_rows = await query_async(
            """
            SELECT
              COALESCE(SUM(CASE WHEN ps.is_forfeit_map = 0 THEN (COALESCE(m.score_team1,0)+COALESCE(m.score_team2,0)) ELSE 0 END),0) AS rounds_played,
              COALESCE(SUM(ps.kills),0) AS total_kills,
              COALESCE(SUM(ps.deaths),0) AS total_deaths
            FROM player_stats ps
            LEFT JOIN maps m ON m.match_id = ps.match_id AND m.round_index = ps.round_index
            JOIN matches mt ON mt.match_id = ps.match_id
            WHERE ps.season = :season AND mt.ignored_due_ban = 0
            """,
            {"season": season},
        )
        if fallback_rows:
            rounds_data = fallback_rows[0]

    # Regular vs playoff matches counts (scheduled)
    regular_rows = await query_async(
        "SELECT COUNT(m.match_id) AS cnt FROM matches m JOIN championships c USING (championship_id) WHERE m.season = :season AND (c.is_playoffs = 0 OR c.is_playoffs IS NULL)",
        {"season": season}
    )

    playoff_rows = await query_async(
        "SELECT COUNT(m.match_id) AS cnt FROM matches m JOIN championships c USING (championship_id) WHERE m.season = :season AND c.is_playoffs = 1",
        {"season": season}
    )

    # Played matches: consider finished_at IS NOT NULL as played
    played_regular_rows = await query_async(
        "SELECT COUNT(m.match_id) AS cnt FROM matches m JOIN championships c USING (championship_id) WHERE m.season = :season AND (c.is_playoffs = 0 OR c.is_playoffs IS NULL) AND m.finished_at IS NOT NULL",
        {"season": season}
    )

    played_playoff_rows = await query_async(
        "SELECT COUNT(m.match_id) AS cnt FROM matches m JOIN championships c USING (championship_id) WHERE m.season = :season AND c.is_playoffs = 1 AND m.finished_at IS NOT NULL",
        {"season": season}
    )

    # Previous season stats for delta computation (season-over-season comparison)
    prev_season = season - 1
    prev_rounds_rows = await query_async(
        "SELECT COALESCE(SUM(rounds_played),0) AS rounds_played, COALESCE(SUM(kills),0) AS total_kills, COALESCE(SUM(deaths),0) AS total_deaths FROM player_season_totals WHERE season = :prev_season",
        {"prev_season": prev_season}
    )
    prev_rounds_data = prev_rounds_rows[0] if prev_rounds_rows else {"rounds_played": 0, "total_kills": 0, "total_deaths": 0}
    
    # Compute deltas
    curr_rounds = int(rounds_data.get("rounds_played", 0))
    curr_kills = int(rounds_data.get("total_kills", 0))
    curr_deaths = int(rounds_data.get("total_deaths", 0))
    prev_rounds = int(prev_rounds_data.get("rounds_played", 0))
    prev_kills = int(prev_rounds_data.get("total_kills", 0))
    prev_deaths = int(prev_rounds_data.get("total_deaths", 0))

    return {
        "season": season,
        "divisions": div_rows[0]["cnt"] if div_rows else 0,
        "teams": team_count,
        "players": player_counts.get("season_players") or 0,
        "matches": match_rows[0]["cnt"] if match_rows else 0,
        "maps": map_rows[0]["cnt"] if map_rows else 0,
        "rounds_played": curr_rounds,
        "rounds_played_prev": prev_rounds,
        "rounds_delta": curr_rounds - prev_rounds,
        "total_kills": curr_kills,
        "total_kills_prev": prev_kills,
        "kills_delta": curr_kills - prev_kills,
        "total_deaths": curr_deaths,
        "total_deaths_prev": prev_deaths,
        "deaths_delta": curr_deaths - prev_deaths,
        "regular_matches": regular_rows[0]["cnt"] if regular_rows else 0,
        "playoff_matches": playoff_rows[0]["cnt"] if playoff_rows else 0,
        "played_regular_matches": played_regular_rows[0]["cnt"] if played_regular_rows else 0,
        "played_playoff_matches": played_playoff_rows[0]["cnt"] if played_playoff_rows else 0,
    }
