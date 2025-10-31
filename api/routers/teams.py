"""Team API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from async_db import compute_team_map_deltas_async, query_async

# Default avatar when remote avatar is missing or blocked
DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"

router = APIRouter()


class TeamInfo(BaseModel):
    """Team basic information."""
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    faceit_url: Optional[str]


class TeamSeasonStats(BaseModel):
    """Team stats for a specific season/division."""
    season: int
    division_num: int
    championship_id: str
    maps_played: int
    matches_played: int
    wins: int
    losses: int
    win_rate: float
    rounds_won: int
    rounds_lost: int
    maps_won: int


class MapStatsWithDelta(BaseModel):
    """Map-specific stats with curr/prev/delta."""
    map_name: str
    curr: Dict[str, Any]
    prev: Optional[Dict[str, Any]]
    delta: Optional[Dict[str, Any]]
    snapshot_ts: Optional[int]


@router.get("/{team_id}", response_model=TeamInfo)
async def get_team_info(team_id: str):
    """Get basic team information."""
    rows = await query_async(
        """
        SELECT team_id, name AS team_name, name AS display_name, avatar
        FROM teams
        WHERE team_id = :team_id
        """,
        {"team_id": team_id}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"Team '{team_id}' not found")
    
    team = rows[0]
    return {
        "team_id": team["team_id"],
        "team_name": team["team_name"],
        "display_name": team.get("display_name"),
        "avatar": team.get("avatar") or DEFAULT_AVATAR,
        "faceit_url": None,  # Not in schema
    }


@router.get("/{team_id}/seasons", response_model=List[TeamSeasonStats])
async def get_team_season_stats(team_id: str):
    """Get team stats across all seasons/divisions."""
    rows = await query_async(
        """
        SELECT tst.season, tst.division_num, c.championship_id,
               tst.maps_played, tst.matches_played, tst.matches_won AS wins,
               (tst.matches_played - tst.matches_won) AS losses,
               CASE WHEN tst.matches_played > 0 
                    THEN (tst.matches_won / tst.matches_played) 
                    ELSE 0.0 END AS win_rate,
               tst.rounds_won, tst.rounds_lost, tst.maps_won
        FROM team_season_totals tst
        JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
        WHERE tst.team_id = :team_id
        ORDER BY tst.season DESC, tst.division_num
        """,
        {"team_id": team_id}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"No stats found for team '{team_id}'")
    
    return [
        {
            "season": r["season"],
            "division_num": r["division_num"],
            "championship_id": r["championship_id"],
            "maps_played": r["maps_played"] or 0,
            "matches_played": r["matches_played"] or 0,
            "wins": r["wins"] or 0,
            "losses": r["losses"] or 0,
            "win_rate": float(r["win_rate"] or 0.0),
            "rounds_won": r["rounds_won"] or 0,
            "rounds_lost": r["rounds_lost"] or 0,
            "maps_won": r["maps_won"] or 0,
        }
        for r in rows
    ]


@router.get("/{team_id}/map-stats/{championship_id}", response_model=List[MapStatsWithDelta])
async def get_team_map_stats(team_id: str, championship_id: str):
    """Get detailed per-map stats with deltas for a team in a specific championship."""
    # Get championship info
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id}
    )
    
    if not champ_rows:
        raise HTTPException(status_code=404, detail=f"Championship {championship_id} not found")
    
    # Use the compute function we built
    map_deltas = await compute_team_map_deltas_async(championship_id, team_id)
    
    if not map_deltas:
        raise HTTPException(
            status_code=404,
            detail=f"No map stats found for team '{team_id}' in championship {championship_id}"
        )
    
    result = []
    for map_name, data in map_deltas.items():
        result.append({
            "map_name": map_name,
            "curr": data["curr"],
            "prev": data["prev"],
            "delta": data["delta"],
            "snapshot_ts": data["prev"].get("snapshot_ts") if data["prev"] else None,
        })
    
    return result


@router.get("/", response_model=List[TeamInfo])
async def list_teams(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(100, ge=1, le=500),
):
    """List all teams, optionally filtered by season/division."""
    if season and division:
        # Get teams that played in specific season/division
        rows = await query_async(
            """
            SELECT DISTINCT t.team_id, t.name AS team_name, t.name AS display_name, t.avatar
            FROM teams t
            JOIN team_season_totals tst ON tst.team_id = t.team_id
            WHERE tst.season = :season AND tst.division_num = :division
            ORDER BY t.name, t.team_id
            LIMIT :limit
            """,
            {"season": season, "division": division, "limit": limit}
        )
    else:
        # Get all teams
        rows = await query_async(
            """
            SELECT team_id, name AS team_name, name AS display_name, avatar
            FROM teams
            ORDER BY name, team_id
            LIMIT :limit
            """,
            {"limit": limit}
        )
    
    return [
        {
            "team_id": r["team_id"],
            "team_name": r["team_name"],
            "display_name": r.get("display_name"),
            "avatar": r.get("avatar") or DEFAULT_AVATAR,
            "faceit_url": None,
        }
        for r in rows
    ]
