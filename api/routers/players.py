"""Player API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from async_db import compute_player_map_deltas_async, query_async

router = APIRouter()

# Default avatar when remote avatar is missing or blocked
DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


class PlayerInfo(BaseModel):
    """Player basic information."""
    player_id: str
    nickname: str
    country: Optional[str]
    avatar: Optional[str]
    faceit_url: Optional[str]


class PlayerSeasonStats(BaseModel):
    """Player stats for a specific season/division."""
    season: int
    division_num: int
    championship_id: str
    team_id: str
    team_name: Optional[str]
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    kd: float
    adr: float
    rating: float
    hs_pct: float
    mvps: int


class PlayerMapStatsWithDelta(BaseModel):
    """Player map-specific stats with curr/prev/delta."""
    map_name: str
    curr: Dict[str, Any]
    prev: Optional[Dict[str, Any]]
    delta: Optional[Dict[str, Any]]
    snapshot_ts: Optional[int]


@router.get("/{player_id}", response_model=PlayerInfo)
async def get_player_info(player_id: str):
    """Get basic player information."""
    rows = await query_async(
        """
        SELECT player_id, nickname
        FROM players
        WHERE player_id = :player_id
        """,
        {"player_id": player_id}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"Player '{player_id}' not found")
    
    player = rows[0]
    return {
        "player_id": player["player_id"],
        "nickname": player["nickname"],
        "country": player.get("country"),
        "avatar": player.get("avatar") or DEFAULT_AVATAR,
        "faceit_url": player.get("faceit_url"),
    }


@router.get("/{player_id}/seasons", response_model=List[PlayerSeasonStats])
async def get_player_season_stats(player_id: str):
    """Get player stats across all seasons/divisions."""
    rows = await query_async(
        """
        SELECT pst.season, pst.division_num, c.championship_id,
               pst.team_id, t.name AS team_name,
               pst.maps_played, pst.rounds_played, pst.kills, pst.deaths, pst.assists,
               pst.kd, pst.adr, pst.rating, pst.hs_pct, pst.mvps
        FROM player_season_totals pst
        JOIN championships c ON c.season = pst.season AND c.division_num = pst.division_num
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE pst.player_id = :player_id
        ORDER BY pst.season DESC, pst.division_num
        """,
        {"player_id": player_id}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"No stats found for player '{player_id}'")
    
    return [
        {
            "season": r["season"],
            "division_num": r["division_num"],
            "championship_id": r["championship_id"],
            "team_id": r["team_id"],
            "team_name": r.get("team_name"),
            "maps_played": r["maps_played"] or 0,
            "rounds_played": r["rounds_played"] or 0,
            "kills": r["kills"] or 0,
            "deaths": r["deaths"] or 0,
            "assists": r["assists"] or 0,
            "kd": float(r["kd"] or 0.0),
            "adr": float(r["adr"] or 0.0),
            "rating": float(r["rating"] or 0.0),
            "hs_pct": float(r["hs_pct"] or 0.0),
            "mvps": r["mvps"] or 0,
        }
        for r in rows
    ]


@router.get("/{player_id}/map-stats/{championship_id}", response_model=List[PlayerMapStatsWithDelta])
async def get_player_map_stats(player_id: str, championship_id: str):
    """Get detailed per-map stats with deltas for a player in a specific championship."""
    # Get championship info
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id}
    )
    
    if not champ_rows:
        raise HTTPException(status_code=404, detail=f"Championship {championship_id} not found")
    
    # Use the compute function we built
    map_deltas = await compute_player_map_deltas_async(championship_id, player_id)
    
    if not map_deltas:
        raise HTTPException(
            status_code=404,
            detail=f"No map stats found for player '{player_id}' in championship {championship_id}"
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


@router.get("/", response_model=List[PlayerInfo])
async def list_players(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    team_id: Optional[str] = Query(None, description="Filter by team"),
    limit: int = Query(100, ge=1, le=500),
):
    """List all players, optionally filtered by season/division/team."""
    if season and division:
        # Get players that played in specific season/division
        query = """
            SELECT DISTINCT p.player_id, p.nickname
            FROM players p
            JOIN player_season_totals pst ON pst.player_id = p.player_id
            WHERE pst.season = :season AND pst.division_num = :division
        """
        params: dict[str, Any] = {"season": season, "division": division, "limit": limit}
        
        if team_id:
            query += " AND pst.team_id = :team_id"
            params["team_id"] = team_id
        
        query += " ORDER BY p.nickname LIMIT :limit"
        rows = await query_async(query, params)
    else:
        # Get all players
        rows = await query_async(
            """
            SELECT player_id, nickname
            FROM players
            ORDER BY nickname
            LIMIT :limit
            """,
            {"limit": limit}
        )
    
    return [
        {
            "player_id": r["player_id"],
            "nickname": r["nickname"],
            "country": r.get("country"),
            "avatar": r.get("avatar") or DEFAULT_AVATAR,
            "faceit_url": r.get("faceit_url"),
        }
        for r in rows
    ]
