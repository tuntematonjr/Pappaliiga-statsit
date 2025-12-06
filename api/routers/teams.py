"""Team API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import teams_service
from async_db import query_async

router = APIRouter()


class TeamInfo(CamelModel):
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    faceit_url: Optional[str]


class TeamSeasonStats(CamelModel):
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


class MapStatsWithDelta(CamelModel):
    map_name: str
    curr: Dict[str, Any]
    prev: Optional[Dict[str, Any]]
    delta: Optional[Dict[str, Any]]
    snapshot_ts: Optional[int]


class TeamMatch(CamelModel):
    match_id: str
    ts: int
    status: str
    best_of: int
    played: int
    team1_id: str
    team2_id: str
    team1_name: str
    team2_name: str
    t1_avatar: Optional[str]
    t2_avatar: Optional[str]
    faceit_url: Optional[str]
    maps: Optional[List[Dict[str, Any]]]


class TeamPlayer(CamelModel):
    player_id: str
    nickname: str
    matches_played: int
    kills: int
    deaths: int
    damage: int
    adr: Optional[float]
    headshots: int


@router.get("/{team_id}", response_model=TeamInfo)
async def get_team_info(team_id: str):
    try:
        team = await teams_service.fetch_team(team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TeamInfo(**team)


@router.get("/{team_id}/seasons", response_model=List[TeamSeasonStats])
async def get_team_season_stats(team_id: str):
    try:
        rows = await teams_service.fetch_team_season_stats(team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [TeamSeasonStats(**row) for row in rows]


@router.get("/{team_id}/map-stats/{championship_id}", response_model=List[MapStatsWithDelta])
async def get_team_map_stats(team_id: str, championship_id: str):
    try:
        rows = await teams_service.fetch_team_map_stats(championship_id, team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [MapStatsWithDelta(**row) for row in rows]


@router.get("/{team_id}/map-stats", response_model=List[MapStatsWithDelta])
async def get_team_map_stats_default(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID")
):
    """Get team map stats. If championship_id not provided, uses latest."""
    try:
        # If no championship provided, get the latest one
        if championship_id is None:
            champ_rows = await query_async(
                """
                SELECT DISTINCT c.championship_id
                FROM team_season_totals tst
                JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
                WHERE tst.team_id = :team_id
                ORDER BY tst.season DESC
                LIMIT 1
                """,
                {"team_id": team_id}
            )
            if not champ_rows:
                raise NotFoundError(f"No championship found for team '{team_id}'")
            championship_id = champ_rows[0]["championship_id"]
        
        rows = await teams_service.fetch_team_map_stats(championship_id, team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [MapStatsWithDelta(**row) for row in rows]


@router.get("/{team_id}/matches", response_model=List[TeamMatch])
async def get_team_matches(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID")
):
    """Get team's matches. If championship_id not provided, uses latest."""
    try:
        rows = await teams_service.fetch_team_matches(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [TeamMatch(**row) for row in rows]


@router.get("/{team_id}/players", response_model=List[TeamPlayer])
async def get_team_players(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID")
):
    """Get team's players. If championship_id not provided, uses latest."""
    try:
        rows = await teams_service.fetch_team_players(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [TeamPlayer(**row) for row in rows]


@router.get("/{team_id}/season/{championship_id}")
async def get_team_season_details(team_id: str, championship_id: str):
    """Get comprehensive team details for a specific championship/season."""
    try:
        # Get team basic info
        team_info = await teams_service.fetch_team(team_id)
        
        # Get season-specific stats
        season_stats = await teams_service.fetch_team_season_stats(team_id)
        # Filter to this championship
        current_season_stats = [s for s in season_stats if s.get('championship_id') == championship_id]
        
        # Get map stats for this championship
        try:
            map_stats = await teams_service.fetch_team_map_stats(championship_id, team_id)
        except NotFoundError:
            map_stats = []
        
        # Get matches for this championship
        try:
            matches = await teams_service.fetch_team_matches(team_id, championship_id)
        except NotFoundError:
            matches = []
        
        # Get players for this championship
        try:
            players = await teams_service.fetch_team_players(team_id, championship_id)
        except NotFoundError:
            players = []
        
        return {
            "team": team_info,
            "season_stats": current_season_stats[0] if current_season_stats else None,
            "map_stats": map_stats,
            "matches": matches,
            "players": players,
            "championship_id": championship_id
        }
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/", response_model=List[TeamInfo])
async def list_teams(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(100, ge=1, le=500),
):
    rows = await teams_service.list_teams(season=season, division=division, limit=limit)
    return [TeamInfo(**row) for row in rows]
