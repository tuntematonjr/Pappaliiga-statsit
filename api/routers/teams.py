"""Team API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import teams_service

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


@router.get("/", response_model=List[TeamInfo])
async def list_teams(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(100, ge=1, le=500),
):
    rows = await teams_service.list_teams(season=season, division=division, limit=limit)
    return [TeamInfo(**row) for row in rows]
