"""Player API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import players_service

router = APIRouter()


class PlayerInfo(CamelModel):
    player_id: str
    nickname: str
    country: Optional[str]
    avatar: Optional[str]
    faceit_url: Optional[str]


class PlayerSeasonStats(CamelModel):
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
    hs_pct: float
    mvps: int


class PlayerSeasonProgressPoint(CamelModel):
    snapshot_ts: int
    snapshot_time: Optional[str] = None
    team_id: Optional[str] = None
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    mvps: int
    kd: float
    adr: float
    kr: float
    hs_pct: float
    damage: int


class PlayerMapStatsWithDelta(CamelModel):
    map_name: str
    curr: Dict[str, Any]
    prev: Optional[Dict[str, Any]]
    delta: Optional[Dict[str, Any]]
    snapshot_ts: Optional[int]


@router.get("/{player_id}", response_model=PlayerInfo)
async def get_player_info(player_id: str):
    try:
        player = await players_service.fetch_player(player_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PlayerInfo(**player)


@router.get("/{player_id}/seasons", response_model=List[PlayerSeasonStats])
async def get_player_season_stats(player_id: str):
    try:
        rows = await players_service.fetch_player_season_stats(player_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [PlayerSeasonStats(**row) for row in rows]


@router.get("/{player_id}/map-stats/{championship_id}", response_model=List[PlayerMapStatsWithDelta])
async def get_player_map_stats(player_id: str, championship_id: str):
    try:
        rows = await players_service.fetch_player_map_stats(championship_id, player_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [PlayerMapStatsWithDelta(**row) for row in rows]


@router.get("/{player_id}/season-progression", response_model=List[PlayerSeasonProgressPoint])
async def get_player_season_progression(
    player_id: str,
    season: int = Query(..., description="Season number"),
    division: int = Query(..., description="Division number"),
):
    try:
        rows = await players_service.fetch_player_season_progression(player_id, season, division)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [PlayerSeasonProgressPoint(**row) for row in rows]


@router.get("/", response_model=List[PlayerInfo])
async def list_players(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    team_id: Optional[str] = Query(None, description="Filter by team"),
    limit: int = Query(100, ge=1, le=500),
):
    rows = await players_service.list_players(
        season=season,
        division=division,
        team_id=team_id,
        limit=limit,
    )
    return [PlayerInfo(**row) for row in rows]
