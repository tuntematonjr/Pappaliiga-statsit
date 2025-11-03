"""Stats and overview API endpoints."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import BadRequestError, NotFoundError
from api.models import CamelModel
from api.services import stats_service

router = APIRouter()


class StatsOverview(CamelModel):
    total_seasons: int
    total_divisions: int
    total_teams: int
    total_players: int
    total_matches: int
    total_maps_played: int
    total_rounds: int
    total_kills: int
    total_deaths: int


class TopPlayer(CamelModel):
    player_id: str
    nickname: str
    team_name: Optional[str]
    stat_value: float
    maps_played: int
    season: int
    division_num: int


@router.get("/overview", response_model=StatsOverview)
async def get_stats_overview():
    stats = await stats_service.get_overview_stats()
    return StatsOverview(**stats)


@router.get("/home")
async def get_home_summary():
    """Compatibility endpoint used by the legacy frontend."""
    overview = await stats_service.get_overview_stats()
    return {"aggregates": overview}


@router.get("/top-players/{stat}", response_model=List[TopPlayer])
async def get_top_players(
    stat: str,
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(10, ge=1, le=100),
    min_maps: int = Query(5, ge=0, description="Minimum maps played"),
):
    try:
        rows = await stats_service.get_top_players(
            stat,
            season=season,
            division=division,
            limit=limit,
            min_maps=min_maps,
        )
    except BadRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [TopPlayer(**row) for row in rows]


@router.get("/division/{championship_id}/averages")
async def get_division_averages(championship_id: int):
    try:
        averages = await stats_service.get_division_averages(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return averages


async def get_season_stats(season: int):
    """Compatibility helper for `api.main` to fetch season stats."""
    return await stats_service.get_season_stats(season)


@router.get("/seasons/{season}/stats")
async def get_season_stats_route(season: int):
    return await get_season_stats(season)
