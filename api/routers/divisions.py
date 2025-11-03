"""Division and season API endpoints."""
from __future__ import annotations

import asyncio
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel, PaginationMeta
from api.services import divisions_service

router = APIRouter()


class SeasonInfo(CamelModel):
    season: int
    divisions: List[int]
    championship_ids: List[str]


class DivisionSummary(CamelModel):
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    teams_count: int | None = 0
    played_matches: int | None = 0
    total_matches: int | None = 0
    last_updated: Optional[str] = None
    tier: Optional[str] = None


class TeamBasic(CamelModel):
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    matches_played: int = 0
    matches_won: int = 0
    matches_lost: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    match_win_rate: float = 0.0
    maps_played: int = 0
    maps_won: int = 0
    maps_lost: int = 0
    rounds_won: int = 0
    rounds_lost: int = 0
    rounds_diff: int = 0
    kills: int = 0
    deaths: int = 0
    kd: float = 0.0
    adr: float = 0.0
    damage: int = 0
    players: Optional[List[dict]] = None


class MapVoteStats(CamelModel):
    map_name: Optional[str]
    pretty_name: Optional[str]
    image_sm: Optional[str]
    maps_played: int
    banned: int
    kills: int
    deaths: int
    damage: int
    rounds_played: int
    adr: float
    kr: float
    udpr: float
    enemy_flash: float
    sniper_kills: int
    assists: int
    k2: int
    k3: int
    k4: int
    ace: int
    pistol_kills: int
    pick_rate: float


class DivisionAggregates(CamelModel):
    played_matches: int
    total_matches: int
    forfeits: int


class DivisionLeader(CamelModel):
    player_id: Optional[str]
    team_id: Optional[str]
    team_name: Optional[str]
    nickname: Optional[str]
    kills: int
    deaths: int
    adr: float
    kr: float
    rating: float
    mvps: int
    utility_damage: int


class DivisionDetails(CamelModel):
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    teams: List[TeamBasic]
    excluded_team_ids: List[str]
    map_stats: Optional[List[MapVoteStats]] = None
    aggregates: Optional[DivisionAggregates] = None
    leaders: Optional[List[DivisionLeader]] = None
    player_count: int | None = None
    season_player_count: int | None = None
    all_time_player_count: int | None = None


class DivisionListResponse(CamelModel):
    items: List[DivisionSummary]
    meta: PaginationMeta


@router.get("/seasons", response_model=List[SeasonInfo])
async def get_seasons():
    rows = await divisions_service.fetch_seasons()
    return [SeasonInfo(**row) for row in rows]


@router.get("", response_model=DivisionListResponse)
async def get_all_divisions(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    rows, total = await asyncio.gather(
        divisions_service.list_divisions(limit, offset),
        divisions_service.count_divisions(),
    )
    items = [_normalize_division_summary(row) for row in rows]
    return DivisionListResponse(
        items=items,
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/season/{season}", response_model=DivisionListResponse)
async def get_divisions_by_season(
    season: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    rows, total = await asyncio.gather(
        divisions_service.list_divisions_by_season(season, limit, offset),
        divisions_service.count_divisions(season=season),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No divisions found for season {season}")
    items = [_normalize_division_summary(row) for row in rows]
    return DivisionListResponse(
        items=items,
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/by-slug/{slug}", response_model=DivisionDetails)
async def get_division_by_slug(slug: str):
    try:
        champ_row = await divisions_service.fetch_division_by_slug(slug)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    details = await divisions_service.get_division_details(champ_row)
    return DivisionDetails(**details)


@router.get("/{championship_id}", response_model=DivisionDetails)
async def get_division_by_id(championship_id: str):
    try:
        champ_row = await divisions_service.fetch_division_by_id(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    details = await divisions_service.get_division_details(champ_row)
    return DivisionDetails(**details)


@router.get("/{championship_id}/details", response_model=DivisionDetails)
async def get_division_by_id_legacy(championship_id: str):
    return await get_division_by_id(championship_id)


def _normalize_division_summary(row: dict[str, Any]) -> DivisionSummary:
    data = dict(row)
    last_updated = data.get("last_updated")
    if last_updated is not None and not isinstance(last_updated, str):
        data["last_updated"] = last_updated.isoformat()
    data["is_playoff"] = bool(data.get("is_playoff"))
    return DivisionSummary(**data)
