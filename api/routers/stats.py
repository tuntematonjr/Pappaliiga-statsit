"""Stats API endpoints used by the frontend."""
from __future__ import annotations

from typing import List, Literal

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import stats_service

router = APIRouter()


class SummaryTotals(CamelModel):
    divisions: int
    teams: int
    players: int
    matches: int
    maps: int
    rounds: int
    kills: int
    deaths: int


class SummaryMetric(CamelModel):
    id: str
    label: str
    value: int


class StatsSummaryResponse(CamelModel):
    scope: Literal["all", "season"]
    season: int | None = None
    label: str | None = None
    summary_totals: SummaryTotals
    metrics: List[SummaryMetric]
    progress: dict | None = None


@router.get("/summary/all", response_model=StatsSummaryResponse)
async def get_lifetime_summary():
    payload = await stats_service.get_stats_summary("all")
    return StatsSummaryResponse(**payload)


@router.get("/summary/season/{season_id}", response_model=StatsSummaryResponse)
async def get_season_summary(season_id: int):
    payload = await stats_service.get_stats_summary("season", season=season_id)
    return StatsSummaryResponse(**payload)


@router.get("/division/{championship_id}/averages")
async def get_division_averages(championship_id: str):
    try:
        averages = await stats_service.get_division_averages(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return averages
