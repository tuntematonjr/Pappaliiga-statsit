"""Season overview routes (SPA-focused)."""
from __future__ import annotations
from typing import List
from fastapi import APIRouter, HTTPException
from api.models import SeasonSummary, DivisionV3
from api.services import season_view_service

router = APIRouter()


@router.get("/season-view/summary/{season_id}", response_model=SeasonSummary)
async def get_season_summary(season_id: int):
    """Returns season summary statistics for the SPA overview."""
    summary = await season_view_service.get_season_summary(season_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Season not found")
    return summary


@router.get("/season-view/divisions/{season_id}", response_model=List[DivisionV3])
async def get_divisions(season_id: int):
    """Returns all divisions for a season with progress metadata."""
    divisions = await season_view_service.get_divisions(season_id)
    return divisions
