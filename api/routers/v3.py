"""API v3 routes."""
from __future__ import annotations
from typing import List
from fastapi import APIRouter, HTTPException
from api.models_v3 import SeasonSummary, DivisionV3
from api.services import v3_service

router = APIRouter()

@router.get("/v3/summary/{season_id}", response_model=SeasonSummary)
async def get_season_summary(season_id: int):
    """Returns season summary statistics."""
    summary = await v3_service.get_season_summary_v3(season_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Season not found")
    return summary

@router.get("/v3/divisions/{season_id}", response_model=List[DivisionV3])
async def get_divisions(season_id: int):
    """Returns all divisions for a season."""
    divisions = await v3_service.get_divisions_v3(season_id)
    return divisions
