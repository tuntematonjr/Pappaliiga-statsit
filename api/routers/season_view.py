"""Season overview routes (SPA-focused)."""
from __future__ import annotations
from typing import List

from fastapi import APIRouter

from api.models import DivisionV3
from api.services import season_view_service

router = APIRouter()


@router.get("/season-view/divisions/{season_id}", response_model=List[DivisionV3])
async def get_divisions(season_id: int):
    """Returns all divisions for a season with progress metadata."""
    divisions = await season_view_service.get_divisions(season_id)
    return divisions
