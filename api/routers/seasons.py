"""Seasons API endpoints used by the frontend."""
from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter, HTTPException

from api.models import CamelModel
from api.services import seasons_service

router = APIRouter()
logger = logging.getLogger(__name__)


class SeasonListItem(CamelModel):
    id: int
    name: str
    status: str
    start_date: str | None
    end_date: str | None
    divisions_count: int


@router.get("", response_model=List[SeasonListItem])
async def list_seasons():
    """Return list of all seasons with metadata."""
    try:
        seasons = await seasons_service.get_seasons_list()
        return [SeasonListItem(**s) for s in seasons]
    except Exception as exc:
        logger.exception("Failed to list seasons")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
