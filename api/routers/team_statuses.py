"""Team statuses API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from api.services import team_status_service

router = APIRouter()


@router.get("/{championship_id}")
async def get_team_statuses(championship_id: str) -> List[Dict[str, Any]]:
    """Return all team status entries for a championship."""
    return await team_status_service.list_team_statuses(championship_id)


@router.get("/{championship_id}/excluded")
async def get_excluded_teams(championship_id: str) -> Dict[str, Any]:
    """Return lookup of excluded teams for a championship."""
    lookup = await team_status_service.get_excluded_team_lookup(championship_id)
    return lookup
