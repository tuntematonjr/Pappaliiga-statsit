"""Division-page bundle endpoint — returns all data the division view needs in one request."""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.services import divisions_service, matches_service
from db_async import query_async

router = APIRouter()


@router.get("/division-page/{championship_id}")
async def get_division_page(championship_id: str) -> Dict[str, Any]:
    """Return division details, match list, and map catalog in a single response.

    Replaces the two separate API calls the division view previously made:
      GET /api/divisions/{championship_id}
      GET /api/matches/division/{championship_id}
    """
    try:
        champ_row = await divisions_service.fetch_division_by_id(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    details_task = asyncio.create_task(divisions_service.get_division_details(champ_row))
    matches_task = asyncio.create_task(
        matches_service.get_division_matches(championship_id, limit=500, offset=0)
    )

    details, matches_result = await asyncio.gather(
        details_task, matches_task, return_exceptions=True
    )

    if isinstance(details, Exception):
        details = {}
    if isinstance(matches_result, Exception):
        matches_items = []
    else:
        matches_items = matches_result[0] if isinstance(matches_result, tuple) else []

    return {
        "ok": True,
        "details": details,
        "matches": matches_items,
    }
