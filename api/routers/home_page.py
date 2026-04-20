"""Home-page bundle endpoint — returns all data the home view needs in one request."""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import APIRouter

from api.services import season_view_service, stats_service

router = APIRouter()


@router.get("/home-page/{season_id}")
async def get_home_page(season_id: int) -> Dict[str, Any]:
    """Return season summary, divisions, and lifetime stats in a single response.

    Replaces the three separate API calls the home view previously made:
      GET /api/stats/summary/season/{id}
      GET /api/season-view/divisions/{id}
      GET /api/stats/summary/all
    """
    summary_task = asyncio.create_task(
        season_view_service.get_season_summary(season_id)
    )
    divisions_task = asyncio.create_task(
        season_view_service.get_divisions(season_id)
    )
    lifetime_task = asyncio.create_task(
        stats_service.get_stats_summary("all")
    )

    summary, divisions, lifetime = await asyncio.gather(
        summary_task, divisions_task, lifetime_task, return_exceptions=True
    )

    return {
        "ok": True,
        "summary": summary if not isinstance(summary, Exception) else {},
        "divisions": divisions if not isinstance(divisions, Exception) else [],
        "lifetime_summary": lifetime if not isinstance(lifetime, Exception) else {},
    }
