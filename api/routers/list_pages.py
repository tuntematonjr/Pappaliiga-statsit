"""List-page bundle endpoints — each returns seasons + list data in one request."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.services import elo_service, seasons_service, teams_service, players_service, matches_service

router = APIRouter()


@router.get("/seasons-teams")
async def get_seasons_teams(
    season: Optional[int] = Query(None),
    division: Optional[int] = Query(None),
    limit: int = Query(2000, ge=1, le=10000),
) -> Dict[str, Any]:
    """Return seasons list and teams list in a single response."""
    seasons_task = asyncio.create_task(seasons_service.get_seasons_list())
    teams_task = asyncio.create_task(
        teams_service.list_teams(season=season, division=division, limit=limit)
    )
    seasons_result, teams_result = await asyncio.gather(
        seasons_task, teams_task, return_exceptions=True
    )
    return {
        "ok": True,
        "seasons": seasons_result if not isinstance(seasons_result, Exception) else [],
        "teams": teams_result if not isinstance(teams_result, Exception) else [],
    }


@router.get("/seasons-players")
async def get_seasons_players(
    season: Optional[int] = Query(None),
    division: Optional[int] = Query(None),
    limit: int = Query(2000, ge=1, le=10000),
) -> Dict[str, Any]:
    """Return seasons list and players list in a single response."""
    seasons_task = asyncio.create_task(seasons_service.get_seasons_list())
    players_task = asyncio.create_task(
        players_service.list_players(season=season, division=division, limit=limit)
    )
    seasons_result, players_result = await asyncio.gather(
        seasons_task, players_task, return_exceptions=True
    )
    return {
        "ok": True,
        "seasons": seasons_result if not isinstance(seasons_result, Exception) else [],
        "players": players_result if not isinstance(players_result, Exception) else [],
    }


@router.get("/seasons-elo")
async def get_seasons_elo(
    season: Optional[int] = Query(None),
    division: Optional[int] = Query(None),
    limit: int = Query(2000, ge=1, le=10000),
    include_seasons: bool = Query(True),
    include_config: bool = Query(True),
) -> Dict[str, Any]:
    """Return seasons list and current player Elo leaderboard in a single response."""
    seasons_task = (
        asyncio.create_task(seasons_service.get_seasons_list())
        if include_seasons else None
    )
    # Elo remains all-time; optional season filters only participants included in that season.
    leaderboard_task = asyncio.create_task(
        elo_service.get_elo_leaderboard(
            division=division,
            participation_season=season,
            limit=limit,
        )
    )
    gathered = await asyncio.gather(
        *(task for task in (seasons_task, leaderboard_task) if task is not None),
        return_exceptions=True,
    )
    if include_seasons:
        seasons_result, leaderboard_result = gathered
    else:
        seasons_result = []
        leaderboard_result = gathered[0] if gathered else []

    return {
        "ok": True,
        "seasons": seasons_result if not isinstance(seasons_result, Exception) else [],
        "players": leaderboard_result if not isinstance(leaderboard_result, Exception) else [],
        "elo_config": elo_service.get_public_elo_config() if include_config else {},
    }


@router.get("/seasons-upcoming")
async def get_seasons_upcoming(
    championship_id: Optional[str] = Query(None),
    season: Optional[int] = Query(None),
    include_playoffs: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Return seasons list and upcoming matches in a single response."""
    seasons_task = asyncio.create_task(seasons_service.get_seasons_list())
    upcoming_task = asyncio.create_task(
        matches_service.get_upcoming_matches(
            championship_id=championship_id,
            team_id=None,
            season=season,
            include_playoffs=include_playoffs,
            limit=limit,
            offset=offset,
        )
    )
    seasons_result, upcoming_result = await asyncio.gather(
        seasons_task, upcoming_task, return_exceptions=True
    )
    if isinstance(upcoming_result, Exception):
        upcoming_items, upcoming_total = [], 0
    else:
        upcoming_items, upcoming_total = upcoming_result[0], upcoming_result[1]

    return {
        "ok": True,
        "seasons": seasons_result if not isinstance(seasons_result, Exception) else [],
        "upcoming": upcoming_items,
        "total": upcoming_total,
    }
