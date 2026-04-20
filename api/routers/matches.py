"""Match API endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Query, Response

from api.models import CamelModel, PaginationMeta
from api.services import matches_service

router = APIRouter()


class MatchSummary(CamelModel):
    match_id: str
    championship_id: str
    best_of: Optional[int] = None
    status: Optional[str] = None
    finished_at: Optional[int]
    team1_id: Optional[str]
    team2_id: Optional[str]
    team1_name: Optional[str]
    team2_name: Optional[str]
    team1_score: int = 0
    team2_score: int = 0
    is_forfeit: bool
    ignored_due_ban: bool


class MatchListResponse(CamelModel):
    items: List[MatchSummary]
    meta: PaginationMeta


class UpcomingMatchSummary(CamelModel):
    match_id: str
    championship_id: str
    season: int
    division_num: int
    is_playoffs: bool
    division_name: Optional[str] = None
    division_slug: Optional[str] = None
    status: Optional[str] = None
    scheduled_ts: Optional[int] = None
    scheduled_at: Optional[str] = None
    team1_id: Optional[str] = None
    team2_id: Optional[str] = None
    team1_name: Optional[str] = None
    team2_name: Optional[str] = None
    team1_avatar: Optional[str] = None
    team2_avatar: Optional[str] = None
    ignored_due_ban: bool = False
    faceit_url: Optional[str] = None


class UpcomingMatchListResponse(CamelModel):
    items: List[UpcomingMatchSummary]
    meta: PaginationMeta


class DemoLinkItem(CamelModel):
    demo_index: int
    url: str
    map_name: Optional[str] = None


class DemoListResponse(CamelModel):
    championship_id: str
    match_id: str
    items: List[DemoLinkItem]


class MatchBundleResponse(CamelModel):
    details: dict
    player_stats: list[dict]


def _set_revision_headers(response: Response, *, etag: str, revision: Optional[str]) -> None:
    response.headers["ETag"] = etag
    if revision:
        if isinstance(revision, datetime):
            response.headers["Last-Modified"] = revision.strftime("%a, %d %b %Y %H:%M:%S GMT")
        else:
            response.headers["X-Revision"] = str(revision)


@router.get("/division/{championship_id}", response_model=MatchListResponse)
async def get_division_matches(
    championship_id: str,
    response: Response,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    items, total, revision, etag = await matches_service.get_division_matches(
        championship_id,
        limit=limit,
        offset=offset,
    )
    _set_revision_headers(response, etag=etag, revision=revision)
    return MatchListResponse(
        items=[MatchSummary(**row) for row in items],
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/upcoming", response_model=UpcomingMatchListResponse)
async def get_upcoming_matches(
    response: Response,
    championship_id: Optional[str] = Query(None),
    team_id: Optional[str] = Query(None),
    season: Optional[int] = Query(None),
    include_playoffs: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    items, total, revision, etag = await matches_service.get_upcoming_matches(
        championship_id=championship_id,
        team_id=team_id,
        season=season,
        include_playoffs=include_playoffs,
        limit=limit,
        offset=offset,
    )
    _set_revision_headers(response, etag=etag, revision=revision)
    return UpcomingMatchListResponse(
        items=[UpcomingMatchSummary(**row) for row in items],
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{match_id}/demos", response_model=DemoListResponse)
async def get_match_demos(
    match_id: str,
    championship_id: str = Query(..., min_length=1),
    expected_count: Optional[int] = Query(None, ge=1, le=12),
    force: bool = Query(False),
):
    payload = await matches_service.get_match_demos(
        championship_id,
        match_id,
        expected_count=expected_count,
        force=force,
    )
    return DemoListResponse(
        championship_id=championship_id,
        match_id=match_id,
        items=[DemoLinkItem(**row) for row in (payload.get("items") or [])],
    )


@router.get("/{match_id}/bundle", response_model=MatchBundleResponse)
async def get_match_bundle(match_id: str):
    payload = await matches_service.get_match_bundle(match_id)
    return MatchBundleResponse(
        details=payload.get("details") or {},
        player_stats=payload.get("player_stats") or [],
    )
