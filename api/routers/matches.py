"""Match API endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response

from api.exceptions import NotFoundError
from api.models import CamelModel, PaginationMeta
from api.services import matches_service

router = APIRouter()


class MatchSummary(CamelModel):
    match_id: str
    championship_id: str
    finished_at: Optional[int]
    team1_id: Optional[str]
    team2_id: Optional[str]
    team1_name: Optional[str]
    team2_name: Optional[str]
    team1_score: int = 0
    team2_score: int = 0
    is_forfeit: bool
    ignored_due_ban: bool


class MapResult(CamelModel):
    map_number: Optional[int]
    map_name: Optional[str]
    score_team1: int
    score_team2: int
    winner_team_id: Optional[str]
    is_forfeit_map: bool


class MatchDetails(CamelModel):
    match_id: str
    championship_id: str
    finished_at: Optional[int]
    team1_id: Optional[str]
    team2_id: Optional[str]
    team1_name: Optional[str]
    team2_name: Optional[str]
    team1_avatar: Optional[str]
    team2_avatar: Optional[str]
    team1_score: int = 0
    team2_score: int = 0
    is_forfeit: bool
    ignored_due_ban: bool
    maps: List[MapResult]


class PlayerMapStats(CamelModel):
    round_index: int
    map_id: Optional[int]
    map_name: Optional[str]
    player_id: str
    nickname: Optional[str]
    team_id: Optional[str]
    opponent_team_id: Optional[str]
    is_forfeit_map: bool
    stats: Dict[str, Any]


class MatchListResponse(CamelModel):
    items: List[MatchSummary]
    meta: PaginationMeta


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


@router.get("/{match_id}", response_model=MatchDetails)
async def get_match_details(match_id: str):
    try:
        payload = await matches_service.get_match_details(match_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    match = payload["match"]
    map_rows = payload["maps"]
    team1_id = match.get("team1_id")
    team2_id = match.get("team2_id")
    team1_score = sum(1 for row in map_rows if team1_id and row.get("winner_team_id") == team1_id)
    team2_score = sum(1 for row in map_rows if team2_id and row.get("winner_team_id") == team2_id)

    return MatchDetails(
        match_id=match["match_id"],
        championship_id=match["championship_id"],
        finished_at=match.get("finished_at"),
        team1_id=team1_id,
        team2_id=team2_id,
        team1_name=match.get("team1_name"),
        team2_name=match.get("team2_name"),
        team1_avatar=match.get("team1_avatar"),
        team2_avatar=match.get("team2_avatar"),
        team1_score=int(team1_score),
        team2_score=int(team2_score),
        is_forfeit=bool(match.get("is_forfeit", False)),
        ignored_due_ban=bool(match.get("ignored_due_ban", False)),
        maps=[
            MapResult(
                map_number=int(row["round_index"]) if row.get("round_index") is not None else None,
                map_name=row.get("map_name"),
                score_team1=int(row.get("score_team1") or 0),
                score_team2=int(row.get("score_team2") or 0),
                winner_team_id=row.get("winner_team_id"),
                is_forfeit_map=bool(row.get("is_forfeit", False)),
            )
            for row in map_rows
        ],
    )


@router.get("/{match_id}/players", response_model=List[PlayerMapStats])
async def get_match_player_stats(match_id: str):
    try:
        rows = await matches_service.get_match_player_stats(match_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [PlayerMapStats(**row) for row in rows]
