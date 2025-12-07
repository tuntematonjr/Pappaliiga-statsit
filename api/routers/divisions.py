"""Division and season API endpoints."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel, PaginationMeta
from api.services import divisions_service

router = APIRouter()


class SeasonInfo(CamelModel):
    season: int
    divisions: List[int]
    championship_ids: List[str]


class DivisionSummary(CamelModel):
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    parent_championship_id: Optional[str] = None
    teams_count: int | None = 0
    played_matches: int | None = 0
    total_matches: int | None = 0
    last_updated: Optional[str] = None
    updated: Optional[str] = None
    updated_at: Optional[str] = None
    updated_ts: Optional[int] = None
    tier: Optional[str] = None
    kind: Optional[str] = None
    status: Optional[str] = None
    progress_percent: Optional[float] = None
    finished_matches: int | None = 0
    live_matches: int | None = 0
    upcoming_matches: int | None = 0
    start: Optional[str] = None
    start_ts: Optional[int] = None
    end: Optional[str] = None
    end_ts: Optional[int] = None
    first_started_at: Optional[str] = None
    first_started_ts: Optional[int] = None
    first_scheduled_at: Optional[str] = None
    first_scheduled_ts: Optional[int] = None
    last_finished_at: Optional[str] = None
    last_finished_ts: Optional[int] = None
    last_scheduled_at: Optional[str] = None
    last_scheduled_ts: Optional[int] = None
    last_activity_at: Optional[str] = None
    last_activity_ts: Optional[int] = None
    winner_team_id: Optional[str] = None
    winner_team_name: Optional[str] = None
    winner: Optional[str] = None


class TeamBasic(CamelModel):
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    matches_played: int = 0
    matches_won: int = 0
    matches_lost: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    match_win_rate: float = 0.0
    maps_played: int = 0
    maps_won: int = 0
    maps_lost: int = 0
    rounds_won: int = 0
    rounds_lost: int = 0
    rounds_diff: int = 0
    kills: int = 0
    deaths: int = 0
    kd: float = 0.0
    adr: float = 0.0
    damage: int = 0
    players: Optional[List[dict]] = None


class MapVoteStats(CamelModel):
    map_name: Optional[str]
    pretty_name: Optional[str]
    image_sm: Optional[str]
    maps_played: int
    banned: int
    kills: int
    deaths: int
    damage: int
    rounds_played: int
    adr: float
    kr: float
    udpr: float
    enemy_flash: float
    sniper_kills: int
    assists: int
    k2: int
    k3: int
    k4: int
    ace: int
    pistol_kills: int
    pick_rate: float


class DivisionAggregates(CamelModel):
    played_matches: int
    total_matches: int
    forfeits: int


class DivisionPlayerTotals(CamelModel):
    player_id: str
    team_id: Optional[str]
    team_name: Optional[str]
    nickname: Optional[str]
    avatar: Optional[str]
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    adr: float
    kr: float
    kd: float
    rating: float
    hs_pct: float
    mvps: int
    pistol_kills: int
    sniper_kills: int
    utility_damage: int
    enemies_flashed: int
    flash_count: int
    flash_successes: int
    clutch_kills: int
    cl_1v1_attempts: int
    cl_1v1_wins: int
    cl_1v2_attempts: int
    cl_1v2_wins: int
    damage: int


class DivisionLeader(CamelModel):
    player_id: Optional[str]
    team_id: Optional[str]
    team_name: Optional[str]
    nickname: Optional[str]
    kills: int
    deaths: int
    adr: float
    kr: float
    rating: float
    mvps: int
    utility_damage: int


class DivisionDetails(CamelModel):
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    parent_championship_id: Optional[str] = None
    teams: List[TeamBasic]
    excluded_team_ids: List[str]
    map_stats: Optional[List[MapVoteStats]] = None
    aggregates: Optional[DivisionAggregates] = None
    leaders: Optional[List[DivisionLeader]] = None
    player_count: int | None = None
    season_player_count: int | None = None
    all_time_player_count: int | None = None
    player_totals: Optional[List[DivisionPlayerTotals]] = None


class DivisionListResponse(CamelModel):
    items: List[DivisionSummary]
    meta: PaginationMeta


@router.get("/seasons", response_model=List[SeasonInfo])
async def get_seasons():
    rows = await divisions_service.fetch_seasons()
    return [SeasonInfo(**row) for row in rows]


@router.get("", response_model=DivisionListResponse)
async def get_all_divisions(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    rows, total = await asyncio.gather(
        divisions_service.list_divisions(limit, offset),
        divisions_service.count_divisions(),
    )
    champ_ids = [str(row["championship_id"]) for row in rows if row.get("championship_id")]
    winners_map = await divisions_service.fetch_division_winners(champ_ids)
    items = [
        _normalize_division_summary(row, winners_map.get(str(row["championship_id"])))
        for row in rows
    ]
    return DivisionListResponse(
        items=items,
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/season/{season}", response_model=DivisionListResponse)
async def get_divisions_by_season(
    season: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    rows, total = await asyncio.gather(
        divisions_service.list_divisions_by_season(season, limit, offset),
        divisions_service.count_divisions(season=season),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No divisions found for season {season}")
    champ_ids = [str(row["championship_id"]) for row in rows if row.get("championship_id")]
    winners_map = await divisions_service.fetch_division_winners(champ_ids)
    items = [
        _normalize_division_summary(row, winners_map.get(str(row["championship_id"])))
        for row in rows
    ]
    return DivisionListResponse(
        items=items,
        meta=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/by-slug/{slug}", response_model=DivisionDetails)
async def get_division_by_slug(slug: str):
    try:
        champ_row = await divisions_service.fetch_division_by_slug(slug)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    details = await divisions_service.get_division_details(champ_row)
    return DivisionDetails(**details)


@router.get("/{championship_id}", response_model=DivisionDetails)
async def get_division_by_id(championship_id: str):
    try:
        champ_row = await divisions_service.fetch_division_by_id(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    details = await divisions_service.get_division_details(champ_row)
    return DivisionDetails(**details)


def _coerce_epoch_ms(value: Any) -> Optional[int]:
    if value in (None, 0):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return int(value.timestamp() * 1000)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp() * 1000)
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if abs(numeric) < 1_000_000_000_000:
        numeric *= 1000
    return numeric


def _iso_from_epoch(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _compute_progress_percent(played: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(min(100.0, (played / total) * 100), 1)


def _determine_kind(payload: dict[str, Any]) -> str:
    if bool(payload.get("is_playoff")):
        return "playoffs"
    division_num = int(payload.get("division_num") or 0)
    if division_num == 0:
        return "masters"
    return "division"


def _determine_status(
    played: int,
    total: int,
    finished: int,
    live_count: int,
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> str:
    now_ms = int(time.time() * 1000)
    if total > 0 and played >= total:
        return "ended"
    if total > 0 and finished >= total:
        return "ended"
    if end_ms and end_ms < now_ms and total > 0 and played >= total:
        return "ended"
    if live_count > 0:
        return "running"
    if played > 0 and (total == 0 or played < total):
        return "running"
    if start_ms and start_ms <= now_ms:
        return "running"
    return "upcoming"


def _normalize_division_summary(row: dict[str, Any], winner: Optional[dict[str, Any]] = None) -> DivisionSummary:
    data = dict(row)
    data["is_playoff"] = bool(data.get("is_playoff"))

    data["teams_count"] = int(data.get("teams_count") or 0)
    played = int(data.get("played_matches") or 0)
    total = int(data.get("total_matches") or 0)
    finished = int(data.get("finished_matches") or played)
    live_count = int(data.get("live_matches") or 0)
    data["upcoming_matches"] = int(data.get("upcoming_matches") or 0)

    raw_first_started = data.get("first_started_at")
    raw_first_scheduled = data.get("first_scheduled_at")
    raw_last_finished = data.get("last_finished_at")
    raw_last_scheduled = data.get("last_scheduled_at")
    raw_last_activity = data.get("last_activity_ts")
    raw_last_updated = data.get("last_updated")

    first_started_ms = _coerce_epoch_ms(raw_first_started)
    first_scheduled_ms = _coerce_epoch_ms(raw_first_scheduled)
    last_finished_ms = _coerce_epoch_ms(raw_last_finished)
    last_scheduled_ms = _coerce_epoch_ms(raw_last_scheduled)
    last_activity_ms = _coerce_epoch_ms(raw_last_activity)
    updated_ms = _coerce_epoch_ms(raw_last_updated)

    start_ms = first_started_ms or first_scheduled_ms
    end_ms = last_finished_ms or last_scheduled_ms

    data["first_started_ts"] = first_started_ms
    data["first_scheduled_ts"] = first_scheduled_ms
    data["last_finished_ts"] = last_finished_ms
    data["last_scheduled_ts"] = last_scheduled_ms
    data["last_activity_ts"] = last_activity_ms
    data["start_ts"] = start_ms
    data["end_ts"] = end_ms
    data["updated_ts"] = updated_ms

    data["first_started_at"] = _iso_from_epoch(first_started_ms)
    data["first_scheduled_at"] = _iso_from_epoch(first_scheduled_ms)
    data["last_finished_at"] = _iso_from_epoch(last_finished_ms)
    data["last_scheduled_at"] = _iso_from_epoch(last_scheduled_ms)
    data["last_activity_at"] = _iso_from_epoch(last_activity_ms)
    data["start"] = _iso_from_epoch(start_ms)
    data["end"] = _iso_from_epoch(end_ms)
    data["updated"] = _iso_from_epoch(updated_ms)
    data["updated_at"] = data["updated"]
    data["last_updated"] = data["updated"]

    data["played_matches"] = played
    data["total_matches"] = total
    data["finished_matches"] = finished
    data["live_matches"] = live_count
    data["progress_percent"] = _compute_progress_percent(played, total)

    data["kind"] = _determine_kind(data)
    if data.get("tier") is None:
        data["tier"] = data["kind"]

    status = _determine_status(played, total, finished, live_count, start_ms, end_ms)
    data["status"] = status

    if winner:
        data["winner_team_id"] = winner.get("team_id")
        data["winner_team_name"] = winner.get("team_name")
        data["winner"] = winner.get("team_name") or winner.get("team_id")

    return DivisionSummary(**data)
