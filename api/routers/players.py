"""Player API endpoints."""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import players_service

router = APIRouter()


class PlayerInfo(CamelModel):
    player_id: str
    nickname: str
    avatar: Optional[str]
    faceit_url: Optional[str]
    championship_id: Optional[str] = None


class PlayerSeasonStats(CamelModel):
    season: int
    division_num: int
    championship_id: str
    is_playoffs: bool = False
    team_id: str
    team_name: Optional[str]
    team_avatar: Optional[str] = None
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    mvps: int
    headshots: int
    damage: int
    sniper_kills: int
    pistol_kills: int
    knife_kills: int
    zeus_kills: int
    first_kills: int
    enemies_flashed: int
    flash_count: int
    flash_successes: int
    utility_damage: int
    utility_count: int
    utility_successes: int
    utility_enemies: int
    mk_2k: int
    mk_3k: int
    mk_4k: int
    mk_5k: int
    clutch_kills: int
    cl_1v1_attempts: int
    cl_1v1_wins: int
    cl_1v2_attempts: int
    cl_1v2_wins: int
    entry_count: int
    entry_wins: int
    kd: float
    kr: float
    adr: float
    hs_pct: float


class PlayerSeasonProgressPoint(CamelModel):
    snapshot_ts: int
    snapshot_time: Optional[datetime] = None
    match_played_at: Optional[datetime] = None
    round_index: Optional[int] = None
    match_id: Optional[str] = None
    match_team1_id: Optional[str] = None
    match_team2_id: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    opponent_team_id: Optional[str] = None
    opponent_team_name: Optional[str] = None
    matchup: Optional[str] = None
    result: Optional[str] = None
    match_is_playoffs: Optional[bool] = None
    map_names_csv: Optional[str] = None
    map_scores_csv: Optional[str] = None
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    mvps: int
    headshots: int
    sniper_kills: int
    pistol_kills: int
    knife_kills: int
    zeus_kills: int
    first_kills: int
    enemies_flashed: int
    flash_count: int
    flash_successes: int
    utility_damage: int
    utility_count: int
    utility_successes: int
    utility_enemies: int
    mk_2k: int
    mk_3k: int
    mk_4k: int
    mk_5k: int
    clutch_kills: int
    cl_1v1_attempts: int
    cl_1v1_wins: int
    cl_1v2_attempts: int
    cl_1v2_wins: int
    entry_count: int
    entry_wins: int
    kd: float
    adr: float
    kr: float
    hs_pct: float
    damage: int


class PlayerMapStatsWithDelta(CamelModel):
    map_name: str
    curr: Dict[str, Any]
    prev: Optional[Dict[str, Any]]
    delta: Optional[Dict[str, Any]]
    snapshot_ts: Optional[int]


class PlayerBundleResponse(CamelModel):
    player: PlayerInfo
    seasons: List[PlayerSeasonStats]
    selected_championship_id: Optional[str] = None
    selected_season: Optional[PlayerSeasonStats] = None
    map_stats: List[PlayerMapStatsWithDelta] = []
    progression: List[PlayerSeasonProgressPoint] = []


@router.get("", response_model=List[PlayerInfo])
async def list_players(
    season: Optional[int] = Query(None, description="Season filter"),
    division: Optional[int] = Query(None, description="Division filter"),
    limit: int = Query(2000, ge=1, le=10000, description="Maximum number of players"),
):
    """List players with optional season/division filters."""
    try:
        rows = await players_service.list_players(season=season, division=division, limit=limit)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [PlayerInfo(**row) for row in rows]


@router.get("/{player_id}/bundle", response_model=PlayerBundleResponse, response_model_by_alias=False)
async def get_player_bundle(
    player_id: str,
    championship_id: Optional[str] = Query(None, description="Optional selected championship id"),
):
    # fetch_player and fetch_player_season_stats are both cached — run in parallel.
    try:
        player_row, seasons_rows = await asyncio.gather(
            players_service.fetch_player(player_id),
            players_service.fetch_player_season_stats(player_id),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    seasons = [PlayerSeasonStats(**row) for row in seasons_rows]
    selected_row: Optional[PlayerSeasonStats] = None

    if seasons:
        if championship_id:
            selected_row = next(
                (row for row in seasons if str(row.championship_id) == str(championship_id)),
                None,
            )
            if selected_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Championship '{championship_id}' not found for player '{player_id}'",
                )
        else:
            selected_row = seasons[0]

    map_stats: List[PlayerMapStatsWithDelta] = []
    progression: List[PlayerSeasonProgressPoint] = []
    if selected_row is not None:
        # map_stats and progression are independent — run in parallel.
        map_result, prog_result = await asyncio.gather(
            players_service.fetch_player_map_stats(selected_row.championship_id, player_id),
            players_service.fetch_player_season_progression(
                player_id,
                selected_row.season,
                selected_row.division_num,
                championship_id=selected_row.championship_id,
            ),
            return_exceptions=True,
        )
        map_stats = (
            [PlayerMapStatsWithDelta(**row) for row in map_result]
            if not isinstance(map_result, Exception)
            else []
        )
        progression = (
            [PlayerSeasonProgressPoint(**row) for row in prog_result]
            if not isinstance(prog_result, Exception)
            else []
        )

    return PlayerBundleResponse(
        player=PlayerInfo(**player_row),
        seasons=seasons,
        selected_championship_id=(selected_row.championship_id if selected_row else None),
        selected_season=selected_row,
        map_stats=map_stats,
        progression=progression,
    )
