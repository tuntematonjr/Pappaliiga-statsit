"""Division API endpoints."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import divisions_service

router = APIRouter()


class TeamBasic(CamelModel):
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    status: Optional[str] = None
    status_reason: Optional[str] = None
    status_note: Optional[str] = None
    status_effective_at: Optional[int] = None
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
    ban1: int = 0
    ban2: int = 0
    decov: int = 0
    picks: int = 0
    opp_picks: int = 0
    pick_wins: int = 0
    opp_pick_wins: int = 0
    pick_win_rate: float = 0.0
    opp_pick_win_rate: float = 0.0
    kills: int
    deaths: int
    damage: int
    rounds_played: int
    adr: float
    kr: float
    hs_pct: float = 0.0
    udpr: float
    enemy_flash: float
    enemies_flashed: int = 0
    flash_count: int = 0
    flash_successes: int = 0
    sniper_kills: int
    assists: int
    mvps: int = 0
    k2: int
    k3: int
    k4: int
    ace: int
    pistol_kills: int
    clutch_kills: int = 0
    pick_rate: float


class DivisionAggregates(CamelModel):
    played_matches: int
    total_matches: int
    forfeits: int
    team_count: int | None = None


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
    hs_pct: float
    mvps: int
    pistol_kills: int
    sniper_kills: int
    knife_kills: int
    zeus_kills: int
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
    excluded_teams: Optional[List[dict]] = None
    map_stats: Optional[List[MapVoteStats]] = None
    aggregates: Optional[DivisionAggregates] = None
    leaders: Optional[List[DivisionLeader]] = None
    player_count: int | None = None
    season_player_count: int | None = None
    all_time_player_count: int | None = None
    player_totals: Optional[List[DivisionPlayerTotals]] = None


@router.get("/{championship_id}", response_model=DivisionDetails)
async def get_division_by_id(championship_id: str):
    try:
        champ_row = await divisions_service.fetch_division_by_id(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    details = await divisions_service.get_division_details(champ_row)
    return DivisionDetails(**details)
