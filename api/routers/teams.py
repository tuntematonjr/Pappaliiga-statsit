"""Team API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import teams_service
from db_async import query_async

router = APIRouter()


class TeamInfo(CamelModel):
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    faceit_url: Optional[str]


class TeamSeasonStats(CamelModel):
    season: int
    division_num: int
    championship_id: str
    name: Optional[str] = None
    is_playoffs: Optional[bool] = None
    maps_played: int
    matches_played: int
    wins: int
    losses: int
    win_rate: float
    rounds_won: int
    rounds_lost: int
    maps_won: int
    rounds_diff: Optional[int] = None


class MapVeto(CamelModel):
    match_id: str
    map_name: str
    status: str
    selected_by_team_id: Optional[str]
    selected_by_team_name: Optional[str]
    round_num: Optional[int]
    order: int


class TeamMapStats(CamelModel):
    map_name: str
    played: int
    picks: int
    opp_picks: int
    pick_wins: Optional[int] = None
    opp_pick_wins: Optional[int] = None
    wins: int
    games: int
    ban1: int
    ban2: int
    opp_ban: int
    total_own_ban: int
    decov: int
    kills: int
    deaths: int
    mvps: int
    rd: int
    kd: float
    adr: float
    damage: int
    utility_damage: int
    winrate: Optional[float] = None
    pick_rate: Optional[float] = None
    udpr: Optional[float] = None
    total_rounds_played: Optional[int] = None
    rounds_won: Optional[int] = None
    rounds_lost: Optional[int] = None
    # Player stats aggregated by map
    assists: Optional[int] = None
    kr: Optional[float] = None
    hs_pct: Optional[float] = None
    sniper_kills: Optional[int] = None
    pistol_kills: Optional[int] = None
    multi_2k: Optional[int] = None
    multi_3k: Optional[int] = None
    multi_4k: Optional[int] = None
    multi_5k: Optional[int] = None
    clutch_kills: Optional[int] = None
    enemies_flashed: Optional[int] = None
    flash_count: Optional[int] = None
    flash_successes: Optional[int] = None


class MatchFull(CamelModel):
    match_id: str
    ts: int
    status: str
    best_of: int
    played: int
    is_forfeit: Optional[bool] = None
    winner_team_id: Optional[str] = None
    team1_id: str
    team2_id: str
    team1_name: str
    team2_name: str
    t1_avatar: Optional[str]
    t2_avatar: Optional[str]
    faceit_url: Optional[str]
    maps: Optional[List[Dict[str, Any]]]
    opponent_name: Optional[str] = None


class PlayerStats(CamelModel):
    player_id: str
    nickname: str
    maps_played: int
    rounds_played: int
    kills: int
    deaths: int
    assists: int
    mvps: int
    sniper_kills: int
    utility_damage: int
    enemies_flashed: int
    flash_count: int
    flash_successes: int
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
    pistol_kills: int
    adr: float
    kr: float
    kd: float
    hs_pct: float
    damage: int


class VetoBanAggregate(CamelModel):
    map_name: str
    times_banned: int
    times_picked: int
    times_opponent_picked: int
    ban_rate: float
    pick_rate: float
    pick_win_rate: Optional[float] = None


class DivisionAverages(CamelModel):
    avg_win_rate: float
    avg_round_diff: float
    avg_map_win_rate: float


class PlayerRoleStats(CamelModel):
    awp_rate: float
    entry_success: float
    assist_rate: float
    clutch_success: float


class PlayerWithRole(CamelModel):
    player_id: str
    nickname: Optional[str]
    maps_played: int
    roles: List[str]
    primary_role: str
    role_stats: PlayerRoleStats


class ComprehensiveTeamSeasonData(CamelModel):
    championship_id: str
    season: int
    division_num: int
    team_stats: Optional[TeamSeasonStats] = None
    map_stats: List[TeamMapStats] = []
    match_history: List[MatchFull] = []
    player_stats: List[PlayerStats] = []
    veto_history: List[MapVeto] = []
    veto_aggregates: List[VetoBanAggregate] = []
    # Phase 1 enhancements
    division_averages: Optional[DivisionAverages] = None
    player_roles: Optional[List[PlayerWithRole]] = None


class TeamPageResponse(CamelModel):
    team: TeamInfo
    seasons: List[TeamSeasonStats] = []
    current_championship_id: Optional[str]
    current_season: Optional[int] = None
    current_division: Optional[int] = None
    season_data: Optional[ComprehensiveTeamSeasonData] = None


@router.get("/{team_id}", response_model=TeamInfo)
async def get_team_info(team_id: str):
    try:
        team = await teams_service.fetch_team(team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TeamInfo(**team)


@router.get("/{team_id}/seasons", response_model=List[TeamSeasonStats])
async def get_team_season_stats(team_id: str):
    try:
        rows = await teams_service.fetch_team_season_stats(team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [TeamSeasonStats(**row) for row in rows]


@router.get("/{team_id}/page", response_model=TeamPageResponse)
async def get_team_page(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID"),
):
    """Get team page overview (basic profile + season list + season data if championship provided)."""
    try:
        # Get basic team info and seasons
        team = await teams_service.fetch_team(team_id)
        
        try:
            seasons = await teams_service.fetch_team_season_stats(team_id)
        except NotFoundError:
            seasons = []
        
        # Determine which championship to load
        available_champs = {row.get("championship_id") for row in seasons if row.get("championship_id")}
        selected_champ = championship_id or None
        
        if selected_champ:
            if selected_champ not in available_champs and available_champs:
                raise NotFoundError(f"Championship {selected_champ} not found for team '{team_id}'")
        elif available_champs:
            # Default to most recent championship
            selected_champ = seasons[0]["championship_id"]
        
        # Fetch comprehensive season data if championship available
        season_data = None
        if selected_champ:
            try:
                season_data = await teams_service.fetch_comprehensive_team_season(team_id, selected_champ)
            except NotFoundError:
                season_data = None

        current_season = None
        current_division = None
        if selected_champ:
            selected_row = next((s for s in seasons if s.get("championship_id") == selected_champ), None)
            if selected_row:
                current_season = selected_row.get("season")
                current_division = selected_row.get("division_num")
        
        return {
            "team": team,
            "seasons": seasons,
            "current_championship_id": selected_champ,
            "current_season": current_season,
            "current_division": current_division,
            "season_data": season_data,
        }
        
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{team_id}/season/{championship_id}", response_model=ComprehensiveTeamSeasonData)
async def get_team_season_comprehensive(team_id: str, championship_id: str):
    """Get comprehensive team season data including stats, maps, matches, players, and veto history."""
    try:
        data = await teams_service.fetch_comprehensive_team_season(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ComprehensiveTeamSeasonData(**data)


@router.get("/{team_id}/matches", response_model=List[MatchFull])
async def get_team_matches(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID")
):
    """Get team's matches. If championship_id not provided, uses latest."""
    try:
        rows = await teams_service.fetch_team_matches(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [MatchFull(**row) for row in rows]


@router.get("/{team_id}/players", response_model=List[PlayerStats])
async def get_team_players(
    team_id: str,
    championship_id: Optional[str] = Query(None, description="Championship ID")
):
    """Get team's players with complete stats. If championship_id not provided, uses latest."""
    try:
        rows = await teams_service.fetch_team_players_comprehensive(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [PlayerStats(**row) for row in rows]



@router.get("/{team_id}/map-stats/{championship_id}", response_model=List[TeamMapStats])
async def get_team_map_stats(team_id: str, championship_id: str):
    """Get detailed team map statistics for a specific championship."""
    try:
        rows = await teams_service.fetch_team_map_stats_comprehensive(championship_id, team_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [TeamMapStats(**row) for row in rows]


@router.get("/{team_id}/veto-history/{championship_id}", response_model=List[MapVeto])
async def get_team_veto_history(team_id: str, championship_id: str):
    """Get team's map veto/pick history for a championship."""
    try:
        rows = await teams_service.fetch_team_veto_history(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [MapVeto(**row) for row in rows]


@router.get("/{team_id}/veto-aggregates/{championship_id}", response_model=List[VetoBanAggregate])
async def get_team_veto_aggregates(team_id: str, championship_id: str):
    """Get aggregated veto/ban statistics for a team in a championship."""
    try:
        rows = await teams_service.fetch_team_veto_aggregates(team_id, championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [VetoBanAggregate(**row) for row in rows]


@router.get("/", response_model=List[TeamInfo])
async def list_teams(
    season: Optional[int] = Query(None, description="Filter by season"),
    division: Optional[int] = Query(None, description="Filter by division"),
    limit: int = Query(100, ge=1, le=500),
):
    rows = await teams_service.list_teams(season=season, division=division, limit=limit)
    return [TeamInfo(**row) for row in rows]
