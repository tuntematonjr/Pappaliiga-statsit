"""Seasons API endpoints for aggregated season data."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException

from api.models import CamelModel
from api.services import seasons_service

router = APIRouter()


class SeasonListItem(CamelModel):
    id: int
    name: str
    status: str
    start_date: str | None
    end_date: str | None
    divisions_count: int


class ProgressInfo(CamelModel):
    divisions_finished: int
    divisions_total: int


class SeasonSummary(CamelModel):
    season_id: int
    divisions: int
    teams: int
    players: int
    matches: int
    maps: int
    rounds: int
    kills: int
    deaths: int
    win_rate: float
    kd_ratio: float
    adr_avg: float
    clutch_wins: int
    entry_diff: int
    utility_damage: int
    finished_percent: float
    progress: ProgressInfo


class SeasonStats(CamelModel):
    teams: int
    matches_played: int
    matches_total: int
    finished_percent: float


class PlayoffStats(CamelModel):
    status: str
    teams: int
    matches_played: int
    matches_total: int
    winner: str | None


class WinnerInfo(CamelModel):
    team_name: str
    place: int


class BestPlayer(CamelModel):
    name: str
    rating: float


class DivisionWithStats(CamelModel):
    division_id: str
    tier: int
    name: str
    status: str
    is_playoff: bool
    season: SeasonStats | None = None
    playoffs: PlayoffStats | None = None
    parent_championship_id: str | None = None
    winners: List[WinnerInfo]
    best_player: BestPlayer | None = None
    mvp_team: str | None = None


class TeamStats(CamelModel):
    name: str
    matches: int
    wins: int
    losses: int
    rounds: int
    kills: int
    deaths: int
    adr: float


class PlayerLeader(CamelModel):
    player: str
    value: float | int


class PlayerLeadersGroup(CamelModel):
    top_frags: PlayerLeader | None
    best_kd: PlayerLeader | None
    most_mvps: PlayerLeader | None


class BracketMatch(CamelModel):
    round: int
    match_id: str
    team1: str | None
    team2: str | None
    winner: str | None


class PlayoffBracket(CamelModel):
    matches_played: int
    matches_total: int
    bracket: List[BracketMatch]


class SeasonSection(CamelModel):
    teams: List[TeamStats]
    player_leaders: PlayerLeadersGroup


class DivisionDetailedStats(CamelModel):
    division_id: str
    season: SeasonSection
    playoffs: PlayoffBracket


@router.get("", response_model=List[SeasonListItem])
async def list_seasons():
    """Return list of all seasons with metadata."""
    try:
        seasons = await seasons_service.get_seasons_list()
        return [SeasonListItem(**s) for s in seasons]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{season_id}/summary", response_model=SeasonSummary)
async def get_season_summary(season_id: int):
    """Return aggregated statistics for a specific season."""
    try:
        summary = await seasons_service.get_season_summary(season_id)
        return SeasonSummary(**summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{season_id}/divisions", response_model=List[DivisionWithStats])
async def get_season_divisions(season_id: int):
    """Return list of divisions for a season with embedded season and playoff stats."""
    try:
        divisions = await seasons_service.get_season_divisions(season_id)
        return [DivisionWithStats(**d) for d in divisions]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{season_id}/divisions/{division_id}/stats", response_model=DivisionDetailedStats)
async def get_division_detailed_stats(season_id: int, division_id: str):
    """Return detailed breakdown for a specific division including team stats, player leaderboards, and playoff bracket."""
    try:
        stats = await seasons_service.get_division_detailed_stats(season_id, division_id)
        return DivisionDetailedStats(**stats)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
