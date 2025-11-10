"""Pydantic models for API v3."""
from __future__ import annotations
from typing import List, Optional, Literal
from pydantic import BaseModel, Field


class SeasonSummary(BaseModel):
    season_id: int
    divisions_total: int
    divisions_finished: int
    teams: int
    players: int
    matches: int
    rounds: int
    kills: int
    deaths: int
    adr_avg: Optional[float] = None
    kd_avg: Optional[float] = None
    win_rate: Optional[float] = None


class DivisionSeasonStats(BaseModel):
    teams: int
    matches_played: int
    matches_total: int


class DivisionPlayoffsStats(BaseModel):
    status: Literal["waiting", "active", "finished"]
    teams: int = 8
    matches_played: int
    matches_total: int = 7
    winner_team: Optional[str] = None


class MvpPlayer(BaseModel):
    name: str
    rating: float


class DivisionMeta(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    winner_team: Optional[str] = None
    mvp_player: Optional[MvpPlayer] = None


class DivisionV3(BaseModel):
    division_id: str  # Championship ID (UUID)
    tier: int
    name: str
    status: Literal["waiting", "active", "finished"]
    season: DivisionSeasonStats
    playoffs: DivisionPlayoffsStats
    meta: Optional[DivisionMeta] = None


class SeasonTableRow(BaseModel):
    team: str
    matches: int
    wins: int
    losses: int
    rounds: int
    kills: int
    deaths: int
    adr: Optional[float] = None
    rating: Optional[float] = None


class PlayerLeader(BaseModel):
    player: str
    value: float


class PlayerLeaders(BaseModel):
    frags: PlayerLeader
    kd: PlayerLeader
    adr: PlayerLeader
    mvps: PlayerLeader


class PlayoffBracketMatch(BaseModel):
    round: Literal[1, 2, 3]
    match_id: str
    team1: str
    team2: str
    score: Optional[str] = None
    winner: Optional[str] = None


class DivisionPlayoffsDetail(BaseModel):
    status: Literal["waiting", "active", "finished"]
    matches_played: int
    matches_total: int = 7
    bracket: List[PlayoffBracketMatch]
    winner_team: Optional[str] = None


class DivisionDetailV3(BaseModel):
    division_id: int
    season_table: List[SeasonTableRow]
    player_leaders: PlayerLeaders
    playoffs: DivisionPlayoffsDetail

