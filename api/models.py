from __future__ import annotations

from pydantic import BaseModel, ConfigDict


def to_camel(string: str) -> str:
    """Convert snake_case strings to camelCase."""
    if not string:
        return string
    parts = string.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class CamelModel(BaseModel):
    """Base model that serializes using camelCase aliases."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )


class PaginationMeta(CamelModel):
    total: int
    limit: int
    offset: int


# --- V3 models merged in from api/models_v3.py ---
from typing import List, Optional, Literal


class SeasonProgress(CamelModel):
    divisions_finished: int
    divisions_total: int
    regular_matches_played: int
    regular_matches_total: int
    playoff_matches_played: int
    playoff_matches_total: int
    overall_matches_played: int
    overall_matches_total: int


class SeasonSummary(CamelModel):
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
    progress: SeasonProgress | None = None


class DivisionSeasonStats(CamelModel):
    teams: int
    matches_played: int
    matches_total: int


class DivisionPlayoffsStats(CamelModel):
    status: Literal["waiting", "active", "finished"]
    teams: int = 8
    matches_played: int
    matches_total: int = 7
    winner_team: Optional[str] = None


class MvpPlayer(CamelModel):
    name: str
    rating: float


class DivisionMeta(CamelModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    winner_team: Optional[str] = None
    mvp_player: Optional[MvpPlayer] = None


class DivisionV3(CamelModel):
    division_id: str  # Championship ID (UUID)
    tier: int
    name: str
    status: Literal["waiting", "active", "finished"]
    season: DivisionSeasonStats
    playoffs: DivisionPlayoffsStats
    meta: Optional[DivisionMeta] = None


class SeasonTableRow(CamelModel):
    team: str
    matches: int
    wins: int
    losses: int
    rounds: int
    kills: int
    deaths: int
    adr: Optional[float] = None


class PlayerLeader(CamelModel):
    player: str
    value: float


class PlayerLeaders(CamelModel):
    frags: PlayerLeader
    kd: PlayerLeader
    adr: PlayerLeader
    mvps: PlayerLeader


class PlayoffBracketMatch(CamelModel):
    round: Literal[1, 2, 3]
    match_id: str
    team1: str
    team2: str
    score: Optional[str] = None
    winner: Optional[str] = None


class DivisionPlayoffsDetail(CamelModel):
    status: Literal["waiting", "active", "finished"]
    matches_played: int
    matches_total: int = 7
    bracket: List[PlayoffBracketMatch]
    winner_team: Optional[str] = None


class DivisionDetailV3(CamelModel):
    division_id: str
    season_table: List[SeasonTableRow]
    player_leaders: PlayerLeaders
    playoffs: DivisionPlayoffsDetail

