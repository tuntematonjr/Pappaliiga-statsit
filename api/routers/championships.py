"""Championship-specific API endpoints."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.models import CamelModel
from api.services import divisions_service

router = APIRouter()


class ChampionshipTeam(CamelModel):
    """Normalized team stats for championship overviews."""

    team_id: str
    name: str
    display_name: str | None = None
    logo: str | None = None
    avatar: str | None = None
    matches_played: int = 0
    matches_won: int = 0
    matches_lost: int = 0
    maps_played: int = 0
    maps_won: int = 0
    maps_lost: int = 0
    rounds_diff: int = 0
    win_rate: float = 0.0
    kd: float = 0.0
    adr: float = 0.0


@router.get("/{championship_id}/teams", response_model=List[ChampionshipTeam])
async def list_championship_teams(championship_id: str):
    """Return normalized team data for a specific championship."""
    try:
        champ_row = await divisions_service.fetch_division_by_id(championship_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    division = await divisions_service.get_division_details(champ_row)
    teams = division.get("teams") or []

    normalized: list[dict] = []
    for team in teams:
        matches_played = int(team.get("matches_played") or 0)
        matches_won = int(team.get("matches_won") or team.get("wins") or 0)
        matches_lost = int(
            team.get("matches_lost")
            or team.get("losses")
            or max(matches_played - matches_won, 0)
        )
        maps_played = int(team.get("maps_played") or team.get("maps") or 0)
        maps_won = int(team.get("maps_won") or team.get("wins") or 0)
        maps_lost = int(
            team.get("maps_lost") or max(maps_played - maps_won, 0)
        )
        rounds_won = int(team.get("rounds_won") or 0)
        rounds_lost = int(team.get("rounds_lost") or 0)
        rounds_diff = int(
            team.get("rounds_diff") or (rounds_won - rounds_lost)
        )
        win_rate = float(team.get("win_rate") or 0.0)
        kd = float(team.get("kd") or 0.0)
        adr = float(team.get("adr") or 0.0)
        logo = team.get("avatar") or team.get("logo")

        normalized.append(
            {
                "team_id": team.get("team_id"),
                "name": team.get("display_name") or team.get("team_name"),
                "display_name": team.get("display_name") or team.get("team_name"),
                "logo": logo,
                "avatar": logo,
                "matches_played": matches_played,
                "matches_won": matches_won,
                "matches_lost": matches_lost,
                "maps_played": maps_played,
                "maps_won": maps_won,
                "maps_lost": maps_lost,
                "rounds_diff": rounds_diff,
                "win_rate": win_rate,
                "kd": kd,
                "adr": adr,
            }
        )

    return normalized
