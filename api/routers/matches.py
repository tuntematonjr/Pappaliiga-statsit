"""Match API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from async_db import query_async

router = APIRouter()


class MatchSummary(BaseModel):
    """Match summary information."""
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


class MapResult(BaseModel):
    """Map-level result."""
    map_number: Optional[int]
    map_name: Optional[str]
    score_team1: int
    score_team2: int
    winner_team_id: Optional[str]
    is_forfeit_map: bool


class MatchDetails(BaseModel):
    """Full match details with map results."""
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


class PlayerMatchStats(BaseModel):
    """Player performance stats for a match."""
    player_id: str
    player_nickname: str
    team_id: str
    team_name: Optional[str]
    kills: int
    deaths: int
    assists: int
    kd: float
    adr: float
    hs_pct: float
    mvps: int


@router.get("/division/{championship_id}", response_model=List[MatchSummary])
async def get_division_matches(championship_id: str):
    """Get all matches for a division."""
    rows = await query_async(
        """
        SELECT
            m.match_id,
            m.championship_id,
            m.finished_at,
            m.team1_id,
            m.team2_id,
            m.is_forfeit,
            m.ignored_due_ban,
            t1.name AS team1_name,
            t2.name AS team2_name,
            COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team1_id THEN 1 ELSE 0 END), 0) AS team1_score,
            COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team2_id THEN 1 ELSE 0 END), 0) AS team2_score
        FROM matches m
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
        LEFT JOIN maps mp ON mp.match_id = m.match_id
        WHERE m.championship_id = :champ_id
        GROUP BY
            m.match_id,
            m.championship_id,
            m.finished_at,
            m.team1_id,
            m.team2_id,
            m.is_forfeit,
            m.ignored_due_ban,
            t1.name,
            t2.name
        ORDER BY COALESCE(m.finished_at, m.started_at, m.scheduled_at, m.last_seen_at, m.configured_at, 0) DESC,
                 m.match_id
        """,
        {"champ_id": championship_id}
    )
    
    if not rows:
        return []  # Empty list instead of 404
    
    return [
        {
            "match_id": r["match_id"],
            "championship_id": r["championship_id"],
            "finished_at": r.get("finished_at"),
            "team1_id": r.get("team1_id"),
            "team2_id": r.get("team2_id"),
            "team1_name": r.get("team1_name"),
            "team2_name": r.get("team2_name"),
            "team1_score": int(r.get("team1_score") or 0),
            "team2_score": int(r.get("team2_score") or 0),
            "is_forfeit": bool(r.get("is_forfeit", False)),
            "ignored_due_ban": bool(r.get("ignored_due_ban", False)),
        }
        for r in rows
    ]


@router.get("/{match_id}", response_model=MatchDetails)
async def get_match_details(match_id: str):
    """Get full match details including map results."""
    # Get match info
    match_rows = await query_async(
        """
        SELECT
               m.match_id,
               m.championship_id,
               m.finished_at,
               m.team1_id,
               m.team2_id,
               m.is_forfeit,
               m.ignored_due_ban,
               t1.name AS team1_name,
               t2.name AS team2_name,
               t1.avatar AS team1_avatar, t2.avatar AS team2_avatar
        FROM matches m
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
        WHERE m.match_id = :match_id
        """,
        {"match_id": match_id}
    )
    
    if not match_rows:
        raise HTTPException(status_code=404, detail=f"Match '{match_id}' not found")
    
    match = match_rows[0]
    
    # Get map results
    map_rows = await query_async(
        """
        SELECT round_index, map_name, score_team1, score_team2, winner_team_id, is_forfeit
        FROM maps
        WHERE match_id = :match_id
        ORDER BY round_index
        """,
        {"match_id": match_id}
    )
    
    team1_id = match.get("team1_id")
    team2_id = match.get("team2_id")
    team1_score = sum(1 for row in map_rows if team1_id and row.get("winner_team_id") == team1_id)
    team2_score = sum(1 for row in map_rows if team2_id and row.get("winner_team_id") == team2_id)
    
    return {
        "match_id": match["match_id"],
        "championship_id": match["championship_id"],
        "finished_at": match.get("finished_at"),
        "team1_id": team1_id,
        "team2_id": team2_id,
        "team1_name": match.get("team1_name"),
        "team2_name": match.get("team2_name"),
        "team1_avatar": match.get("team1_avatar"),
        "team2_avatar": match.get("team2_avatar"),
        "team1_score": int(team1_score),
        "team2_score": int(team2_score),
        "is_forfeit": bool(match.get("is_forfeit", False)),
        "ignored_due_ban": bool(match.get("ignored_due_ban", False)),
        "maps": [
            {
                "map_number": int(m["round_index"]) if m.get("round_index") is not None else None,
                "map_name": m["map_name"],
                "score_team1": int(m.get("score_team1") or 0),
                "score_team2": int(m.get("score_team2") or 0),
                "winner_team_id": m.get("winner_team_id"),
                "is_forfeit_map": bool(m.get("is_forfeit", False)),
            }
            for m in map_rows
        ],
    }


@router.get("/{match_id}/player-stats", response_model=List[PlayerMatchStats])
async def get_match_player_stats(match_id: str):
    """Get all player performance stats for a specific match.
    
    Args:
        match_id: Match identifier
    
    Returns:
        List of player stats aggregated across all maps in the match
    
    Raises:
        HTTPException: 404 if match not found
    """
    rows = await query_async(
        """
        SELECT 
            ps.player_id, p.nickname AS player_nickname,
            ps.team_id, t.name AS team_name,
            SUM(ps.kills) AS kills,
            SUM(ps.deaths) AS deaths,
            SUM(ps.assists) AS assists,
            AVG(ps.kd) AS kd,
            AVG(ps.adr) AS adr,
            AVG(ps.hs_pct) AS hs_pct,
            SUM(ps.mvps) AS mvps
        FROM player_stats ps
        JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN teams t ON t.team_id = ps.team_id
        WHERE ps.match_id = :match_id
        GROUP BY ps.player_id, ps.team_id
        ORDER BY kills DESC
        """,
        {"match_id": match_id}
    )
    
    if not rows:
        # Check if match exists
        match_check = await query_async(
            "SELECT match_id FROM matches WHERE match_id = :match_id",
            {"match_id": match_id}
        )
        if not match_check:
            raise HTTPException(status_code=404, detail=f"Match '{match_id}' not found")
        # Match exists but no player stats (maybe forfeit or in progress)
        return []
    
    return [
        {
            "player_id": r["player_id"],
            "player_nickname": r["player_nickname"],
            "team_id": r["team_id"],
            "team_name": r.get("team_name"),
            "kills": int(r["kills"] or 0),
            "deaths": int(r["deaths"] or 0),
            "assists": int(r["assists"] or 0),
            "kd": float(r["kd"] or 0.0),
            "adr": float(r["adr"] or 0.0),
            "hs_pct": float(r["hs_pct"] or 0.0),
            "mvps": int(r["mvps"] or 0),
        }
        for r in rows
    ]

