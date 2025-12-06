from __future__ import annotations

from typing import Any, Collection, Dict, Optional

from async_db import compute_team_map_deltas_async, get_team_matches_mirror_async, query_async

from api.exceptions import NotFoundError

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


async def fetch_team(team_id: str) -> dict[str, Any]:
    rows = await query_async(
        """
        SELECT team_id, name AS team_name, name AS display_name, avatar
        FROM teams
        WHERE team_id = :team_id
        """,
        {"team_id": team_id},
    )
    if not rows:
        raise NotFoundError(f"Team '{team_id}' not found")
    team = rows[0]
    team.setdefault("avatar", DEFAULT_AVATAR)
    team["faceit_url"] = None
    return team


async def fetch_team_season_stats(team_id: str) -> list[dict[str, Any]]:
    rows = await query_async(
        """
        SELECT tst.season, tst.division_num, c.championship_id,
               tst.maps_played, tst.matches_played, tst.matches_won AS wins,
               (tst.matches_played - tst.matches_won) AS losses,
               CASE WHEN tst.matches_played > 0
                    THEN (tst.matches_won / tst.matches_played)
                    ELSE 0.0 END AS win_rate,
               tst.rounds_won, tst.rounds_lost, tst.maps_won
        FROM team_season_totals tst
        JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
        WHERE tst.team_id = :team_id
        ORDER BY tst.season DESC, tst.division_num
        """,
        {"team_id": team_id},
    )
    if not rows:
        raise NotFoundError(f"No stats found for team '{team_id}'")
    return rows


async def fetch_team_map_stats(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    map_deltas = await compute_team_map_deltas_async(championship_id, team_id)
    if not map_deltas:
        raise NotFoundError(
            f"No map stats found for team '{team_id}' in championship {championship_id}"
        )

    result: list[dict[str, Any]] = []
    for map_name, data in map_deltas.items():
        result.append(
            {
                "map_name": map_name,
                "curr": data["curr"],
                "prev": data["prev"],
                "delta": data.get("delta"),
                "snapshot_ts": data["prev"].get("snapshot_ts") if data.get("prev") else None,
            }
        )
    return result


async def list_teams(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    limit: int,
) -> list[dict[str, Any]]:
    if season is not None and division is not None:
        rows = await query_async(
            """
            SELECT DISTINCT t.team_id, t.name AS team_name, t.name AS display_name, t.avatar
            FROM teams t
            JOIN team_season_totals tst ON tst.team_id = t.team_id
            WHERE tst.season = :season AND tst.division_num = :division
            ORDER BY t.name, t.team_id
            LIMIT :limit
            """,
            {"season": season, "division": division, "limit": limit},
        )
    else:
        rows = await query_async(
            """
            SELECT team_id, name AS team_name, name AS display_name, avatar
            FROM teams
            ORDER BY name, team_id
            LIMIT :limit
            """,
            {"limit": limit},
        )
    for row in rows:
        row.setdefault("avatar", DEFAULT_AVATAR)
        row["faceit_url"] = None
    return rows


async def fetch_team_matches(team_id: str, championship_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch team's matches. If championship_id provided, filter to that championship."""
    # Verify team exists
    team_check = await query_async(
        "SELECT team_id FROM teams WHERE team_id = :team_id",
        {"team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found")
    
    # Get championship_id if not provided - use the one with actual matches
    if championship_id is None:
        # Get the latest championship with actual matches for this team
        champ_rows = await query_async(
            """
            SELECT DISTINCT c.championship_id
            FROM team_season_totals tst
            JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
            WHERE tst.team_id = :team_id
            AND EXISTS (
                SELECT 1 FROM matches m
                WHERE m.championship_id = c.championship_id
                AND (m.team1_id = :team_id OR m.team2_id = :team_id)
            )
            ORDER BY tst.season DESC
            LIMIT 1
            """,
            {"team_id": team_id}
        )
        if not champ_rows:
            raise NotFoundError(f"No matches found for team '{team_id}'")
        championship_id = champ_rows[0]["championship_id"]
    else:
        # Verify championship exists
        champ_rows = await query_async(
            "SELECT championship_id FROM championships WHERE championship_id = :champ_id",
            {"champ_id": championship_id}
        )
        if not champ_rows:
            raise NotFoundError(f"Championship {championship_id} not found")
    
    matches = await get_team_matches_mirror_async(championship_id, team_id)
    if not matches:
        raise NotFoundError(f"No matches found for team '{team_id}' in championship {championship_id}")
    
    # Transform to flat list format for API response
    result = []
    for match in matches:
        left = match.get("left", {})
        right = match.get("right", {})
        result.append({
            "match_id": match["match_id"],
            "ts": match["ts"],
            "status": match["status"],
            "best_of": match["best_of"],
            "played": match["played"],
            "team1_id": left.get("team_id"),
            "team2_id": right.get("team_id"),
            "team1_name": left.get("team_name"),
            "team2_name": right.get("team_name"),
            "t1_avatar": left.get("avatar"),
            "t2_avatar": right.get("avatar"),
            "faceit_url": match.get("faceit_url"),
            "maps": match.get("maps", [])
        })
    
    return result


async def fetch_team_players(team_id: str, championship_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch team's players. If championship_id provided, filter to that championship."""
    # Verify team exists
    team_check = await query_async(
        "SELECT team_id FROM teams WHERE team_id = :team_id",
        {"team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found")
    
    # Get championship for filtering - use one with actual player data
    if championship_id is None:
        # Get the latest championship with actual player data for this team
        champ_rows = await query_async(
            """
            SELECT DISTINCT c.championship_id
            FROM team_season_totals tst
            JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
            WHERE tst.team_id = :team_id
            AND EXISTS (
                SELECT 1 FROM player_stats ps
                JOIN matches m ON m.match_id = ps.match_id
                WHERE m.championship_id = c.championship_id AND ps.team_id = :team_id
            )
            ORDER BY tst.season DESC
            LIMIT 1
            """,
            {"team_id": team_id}
        )
        if not champ_rows:
            raise NotFoundError(f"No championship found for team '{team_id}'")
        championship_id = champ_rows[0]["championship_id"]
    
    # Query player stats for the team in this championship
    rows = await query_async(
        """
        SELECT
            pp.player_id,
            pp.nickname,
            COUNT(DISTINCT ps.match_id) AS matches_played,
            SUM(COALESCE(ps.kills, 0)) AS kills,
            SUM(COALESCE(ps.deaths, 0)) AS deaths,
            SUM(COALESCE(ps.damage, 0)) AS damage,
            AVG(NULLIF(ps.adr, 0)) AS adr,
            SUM(COALESCE(ps.mvps, 0)) AS headshots
        FROM player_stats ps
        JOIN players pp ON pp.player_id = ps.player_id
        JOIN matches m ON m.match_id = ps.match_id
        WHERE m.championship_id = :champ_id AND ps.team_id = :team_id
        GROUP BY pp.player_id, pp.nickname
        ORDER BY matches_played DESC, kills DESC
        """,
        {"champ_id": championship_id, "team_id": team_id}
    )
    
    if not rows:
        raise NotFoundError(f"No players found for team '{team_id}' in championship {championship_id}")
    
    return rows
