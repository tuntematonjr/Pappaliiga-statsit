from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException

from db_async import compute_player_map_deltas_async, query_async

from api.exceptions import NotFoundError

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


async def fetch_player(player_id: str) -> dict[str, Any]:
    rows = await query_async(
        """
        SELECT player_id, nickname, country, avatar, faceit_url
        FROM players
        WHERE player_id = :player_id
        """,
        {"player_id": player_id},
    )
    if not rows:
        raise NotFoundError(f"Player '{player_id}' not found")
    player = rows[0]
    player.setdefault("avatar", DEFAULT_AVATAR)
    return player


async def fetch_player_season_stats(player_id: str) -> list[dict[str, Any]]:
    rows = await query_async(
        """
        SELECT pst.season, pst.division_num, c.championship_id,
               pst.team_id, t.name AS team_name,
               pst.maps_played, pst.rounds_played, pst.kills, pst.deaths, pst.assists,
               pst.kd, pst.adr, pst.rating, pst.hs_pct, pst.mvps
        FROM player_season_totals pst
        JOIN championships c ON c.season = pst.season AND c.division_num = pst.division_num
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE pst.player_id = :player_id
        ORDER BY pst.season DESC, pst.division_num
        """,
        {"player_id": player_id},
    )
    if not rows:
        raise NotFoundError(f"No stats found for player '{player_id}'")
    return rows


async def fetch_player_map_stats(championship_id: str, player_id: str) -> list[dict[str, Any]]:
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    map_deltas = await compute_player_map_deltas_async(championship_id, player_id)
    if not map_deltas:
        raise NotFoundError(
            f"No map stats found for player '{player_id}' in championship {championship_id}"
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


async def list_players(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    team_id: Optional[str] = None,
    limit: int,
) -> list[dict[str, Any]]:
    if season is not None and division is not None:
        query = """
            SELECT DISTINCT p.player_id, p.nickname, p.country, p.avatar, p.faceit_url
            FROM players p
            JOIN player_season_totals pst ON pst.player_id = p.player_id
            WHERE pst.season = :season AND pst.division_num = :division
        """
        params: Dict[str, Any] = {"season": season, "division": division, "limit": limit}
        if team_id:
            query += " AND pst.team_id = :team_id"
            params["team_id"] = team_id
        query += " ORDER BY p.nickname LIMIT :limit"
        rows = await query_async(query, params)
    else:
        rows = await query_async(
            """
            SELECT player_id, nickname, country, avatar, faceit_url
            FROM players
            ORDER BY nickname
            LIMIT :limit
            """,
            {"limit": limit},
        )

    for row in rows:
        row.setdefault("avatar", DEFAULT_AVATAR)
    return rows
