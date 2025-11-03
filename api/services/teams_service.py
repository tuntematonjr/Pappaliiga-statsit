from __future__ import annotations

from typing import Any, Dict, Optional

from async_db import compute_team_map_deltas_async, query_async

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
