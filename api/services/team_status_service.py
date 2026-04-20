"""Team status service — reads championship_team_statuses from the DB."""
from __future__ import annotations

from typing import Any, Dict, List, Set

from db_async import (
    connection,
    query_async,
    upsert_player_map_season_totals_bulk_async,
    upsert_player_season_totals_bulk_async,
    upsert_team_map_season_totals_bulk_async,
    upsert_team_season_totals_bulk_async,
)
from api.services.cache_helpers import GLOBAL_CACHE, get_championship_revision

_EXCLUDED_STATUSES = {"banned", "quit"}


async def list_team_statuses(championship_id: str) -> List[Dict[str, Any]]:
    """Return all status rows for a championship from the DB."""
    revision = await get_championship_revision(championship_id)
    cache_key = ("list_team_statuses", championship_id, revision)

    async def _compute() -> List[Dict[str, Any]]:
        rows = await query_async(
            """
            SELECT
                cts.championship_id,
                cts.team_id,
                cts.status,
                cts.effective_at,
                cts.reason,
                cts.note,
                COALESCE(tc.team_name, t.name) AS team_name,
                COALESCE(t.avatar, '') AS avatar
            FROM championship_team_statuses cts
            LEFT JOIN teams t ON t.team_id = cts.team_id
            LEFT JOIN team_championships tc
                ON tc.team_id = cts.team_id
                AND tc.championship_id = cts.championship_id
            WHERE cts.championship_id = :champ_id
            """,
            {"champ_id": championship_id},
        )
        return list(rows)

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def get_excluded_team_ids(championship_id: str) -> Set[str]:
    """Return the set of team IDs that should be excluded from stats for a championship."""
    statuses = await list_team_statuses(championship_id)
    return {
        str(row["team_id"])
        for row in statuses
        if str(row.get("status") or "").lower() in _EXCLUDED_STATUSES
    }


async def get_excluded_team_lookup(championship_id: str) -> Dict[str, Dict[str, Any]]:
    """Return a dict of team_id → status row for excluded teams."""
    statuses = await list_team_statuses(championship_id)
    return {
        str(row["team_id"]): row
        for row in statuses
        if str(row.get("status") or "").lower() in _EXCLUDED_STATUSES
    }


async def apply_team_status_backfill(
    championship_id: str,
    team_id: str,
    *,
    flag: int,
) -> Dict[str, int]:
    """Update ignored_due_ban on matches and recalculate season totals.

    ``flag=1`` when adding a ban/quit; ``flag=0`` when removing one.
    Returns a dict with counts of affected rows/teams/players.
    """
    # Fetch championship metadata.
    champ_rows = await query_async(
        "SELECT season, division_num, is_playoffs FROM championships WHERE championship_id = :cid",
        {"cid": championship_id},
    )
    if not champ_rows:
        raise ValueError(f"Championship {championship_id} not found")
    champ = champ_rows[0]
    season: int = int(champ["season"])
    division_num: int = int(champ["division_num"])
    is_playoffs: bool = bool(champ.get("is_playoffs"))

    matches_updated = 0
    async with connection(label="team-status-backfill") as conn:
        async with conn.cursor() as cur:
            if flag == 1:
                # Flag all matches in this championship involving this team.
                await cur.execute(
                    """
                    UPDATE matches
                    SET ignored_due_ban = 1
                    WHERE championship_id = %s
                      AND (team1_id = %s OR team2_id = %s)
                    """,
                    (championship_id, team_id, team_id),
                )
                matches_updated = cur.rowcount
            else:
                # Clear flag for this team's matches, then re-apply for any
                # matches where the *other* team is still excluded.
                await cur.execute(
                    """
                    UPDATE matches
                    SET ignored_due_ban = 0
                    WHERE championship_id = %s
                      AND (team1_id = %s OR team2_id = %s)
                    """,
                    (championship_id, team_id, team_id),
                )
                matches_updated = cur.rowcount

                # Re-flag matches where the opponent is still excluded.
                still_excluded = await query_async(
                    """
                    SELECT team_id FROM championship_team_statuses
                    WHERE championship_id = :cid
                      AND LOWER(status) IN ('banned', 'quit')
                      AND team_id != :tid
                    """,
                    {"cid": championship_id, "tid": team_id},
                )
                still_ids = [str(r["team_id"]) for r in still_excluded]
                for other_id in still_ids:
                    async with conn.cursor() as cur2:
                        await cur2.execute(
                            """
                            UPDATE matches
                            SET ignored_due_ban = 1
                            WHERE championship_id = %s
                              AND (team1_id = %s OR team2_id = %s)
                            """,
                            (championship_id, other_id, other_id),
                        )

    teams_recalculated = 0
    players_recalculated = 0

    # Totals tables are only reliable for regular-season championships.
    if not is_playoffs:
        team_id_rows = await query_async(
            """
            SELECT DISTINCT team_id FROM (
                SELECT team1_id AS team_id FROM matches WHERE championship_id = :cid AND team1_id IS NOT NULL
                UNION
                SELECT team2_id AS team_id FROM matches WHERE championship_id = :cid AND team2_id IS NOT NULL
            ) t
            """,
            {"cid": championship_id},
        )
        all_team_ids = [str(r["team_id"]) for r in team_id_rows]

        player_id_rows = await query_async(
            """
            SELECT DISTINCT ps.player_id
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            WHERE m.championship_id = :cid
            """,
            {"cid": championship_id},
        )
        all_player_ids = [str(r["player_id"]) for r in player_id_rows]

        if all_team_ids:
            await upsert_team_season_totals_bulk_async(
                season, division_num, all_team_ids,
                label=f"backfill:{championship_id}:team-season",
            )
            await upsert_team_map_season_totals_bulk_async(
                season, division_num, all_team_ids,
                label=f"backfill:{championship_id}:team-map",
            )
            teams_recalculated = len(all_team_ids)

        if all_player_ids:
            await upsert_player_season_totals_bulk_async(
                season, division_num, all_player_ids,
                label=f"backfill:{championship_id}:player-season",
            )
            await upsert_player_map_season_totals_bulk_async(
                season, division_num, all_player_ids,
                label=f"backfill:{championship_id}:player-map",
            )
            players_recalculated = len(all_player_ids)

    return {
        "matches_updated": matches_updated,
        "teams_recalculated": teams_recalculated,
        "players_recalculated": players_recalculated,
    }
