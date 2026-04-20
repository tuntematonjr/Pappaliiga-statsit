"""Team status service — reads championship_team_statuses and division_overrides."""
from __future__ import annotations

from typing import Any, Dict, List, Set

from db_async import query_async
from division_overrides import combined_status_teams

_EXCLUDED_STATUSES = {"banned", "quit"}


async def list_team_statuses(championship_id: str) -> List[Dict[str, Any]]:
    """Return all status rows for a championship from the DB, merged with overrides."""
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

    # Also incorporate file-based overrides so sync_pipeline retains compatibility
    # with division_overrides.json.
    from division_overrides import load_division_overrides

    overrides = load_division_overrides()
    override_entries = combined_status_teams(championship_id, overrides)

    # Merge: DB rows take precedence; overrides fill gaps.
    db_team_ids: Set[str] = {str(r["team_id"]) for r in rows}
    merged = list(rows)
    for entry in override_entries:
        tid = entry.get("team_id")
        if tid and str(tid) not in db_team_ids:
            merged.append(
                {
                    "championship_id": championship_id,
                    "team_id": tid,
                    "status": entry.get("status") or "banned",
                    "effective_at": None,
                    "reason": entry.get("reason"),
                    "note": entry.get("note"),
                    "team_name": entry.get("team_name"),
                    "avatar": entry.get("avatar") or "",
                }
            )
    return merged


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
