"""CLI tool for managing mid-season team bans and quits.

Usage examples (run from repo root with venv active):

  # List all banned/quit teams for a season
  python scripts/manage_team_status.py list --season 4

  # Mark a team as banned (championship resolved automatically from team + season)
  python scripts/manage_team_status.py add --team-id <id> --season 4 --status banned --reason "Sääntörikkomus 1.16.5"

  # Mark a team as having quit
  python scripts/manage_team_status.py add --team-id <id> --season 4 --status quit

  # Remove a team's status
  python scripts/manage_team_status.py remove --team-id <id> --season 4

Effect of add:
  - Inserts/updates a row in championship_team_statuses
  - Sets ignored_due_ban=1 on all matches for that team in the championship
  - Recalculates team and player season totals (regular season only)

Effect of remove:
  - Deletes the row from championship_team_statuses
  - Clears ignored_due_ban=0 on those matches (re-flags matches where the
    other team is still excluded)
  - Recalculates team and player season totals (regular season only)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure repo root is on sys.path.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# faceit_config triggers env loading (.env in repo root).
import faceit_config  # noqa: F401 — side-effect import for env/DB config

from db_async import (
    close_pool,
    delete_championship_team_status,
    list_championship_team_statuses,
    query_async,
    upsert_championship_team_status,
)
from api.services.team_status_service import apply_team_status_backfill


async def _resolve_championship_id(team_id: str, season: int) -> str:
    """Find the regular-season championship for this team in the given season.

    Prefers non-playoff championships. Errors if none or ambiguous.
    """
    rows = await query_async(
        """
        SELECT DISTINCT c.championship_id, c.division_num, c.is_playoffs, c.name
        FROM championships c
        JOIN matches m ON m.championship_id = c.championship_id
        WHERE c.season = :season
          AND (m.team1_id = :tid OR m.team2_id = :tid)
        ORDER BY c.is_playoffs ASC, c.division_num ASC
        """,
        {"season": season, "tid": team_id},
    )
    if not rows:
        raise SystemExit(
            f"No matches found for team {team_id!r} in season {season}. "
            "Check the team ID and season number."
        )

    # Prefer regular season; fall back to any if only playoff exists.
    regular = [r for r in rows if not r.get("is_playoffs")]
    candidates = regular if regular else rows

    if len(candidates) > 1:
        print("Multiple championships found for this team in that season:")
        for r in candidates:
            print(f"  {r['championship_id']}  div {r['division_num']}  {r['name']}")
        raise SystemExit(
            "Ambiguous — use 'python scripts/manage_team_status.py add-by-id' "
            "with --championship-id instead."
        )

    cid = str(candidates[0]["championship_id"])
    print(f"Resolved championship: {candidates[0]['name']} ({cid})")
    return cid


async def _find_status_championship(team_id: str, season: int) -> str:
    """Find the championship that has an existing status row for this team + season."""
    rows = await query_async(
        """
        SELECT cts.championship_id, c.name
        FROM championship_team_statuses cts
        JOIN championships c ON c.championship_id = cts.championship_id
        WHERE c.season = :season
          AND cts.team_id = :tid
        """,
        {"season": season, "tid": team_id},
    )
    if rows:
        cid = str(rows[0]["championship_id"])
        print(f"Found existing status in: {rows[0]['name']} ({cid})")
        return cid
    # Fall back to resolving from matches.
    return await _resolve_championship_id(team_id, season)


def _prompt(label: str, default: str | None = None, *, required: bool = True) -> str:
    """Prompt the user for a value, optionally with a default."""
    suffix = f" [{default}]" if default else (" (optional, press Enter to skip)" if not required else "")
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if not value and default:
            return default
        if not value and not required:
            return ""
        if value:
            return value
        print("  Value is required.")


def _prompt_choice(label: str, choices: list[str]) -> str:
    """Prompt the user to pick from a fixed list."""
    choices_str = " / ".join(choices)
    while True:
        value = input(f"{label} ({choices_str}): ").strip().lower()
        if value in choices:
            return value
        print(f"  Please enter one of: {choices_str}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage mid-season team bans and quits in the database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- list ---
    p_list = sub.add_parser("list", help="List banned/quit teams for a season")
    p_list.add_argument("--season", default=None, type=int, metavar="N")

    # --- add ---
    p_add = sub.add_parser("add", help="Mark a team as banned or quit")
    p_add.add_argument("--team-id", default=None, metavar="ID")
    p_add.add_argument("--season", default=None, type=int, metavar="N")
    p_add.add_argument("--status", default=None, choices=["banned", "quit"])
    p_add.add_argument("--reason", default=None, metavar="TEXT")

    # --- remove ---
    p_remove = sub.add_parser("remove", help="Remove a team's banned/quit status")
    p_remove.add_argument("--team-id", default=None, metavar="ID")
    p_remove.add_argument("--season", default=None, type=int, metavar="N")

    return parser


async def cmd_list(args: argparse.Namespace) -> None:
    season = args.season
    if season is None:
        season = int(_prompt("Season"))

    champ_rows = await query_async(
        "SELECT championship_id FROM championships WHERE season = :season AND is_playoffs = 0 ORDER BY division_num",
        {"season": season},
    )
    if not champ_rows:
        print(f"No championships found for season {args.season}.")
        return

    any_found = False
    for champ in champ_rows:
        cid = str(champ["championship_id"])
        rows = await list_championship_team_statuses(cid)
        if not rows:
            continue
        any_found = True
        for r in rows:
            eff = ""
            if r.get("effective_at"):
                try:
                    eff = datetime.fromtimestamp(int(r["effective_at"]), tz=timezone.utc).strftime("%Y-%m-%d")
                except Exception:
                    eff = str(r["effective_at"])
            print(f"{r['team_id']:<40}  {r['status']:<8}  {eff:<12}  {r.get('team_name') or ''}")
            if r.get("reason"):
                print(f"  reason: {r['reason']}")

    if not any_found:
        print(f"No banned/quit teams found for season {season}.")


async def cmd_add(args: argparse.Namespace) -> None:
    team_id = args.team_id or _prompt("Team ID")
    season = args.season if args.season is not None else int(_prompt("Season"))
    status = args.status or _prompt_choice("Status", ["banned", "quit"])
    reason = args.reason if args.reason is not None else _prompt("Reason", required=False) or None

    championship_id = await _resolve_championship_id(team_id, season)

    await upsert_championship_team_status(
        championship_id,
        team_id,
        status=status,
        reason=reason,
    )
    print(f"Saved: {team_id} → {status}")

    print("Running retroactive backfill…")
    result = await apply_team_status_backfill(championship_id, team_id, flag=1)
    print(
        f"  Matches flagged (ignored_due_ban=1): {result['matches_updated']}\n"
        f"  Teams recalculated:                  {result['teams_recalculated']}\n"
        f"  Players recalculated:                {result['players_recalculated']}"
    )
    print("Done.")


async def cmd_remove(args: argparse.Namespace) -> None:
    team_id = args.team_id or _prompt("Team ID")
    season = args.season if args.season is not None else int(_prompt("Season"))

    championship_id = await _find_status_championship(team_id, season)

    deleted = await delete_championship_team_status(championship_id, team_id)
    if deleted == 0:
        print(f"No status row found for team {team_id} in season {season}. Nothing removed.")
        return
    print(f"Removed status for {team_id}")

    print("Running retroactive backfill…")
    result = await apply_team_status_backfill(championship_id, team_id, flag=0)
    print(
        f"  Matches un-flagged (ignored_due_ban=0): {result['matches_updated']}\n"
        f"  Teams recalculated:                     {result['teams_recalculated']}\n"
        f"  Players recalculated:                   {result['players_recalculated']}"
    )
    print("Done.")


async def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "list":
            await cmd_list(args)
        elif args.command == "add":
            await cmd_add(args)
        elif args.command == "remove":
            await cmd_remove(args)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
