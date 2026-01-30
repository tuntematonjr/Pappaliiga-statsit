#!/usr/bin/env python3
"""Recompute season totals for a championship without running a full Faceit sync.

This helper does two things:
 1. Applies the current `division_overrides.json` banned/quit lists to the
    `matches.ignored_due_ban` column for the given championship.
 2. Recomputes `player_season_totals` and `team_season_totals` for affected
    players and teams by calling the existing upsert functions in `db_async.py`.

Usage:
  python tools/recompute_totals.py --championship-id <CHAMP_ID>

This is a fast, DB-only path and does not fetch data from Faceit. It is safe
for production if you only need to apply policy changes (bans/quit) to
already-imported matches.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Set

from db_async import (
    connection,
    upsert_player_season_totals_async,
    upsert_team_season_totals_async,
)
import faceit_config

OVERRIDES_PATH = (Path(__file__).resolve().parent.parent / "division_overrides.json")


def load_overrides(path: Path = OVERRIDES_PATH) -> dict:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _find_division(championship_id: str):
    for d in faceit_config.DIVISIONS:
        if d.get("championship_id") == championship_id:
            return d
    return None


async def _apply_ignored_flags(conn, championship_id: str, banned_team_ids: Set[str]) -> None:
    """Set matches.ignored_due_ban = 1 for matches where either team is banned,
    and 0 otherwise, for the given championship.
    """
    if banned_team_ids:
        # Prepare placeholders for SQL IN
        placeholders = ",".join(["%s"] * len(banned_team_ids))
        # Build CASE expression: 1 if team1 or team2 in banned set else 0
        sql = (
            "UPDATE matches SET ignored_due_ban = CASE "
            f"WHEN team1_id IN ({placeholders}) OR team2_id IN ({placeholders}) THEN 1 ELSE 0 END "
            "WHERE championship_id = %s"
        )
        args = tuple(list(banned_team_ids) + list(banned_team_ids) + [championship_id])
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
    else:
        # No banned teams: clear flags for this championship
        async with conn.cursor() as cur:
            await cur.execute("UPDATE matches SET ignored_due_ban = 0 WHERE championship_id = %s", (championship_id,))


async def _collect_affected_ids(conn, championship_id: str) -> tuple[Set[str], Set[str], int, int]:
    # Players affected by matches in this championship
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT DISTINCT ps.player_id FROM player_stats ps JOIN matches m ON ps.match_id = m.match_id WHERE m.championship_id = %s",
            (championship_id,),
        )
        rows = await cur.fetchall()
        player_ids = {r[0] for r in rows if r and r[0]}

        # Teams affected via team_stats OR matches team1/team2
        await cur.execute(
            "SELECT DISTINCT ts.team_id FROM team_stats ts JOIN matches m ON ts.match_id = m.match_id WHERE m.championship_id = %s",
            (championship_id,),
        )
        rows = await cur.fetchall()
        team_ids = {r[0] for r in rows if r and r[0]}

        # Also include teams referenced directly on matches (covers cases with no team_stats rows)
        await cur.execute(
            "SELECT DISTINCT team1_id, team2_id FROM matches WHERE championship_id = %s",
            (championship_id,),
        )
        rows = await cur.fetchall()
        for r in rows:
            t1, t2 = r[0], r[1]
            if t1:
                team_ids.add(t1)
            if t2:
                team_ids.add(t2)

    return player_ids, team_ids, len(player_ids), len(team_ids)


async def recompute_for_championship(championship_id: str) -> None:
    division = _find_division(championship_id)
    if not division:
        raise SystemExit(f"Championship {championship_id} not found in faceit_config.DIVISIONS")
    season = division["season"]
    division_num = division["division_num"]

    overrides = load_overrides()
    section = overrides.get(championship_id) or overrides.get(str(championship_id)) or {}
    banned_entries = section.get("banned_teams") or []
    banned_team_ids = {entry.get("team_id") for entry in banned_entries if entry.get("team_id")}

    async with connection() as conn:
        # 1) Apply ignored_due_ban flags in matches table based on overrides
        await _apply_ignored_flags(conn, championship_id, banned_team_ids)

        # 2) Collect affected players and teams and run upserts
        player_ids, team_ids, pcnt, tcnt = await _collect_affected_ids(conn, championship_id)
        print(f"Found {pcnt} players and {tcnt} teams to update for championship {championship_id}")

        # Batch upserts (reuse existing upsert functions one-by-one)
        updated_players = 0
        updated_teams = 0
        for tid in sorted(team_ids):
            try:
                await upsert_team_season_totals_async(
                    season,
                    division_num,
                    tid,
                )
                updated_teams += 1
            except Exception as exc:
                print(f"Failed to update team totals {tid}: {exc}")

        for pid in sorted(player_ids):
            try:
                await upsert_player_season_totals_async(
                    season,
                    division_num,
                    pid,
                )
                updated_players += 1
            except Exception as exc:
                print(f"Failed to update player totals {pid}: {exc}")

        print(f"Recomputed totals: {updated_players} players, {updated_teams} teams")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Recompute season totals for a championship using DB-only path")
    p.add_argument("--championship-id", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(recompute_for_championship(args.championship_id))
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
