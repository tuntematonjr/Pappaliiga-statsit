#!/usr/bin/env python3
"""Interactive CLI tool to manage banned and quit teams for Pappaliiga divisions."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from db_async import fetch_all
import faceit_config

OVERRIDES_PATH = Path(__file__).with_name("division_overrides.json")
STATUS_KEYS = {"banned": "banned_teams", "quit": "quit_teams"}


def _load_overrides_raw(path: Path = OVERRIDES_PATH) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - surfaced to caller
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


def _save_overrides(data: Dict[str, Dict[str, List[Dict[str, Any]]]], path: Path = OVERRIDES_PATH) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


async def _fetch_division_teams(championship_id: str) -> List[Dict[str, Any]]:
    sql = """
        SELECT DISTINCT t.team_id, COALESCE(NULLIF(t.name, ''), t.team_id) AS team_name, COALESCE(t.avatar, '') AS avatar
        FROM matches m
        JOIN teams t ON t.team_id = m.team1_id
        WHERE m.championship_id = %s
        UNION
        SELECT DISTINCT t.team_id, COALESCE(NULLIF(t.name, ''), t.team_id) AS team_name, COALESCE(t.avatar, '') AS avatar
        FROM matches m
        JOIN teams t ON t.team_id = m.team2_id
        WHERE m.championship_id = %s
        ORDER BY team_name
    """
    rows = await fetch_all(sql, (championship_id, championship_id))
    return [dict(row) for row in rows]


def _print_division_summary(divisions: Sequence[dict[str, Any]], show_season: bool = True) -> None:
    print("Available divisions:")
    for idx, division in enumerate(divisions, start=1):
        status = "(playoffs)" if division.get("is_playoffs") else ""
        season_txt = f"S{division.get('season')}" if show_season else ""
        print(f"  {idx:>2}. {division.get('name')} {season_txt} {status}".strip())


def _prompt_choice(count: int, prompt: str) -> int:
    while True:
        value = input(f"{prompt} [1-{count}] (or 'q' to cancel): ").strip().lower()
        if value in {"q", "quit", "exit"}:
            raise KeyboardInterrupt
        if value.isdigit():
            idx = int(value)
            if 1 <= idx <= count:
                return idx - 1
        print("Invalid selection. Please try again.")


def _prompt_status() -> str:
    while True:
        value = input("Status type? [b]anned / [q]uit: ").strip().lower()
        if value in {"b", "banned"}:
            return "banned"
        if value in {"q", "quit"}:
            return "quit"
        print("Please enter 'b' for banned or 'q' for quit.")


def _prompt_reason() -> str:
    return input("Reason (optional, press Enter to skip): ").strip()


def _current_stamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _ensure_champ_entry(data: Dict[str, Dict[str, List[Dict[str, Any]]]], championship_id: str) -> Dict[str, List[Dict[str, Any]]]:
    return data.setdefault(championship_id, {})


def _update_status_entry(
    data: Dict[str, Dict[str, List[Dict[str, Any]]]],
    championship_id: str,
    *,
    team_id: str,
    team_name: str,
    avatar: str,
    status: str,
    reason: str,
) -> Dict[str, Any]:
    container = _ensure_champ_entry(data, championship_id)
    list_key = STATUS_KEYS[status]
    entries = container.setdefault(list_key, [])
    timestamp_field = "banned_at" if status == "banned" else "quit_at"

    for existing in entries:
        if existing.get("team_id") == team_id:
            existing.update(
                {
                    "team_name": team_name,
                    "reason": reason,
                    "avatar": avatar,
                    timestamp_field: existing.get(timestamp_field) or _current_stamp(),
                }
            )
            existing["status"] = status
            return existing

    entry = {
        "team_id": team_id,
        "team_name": team_name,
        "reason": reason,
        timestamp_field: _current_stamp(),
        "note": "",
        "avatar": avatar,
        "status": status,
    }
    entries.append(entry)
    entries.sort(key=lambda item: item.get("team_name", item.get("team_id", "")))
    return entry


def _remove_status_entry(
    data: Dict[str, Dict[str, List[Dict[str, Any]]]],
    championship_id: str,
    *,
    team_id: str,
    status: Optional[str],
) -> bool:
    section = data.get(championship_id)
    if not section:
        return False

    statuses = (STATUS_KEYS.keys() if status is None else [status])
    removed = False
    for status_name in statuses:
        list_key = STATUS_KEYS[status_name]
        entries = section.get(list_key)
        if not entries:
            continue
        original_len = len(entries)
        entries[:] = [entry for entry in entries if entry.get("team_id") != team_id]
        removed = removed or (len(entries) != original_len)
        if not entries:
            section.pop(list_key, None)
    if not section:
        data.pop(championship_id, None)
    return removed


def _render_status_table(data: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> None:
    if not data:
        print("No banned or quit teams recorded.")
        return
    for champ_id, section in sorted(data.items()):
        division = next((d for d in faceit_config.DIVISIONS if d.get("championship_id") == champ_id), None)
        title = division["name"] if division else champ_id
        print(f"\n{title} ({champ_id})")
        for status, key in STATUS_KEYS.items():
            entries = section.get(key) or []
            if not entries:
                continue
            print(f"  {status.title()} teams:")
            for entry in entries:
                reason = entry.get("reason") or "(no reason provided)"
                timestamp = entry.get("banned_at") or entry.get("quit_at") or ""
                print(f"    - {entry.get('team_name')} [{entry.get('team_id')}] :: {reason} {timestamp}")


async def _interactive_flow() -> None:
    all_divisions = [div for div in faceit_config.DIVISIONS if div.get("championship_id")]
    if not all_divisions:
        raise RuntimeError("No divisions configured in divisions.json")

    # Show only current season by default
    current_season = faceit_config.CURRENT_SEASON
    current_divisions = [d for d in all_divisions if d.get("season") == current_season]

    if current_divisions:
        show_all = input(f"Show only Season {current_season} divisions? [Y/n]: ").strip().lower()
        divisions = all_divisions if show_all in {"n", "no"} else current_divisions
    else:
        divisions = all_divisions
    
    _print_division_summary(divisions)
    try:
        division_idx = _prompt_choice(len(divisions), "Select division")
    except KeyboardInterrupt:
        print("\nCancelled.")
        return

    division = divisions[division_idx]
    champ_id = division["championship_id"]
    teams = await _fetch_division_teams(champ_id)
    if not teams:
        print("No teams found for this division. Make sure the database is synced.")
        return

    print(f"\nTeams in {division.get('name')}:")
    for idx, team in enumerate(teams, start=1):
        print(f"  {idx:>2}. {team.get('team_name')} [{team.get('team_id')}]")

    try:
        team_idx = _prompt_choice(len(teams), "Select team")
    except KeyboardInterrupt:
        print("\nCancelled.")
        return

    team = teams[team_idx]
    status = _prompt_status()
    reason = _prompt_reason()

    confirmation = input(
        f"Add {team.get('team_name')} as {status}? [y/N]: "
    ).strip().lower()
    if confirmation not in {"y", "yes"}:
        print("Aborted.")
        return

    overrides = _load_overrides_raw()
    entry = _update_status_entry(
        overrides,
        champ_id,
        team_id=team.get("team_id"),
        team_name=team.get("team_name"),
        avatar=team.get("avatar") or "",
        status=status,
        reason=reason,
    )
    _save_overrides(overrides)
    stamp = entry.get("banned_at") or entry.get("quit_at") or ""
    print(f"[OK] {team.get('team_name')} tagged as {status}. (Recorded {stamp})")


async def _add_via_args(args: argparse.Namespace) -> None:
    overrides = _load_overrides_raw()
    entry = _update_status_entry(
        overrides,
        args.championship_id,
        team_id=args.team_id,
        team_name=args.team_name,
        avatar=args.avatar or "",
        status=args.status,
        reason=args.reason or "",
    )
    _save_overrides(overrides)
    stamp = entry.get("banned_at") or entry.get("quit_at") or ""
    print(
        f"Added/updated {entry.get('team_name')} ({entry.get('team_id')}) as {args.status}."
        f" Recorded {stamp}."
    )


def _remove_via_args(args: argparse.Namespace) -> None:
    overrides = _load_overrides_raw()
    removed = _remove_status_entry(
        overrides,
        args.championship_id,
        team_id=args.team_id,
        status=args.status,
    )
    if not removed:
        print("No matching entry found.")
        return
    _save_overrides(overrides)
    target_status = args.status or "any"
    print(f"Removed {args.team_id} ({target_status}) from {args.championship_id} overrides.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage banned/quit team overrides")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List current banned/quit overrides")

    add = sub.add_parser("add", help="Add or update a team override via arguments")
    add.add_argument("--championship-id", required=True)
    add.add_argument("--team-id", required=True)
    add.add_argument("--team-name", required=True)
    add.add_argument("--status", required=True, choices=sorted(STATUS_KEYS.keys()))
    add.add_argument("--reason")
    add.add_argument("--avatar", help="Optional avatar URL override", default="")

    remove = sub.add_parser("remove", help="Remove an override")
    remove.add_argument("--championship-id", required=True)
    remove.add_argument("--team-id", required=True)
    remove.add_argument(
        "--status",
        choices=sorted(STATUS_KEYS.keys()),
        help="Restrict removal to a specific status (default removes from both collections)",
    )

    return parser


async def _main_async() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "list":
        _render_status_table(_load_overrides_raw())
        return
    if args.command == "add":
        await _add_via_args(args)
        return
    if args.command == "remove":
        _remove_via_args(args)
        return

    await _interactive_flow()


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        print("\nCancelled by user.")


if __name__ == "__main__":
    main()
