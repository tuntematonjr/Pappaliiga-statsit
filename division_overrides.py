"""Utilities for optional division-level overrides (banned teams, etc.)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

_DEFAULT_PATH = Path(__file__).with_name("division_overrides.json")

__all__ = [
    "load_division_overrides",
    "banned_teams_for_division",
    "quit_teams_for_division",
    "combined_status_teams",
]


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalise_status_entry(entry: Mapping[str, Any] | None, *, status: str) -> dict[str, str] | None:
    if not isinstance(entry, Mapping):
        return None

    team_id = _coerce_str(entry.get("team_id"))
    if not team_id:
        return None

    team_name = _coerce_str(entry.get("team_name")) or team_id
    reason = _coerce_str(entry.get("reason")) or ""
    status_at_key = "banned_at" if status == "banned" else "quit_at"
    status_at = _coerce_str(entry.get(status_at_key)) or ""
    note = _coerce_str(entry.get("note")) or ""
    avatar = _coerce_str(entry.get("avatar")) or ""

    return {
        "team_id": team_id,
        "team_name": team_name,
        "reason": reason,
        status_at_key: status_at,
        "note": note,
        "avatar": avatar,
        "status": status,
    }


def load_division_overrides(path: str | Path | None = None) -> dict[str, dict[str, List[dict[str, str]]]]:
    """Load optional overrides from JSON.

    The file is optional; a missing file returns an empty mapping. Only the
    ``banned_teams`` collection is recognised currently. Entries missing a
    ``team_id`` are ignored to keep the structure predictable.
    """

    target = Path(path) if path is not None else _DEFAULT_PATH
    try:
        raw_text = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as exc:  # pragma: no cover - extremely unlikely but useful context
        raise RuntimeError(f"Unable to read division overrides: {target}") from exc

    if not raw_text.strip():
        return {}

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in division overrides: {target}") from exc

    if not isinstance(payload, Mapping):
        return {}

    overrides: dict[str, dict[str, List[dict[str, str]]]] = {}
    for champ_id, section in payload.items():
        if not isinstance(section, Mapping):
            continue

        banned_entries: List[dict[str, str]] = []
        banned_source = section.get("banned_teams") if isinstance(section.get("banned_teams"), Sequence) else []
        for entry in banned_source:
            normalised = _normalise_status_entry(entry, status="banned")
            if normalised:
                banned_entries.append(normalised)

        quit_entries: List[dict[str, str]] = []
        quit_source = section.get("quit_teams") if isinstance(section.get("quit_teams"), Sequence) else []
        for entry in quit_source:
            normalised = _normalise_status_entry(entry, status="quit")
            if normalised:
                quit_entries.append(normalised)

        if banned_entries or quit_entries:
            champ_overrides: Dict[str, List[dict[str, str]]] = {}
            if banned_entries:
                champ_overrides["banned_teams"] = banned_entries
            if quit_entries:
                champ_overrides["quit_teams"] = quit_entries
            overrides[str(champ_id)] = champ_overrides

    return overrides


def banned_teams_for_division(
    championship_id: str,
    overrides: Mapping[str, dict[str, List[dict[str, str]]]] | None = None,
) -> list[dict[str, str]]:
    """Return banned team metadata for ``championship_id``.

    ``overrides`` can be supplied to avoid re-reading the JSON when used in a
    tight loop. The return value is a list of dictionaries with the keys from
    :func:`_normalise_banned_entry`.
    """

    source = overrides if overrides is not None else load_division_overrides()
    if not source:
        return []

    entry = source.get(championship_id) or source.get(str(championship_id))
    if not entry:
        return []

    banned = entry.get("banned_teams") or []
    return [dict(item) for item in banned]


def quit_teams_for_division(
    championship_id: str,
    overrides: Mapping[str, dict[str, List[dict[str, str]]]] | None = None,
) -> list[dict[str, str]]:
    source = overrides if overrides is not None else load_division_overrides()
    if not source:
        return []

    entry = source.get(championship_id) or source.get(str(championship_id))
    if not entry:
        return []

    quit_entries = entry.get("quit_teams") or []
    return [dict(item) for item in quit_entries]


def combined_status_teams(
    championship_id: str,
    overrides: Mapping[str, dict[str, List[dict[str, str]]]] | None = None,
) -> list[dict[str, str]]:
    banned = banned_teams_for_division(championship_id, overrides)
    quitters = quit_teams_for_division(championship_id, overrides)
    return banned + quitters
