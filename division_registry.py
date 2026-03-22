"""Helpers for keeping ``divisions.json`` in sync with Faceit championships.

This module implements discovery, merge, and persistence logic so both
CLI tools and webhooks can refresh the division registry without
duplicating code. It updates the JSON file on disk and, when possible,
refreshes the in-memory copy exposed via :mod:`faceit_config`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import faceit_config
from faceit_config import DIVISIONS_JSON, PAPPALIIGA_ORG_ID
from faceit_client_async import (
    get_championship_matches_async,
    get_championship_teams_async,
    list_championships_for_organizer_async,
)

DIV_RX = re.compile(r"(divisioona|division|mestaruussarja)", re.IGNORECASE)
LEAD_NUM = re.compile(r"^\s*(\d{1,3})\s*[\.\-]?\s*")
SEASON_RX = re.compile(r"(?:S|Season)\s*([0-9]{1,2})", re.IGNORECASE)
POFF_RX = re.compile(r"playoff", re.IGNORECASE)
MESTAR_RX = re.compile(r"mestaruussarja", re.IGNORECASE)

CS_TAGS = {"cs2"}


def _parse_leading_divnum(name: str) -> int | None:
    match = LEAD_NUM.match(name or "")
    if match:
        return int(match.group(1))
    if MESTAR_RX.search(name or ""):
        return 0
    return None


def _parse_season(name: str) -> int:
    match = SEASON_RX.search(name or "")
    return int(match.group(1)) if match else 0


def _is_playoffs(name: str) -> bool:
    return bool(POFF_RX.search(name or ""))


def _is_cs_championship(championship: Dict[str, Any]) -> bool:
    game = (championship.get("game") or championship.get("game_id") or "").strip().lower()
    if game in CS_TAGS:
        return True
    return False


def _base_slug(division_num: int | None, season: int, is_po: bool) -> str:
    if division_num is not None and season:
        return f"div{division_num}-s{season}{'-po' if is_po else ''}"
    core = f"div{division_num}" if division_num is not None else "division"
    return f"{core}{'-po' if is_po else ''}"


def _make_unique_slug(proposed: str, championship_id: str, existing: set[str]) -> str:
    slug = proposed
    if slug in existing:
        short = championship_id.replace("-", "")[:6]
        slug = f"{slug}-{short}"
    return slug


async def discover_cs_divisions(
    organizer_id: str,
    *,
    min_season: int = 0,
    require_matches: bool = False,
    min_matches: int = 0,
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    championships = await list_championships_for_organizer_async(organizer_id)
    output: List[Dict[str, Any]] = []
    seen_cids: set[str] = set()
    stats: Dict[str, int] = {"api_count": 0, "skipped_status": 0, "skipped_matches": 0}

    for championship in championships:
        stats["api_count"] += 1
        cid = championship.get("championship_id") or championship.get("id")
        name = (championship.get("name") or "").strip()
        if not cid or not name:
            continue
        if cid in seen_cids:
            continue
        if not _is_cs_championship(championship):
            continue
        if not DIV_RX.search(name):
            continue

        division_num = _parse_leading_divnum(name)
        description = (championship.get("description") or "").strip()
        season = _parse_season(name) or _parse_season(description)
        is_po = _is_playoffs(name)

        if division_num is None and MESTAR_RX.search(name):
            division_num = 0

        if season < min_season:
            continue

        status = (championship.get("status") or championship.get("state") or "").strip().lower()
        if status in {"cancelled", "canceled", "closed", "inactive"}:
            stats["skipped_status"] += 1
            continue

        entry = {
            "championship_id": cid,
            "name": name,
            "season": season,
            "division_num": division_num if division_num is not None else 0,
            "slug": _base_slug(division_num if division_num is not None else 0, season, is_po),
            "is_playoffs": 1 if is_po else 0,
        }

        if require_matches and min_matches > 0:
            try:
                fetch_limit = max(min_matches, 100)
                matches = await get_championship_matches_async(cid, limit=fetch_limit)
                match_count = len(matches or [])
                if match_count < min_matches:
                    stats["skipped_matches"] += 1
                    continue
            except Exception:
                pass

        output.append(entry)
        seen_cids.add(cid)

    output.sort(key=lambda item: (-int(item.get("season", 0)), int(item.get("division_num", 0))))
    return output, stats


def load_existing(path: Path) -> List[Dict[str, Any]]:
    """Load existing divisions from disk with tolerant encoding handling.

    The file may be saved with various encodings (UTF-8 with BOM, UTF-16, etc.).
    Try a few decodings and fall back to a tolerant cleanup approach similar to
    the prior implementation. Return an empty list when the file cannot be
    decoded or parsed.
    """
    if not path.exists():
        return []

    try:
        raw_bytes = path.read_bytes()
    except Exception as exc:
        print(f"Warning: failed to read {path}: {exc}. Proceeding with empty DIVISIONS.")
        return []

    text: str | None = None
    # Try common encodings in a sensible order
    for enc in ("utf-8", "utf-8-sig", "utf-16", "latin-1"):
        try:
            text = raw_bytes.decode(enc)
            if enc != "utf-8":
                # Lightweight notice for unusual encodings; preserve prior print-based warnings
                print(f"Note: decoded {path} using encoding {enc}.")
            break
        except Exception:
            continue

    if text is None:
        print(f"Warning: could not decode {path} with known encodings. Proceeding with empty DIVISIONS.")
        return []

    try:
        data = json.loads(text)
    except Exception:
        # Fallback: perform the same tolerant cleanup previously used but operate on the decoded text
        raw_clean = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
        lines = [
            line
            for line in raw_clean.splitlines()
            if not line.strip().startswith("...") and "Lines" not in line
        ]
        raw_clean = "\n".join(lines)
        try:
            data = json.loads(raw_clean)
        except Exception as exc:
            print(
                "Warning: divisions.json exists but could not be parsed after tolerant cleanup:"
                f" {exc}. Proceeding with empty existing list."
            )
            return []

    if isinstance(data, list):
        return data
    print("Warning: divisions.json parsed but top-level is not a JSON array. Proceeding with empty list.")
    return []


def non_destructive_merge(existing: List[Dict[str, Any]], discovered: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_cid: Dict[str, Dict[str, Any]] = {}
    used_slugs: set[str] = set()
    for entry in existing:
        if isinstance(entry, dict):
            entry = dict(entry)
            entry.pop("division_id", None)
            entry.pop("game", None)
        slug = entry.get("slug")
        if isinstance(slug, str):
            used_slugs.add(slug)
        cid = entry.get("championship_id")
        if cid:
            by_cid[cid] = dict(entry)

    for discovered_entry in discovered:
        cid = discovered_entry["championship_id"]
        proposed_slug = discovered_entry["slug"]
        if cid in by_cid:
            current = by_cid[cid]
            if not current.get("slug"):
                current_slug = _make_unique_slug(proposed_slug, cid, used_slugs)
                current["slug"] = current_slug
                used_slugs.add(current_slug)
            else:
                used_slugs.add(current["slug"])

            for key, value in discovered_entry.items():
                if key not in current:
                    current[key] = value
                else:
                    existing_value = current[key]
                    if isinstance(value, str):
                        if existing_value is None or (isinstance(existing_value, str) and existing_value.strip() == ""):
                            current[key] = value
                    elif isinstance(value, int):
                        if existing_value is None or (isinstance(existing_value, int) and existing_value == 0 and value > 0):
                            current[key] = value
                    else:
                        if existing_value is None:
                            current[key] = value
            by_cid[cid] = current
        else:
            new_row = dict(discovered_entry)
            unique_slug = _make_unique_slug(proposed_slug, cid, used_slugs)
            new_row["slug"] = unique_slug
            used_slugs.add(unique_slug)
            by_cid[cid] = new_row

    merged = list(by_cid.values())
    merged.sort(
        key=lambda item: (
            -int(item.get("season", 0)),
            int(item.get("division_num", 0)),
        )
    )
    return merged


@dataclass(slots=True)
class DivisionRefreshResult:
    """Return metadata from :func:`refresh_divisions`."""

    output_path: Path
    total: int
    created: int
    changed: bool
    new_championship_ids: List[str]
    skipped_championship_ids: List[str]
    stats: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to plain dict for JSON responses."""
        return {
            "output_path": str(self.output_path),
            "total": self.total,
            "created": self.created,
            "changed": self.changed,
            "new_championship_ids": self.new_championship_ids,
            "skipped_championship_ids": self.skipped_championship_ids,
            "stats": dict(self.stats),
        }


async def _has_registered_teams(championship_id: str, min_teams: int = 1) -> bool:
    """
    Return ``True`` when a championship has at least ``min_teams`` registered teams.
    Falls back to ``True`` if the API call fails so we don't accidentally drop
    valid championships due to transient errors.
    """
    try:
        teams = await get_championship_teams_async(championship_id, limit=max(10, min_teams))
    except Exception:
        # On failure be permissive; caller can retry later.
        return True
    if teams is None:
        return True
    count = 0
    for team in teams or []:
        # Some responses include placeholders without team_id; ignore them.
        tid = team.get("team_id") or team.get("id")
        if tid:
            count += 1
        if count >= min_teams:
            return True
    return False


async def refresh_divisions(
    *,
    out_path: Path | None = None,
    min_season: int = faceit_config.DEFAULT_CURRENT_SEASON,
    require_matches: bool = False,
    min_matches: int = 0,
    min_new_division_teams: int = 0,
    dry_run: bool = False,
) -> DivisionRefreshResult:
    """Discover new championships and update ``divisions.json``.

    Args:
        out_path: Optional override for the output JSON file.
        min_season: Ignore championships with ``season`` lower than this.
        require_matches: When ``True`` also fetch match lists and filter out
            championships with fewer matches than ``min_matches``.
        min_matches: Minimum number of matches required when
            ``require_matches`` is enabled.
        min_new_division_teams: When greater than zero, new championships must
            have at least this many registered teams to be added.
        dry_run: When ``True`` do not write to disk; still return the merged
            payload so callers can inspect changes.

    Returns:
        :class:`DivisionRefreshResult` describing the outcome.
    """

    target = Path(out_path) if out_path is not None else DIVISIONS_JSON
    existing = load_existing(target)
    existing_ids = {
        str(entry.get("championship_id"))
        for entry in existing
        if entry.get("championship_id")
    }

    discovered, stats = await discover_cs_divisions(
        PAPPALIIGA_ORG_ID,
        min_season=min_season,
        require_matches=require_matches,
        min_matches=min_matches,
    )
    skipped_new: List[str] = []
    if min_new_division_teams > 0:
        candidate_new = [
            entry for entry in discovered
            if str(entry.get("championship_id")) not in existing_ids
        ]
        for entry in candidate_new:
            cid = entry.get("championship_id")
            if not cid:
                continue
            ok = await _has_registered_teams(str(cid), min_teams=min_new_division_teams)
            if ok:
                continue
            else:
                skipped_new.append(str(cid))
        if skipped_new:
            discovered = [
                entry for entry in discovered
                if str(entry.get("championship_id")) not in skipped_new
            ]

    merged = non_destructive_merge(existing, discovered)

    new_ids = [
        str(entry.get("championship_id"))
        for entry in merged
        if entry.get("championship_id") and str(entry.get("championship_id")) not in existing_ids
    ]

    changed = merged != existing

    if not dry_run:
        if changed:
            target.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        # Always refresh the in-memory cache so the current process sees updates.
        faceit_config.update_divisions(merged)

    return DivisionRefreshResult(
        output_path=target,
        total=len(merged),
        created=len(new_ids),
        changed=changed,
        new_championship_ids=new_ids,
        skipped_championship_ids=skipped_new,
        stats=stats,
    )


async def refresh_divisions_dry_run(
    *,
    out_path: Path | None = None,
    min_season: int = faceit_config.DEFAULT_CURRENT_SEASON,
    require_matches: bool = False,
    min_matches: int = 0,
    min_new_division_teams: int = 0,
) -> DivisionRefreshResult:
    """Convenience wrapper for :func:`refresh_divisions` with ``dry_run=True``."""

    return await refresh_divisions(
        out_path=out_path,
        min_season=min_season,
        require_matches=require_matches,
        min_matches=min_matches,
        min_new_division_teams=min_new_division_teams,
        dry_run=True,
    )
