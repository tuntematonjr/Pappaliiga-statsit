"""Helpers for keeping the division registry in sync with Faceit championships.

This module implements discovery, merge, and persistence logic so both
CLI tools and webhooks can refresh the division registry without
duplicating code. Divisions are persisted in the DB ``championships`` table
and the in-memory copy is exposed via :mod:`faceit_config`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

import db_async
import faceit_config
from faceit_config import PAPPALIIGA_ORG_ID
from faceit_client_async import (
    get_championship_matches_async,
    list_championships_for_organizer_async,
)

DIV_RX = re.compile(r"(divisioona|division|mestaruussarja)", re.IGNORECASE)
LEAD_NUM = re.compile(r"^\s*(\d{1,3})\s*[\.\-]?\s*")
SEASON_RX = re.compile(r"(?:S|Season)\s*([0-9]{1,2})", re.IGNORECASE)
POFF_RX = re.compile(r"playoff", re.IGNORECASE)
MESTAR_RX = re.compile(r"mestaruussarja", re.IGNORECASE)

CS_TAGS = {"cs2"}


# ---------------------------------------------------------------------------
# Season-from-date inference
# ---------------------------------------------------------------------------
# Two seasons per year:
#   Even seasons (8, 10, 12, …) start in February  (H1 – after year start)
#   Odd  seasons (7,  9, 11, …) start in August    (H2 – after summer)
#
# Observed anchors:
#   Season  7 → 2023-08-27   Season  8 → 2024-02-11
#   Season  9 → 2024-08-11   Season 10 → 2025-02-09
#   Season 11 → 2025-08-10   Season 12 → 2026-02-08
#
# Derivation:
#   If month in [8..12] or month == 1 (still H2 of the same calendar half-year):
#       Use August half-year → odd season
#   If month in [2..7]:
#       Use February half-year → even season


def _date_to_season(year: int, month: int) -> int:
    """Return the season number for a given year/month based on the biannual calendar."""
    if month >= 8:
        # H2: August start; odd season
        return 2 * (year - 2020) + 1
    elif month >= 2:
        # H1: February start; even season
        return 2 * (year - 2021) + 2
    else:
        # January still belongs to the preceding H2 season
        return 2 * ((year - 1) - 2020) + 1


def _infer_season_from_ts(championship_start_ms: int | None) -> int:
    """Return season inferred from ``championship_start`` (ms epoch), or 0 on failure."""
    if not championship_start_ms:
        return 0
    try:
        dt = datetime.fromtimestamp(championship_start_ms / 1000, tz=timezone.utc)
        return _date_to_season(dt.year, dt.month)
    except Exception:
        return 0


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
        # Fallback: infer from championship_start when name and description give nothing
        if not season:
            season = _infer_season_from_ts(championship.get("championship_start"))
        is_po = _is_playoffs(name)

        if division_num is None and MESTAR_RX.search(name):
            division_num = 0

        if season > 0 and season < min_season:
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


async def _load_existing_from_db() -> List[Dict[str, Any]]:
    """Load existing divisions from the DB ``championships`` table."""
    rows = await db_async.fetch_all(
        """
        SELECT championship_id, season, division_num, name, is_playoffs, slug,
               parent_championship_id
        FROM championships
        ORDER BY season DESC, division_num ASC
        """
    )
    return [
        {
            "championship_id": str(r["championship_id"]),
            "name": r["name"] or "",
            "season": int(r["season"]),
            "division_num": int(r["division_num"]),
            "slug": r["slug"] or "",
            "is_playoffs": int(r["is_playoffs"]),
            "parent_championship_id": r.get("parent_championship_id"),
        }
        for r in rows
    ]


async def load_existing() -> List[Dict[str, Any]]:
    """Return existing divisions from the DB."""
    return await _load_existing_from_db()


async def load_divisions_from_db() -> List[Dict[str, Any]]:
    """Load all championships from DB and refresh the in-memory ``faceit_config.DIVISIONS`` cache.

    Call this at application/sync startup after the DB pool is ready.
    """
    rows = await _load_existing_from_db()
    faceit_config.update_divisions(rows)
    return rows


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

    total: int
    created: int
    changed: bool
    new_championship_ids: List[str]
    skipped_championship_ids: List[str]
    stats: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to plain dict for JSON responses."""
        return {
            "total": self.total,
            "created": self.created,
            "changed": self.changed,
            "new_championship_ids": self.new_championship_ids,
            "skipped_championship_ids": self.skipped_championship_ids,
            "stats": dict(self.stats),
        }


async def refresh_divisions(
    *,
    min_season: int = faceit_config.DEFAULT_CURRENT_SEASON,
    require_matches: bool = False,
    min_matches: int = 0,
    min_new_division_teams: int = 0,
    dry_run: bool = False,
) -> DivisionRefreshResult:
    """Discover new championships and upsert them into the DB ``championships`` table.

    Args:
        min_season: Ignore championships with ``season`` lower than this.
        require_matches: When ``True`` also fetch match lists and filter out
            championships with fewer matches than ``min_matches``.
        min_matches: Minimum number of matches required when
            ``require_matches`` is enabled.
        min_new_division_teams: Deprecated no-op kept for backwards compatibility.
        dry_run: When ``True`` do not persist; still return the merged payload
            so callers can inspect changes.

    Returns:
        :class:`DivisionRefreshResult` describing the outcome.
    """

    existing = await load_existing()
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

    merged = non_destructive_merge(existing, discovered)

    new_ids = [
        str(entry.get("championship_id"))
        for entry in merged
        if entry.get("championship_id") and str(entry.get("championship_id")) not in existing_ids
    ]

    changed = merged != existing

    if not dry_run:
        if changed or new_ids:
            async with db_async.connection(label="refresh_divisions") as conn:
                await db_async.upsert_championships_async(conn, merged)
        # Always refresh the in-memory cache so the current process sees updates.
        faceit_config.update_divisions(merged)

    return DivisionRefreshResult(
        total=len(merged),
        created=len(new_ids),
        changed=changed,
        new_championship_ids=new_ids,
        skipped_championship_ids=skipped_new,
        stats=stats,
    )


async def refresh_divisions_dry_run(
    *,
    min_season: int = faceit_config.DEFAULT_CURRENT_SEASON,
    require_matches: bool = False,
    min_matches: int = 0,
    min_new_division_teams: int = 0,
) -> DivisionRefreshResult:
    """Convenience wrapper for :func:`refresh_divisions` with ``dry_run=True``."""

    return await refresh_divisions(
        min_season=min_season,
        require_matches=require_matches,
        min_matches=min_matches,
        min_new_division_teams=min_new_division_teams,
        dry_run=True,
    )
