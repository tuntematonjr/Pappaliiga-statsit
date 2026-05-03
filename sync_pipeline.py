from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

from asyncmy import errors as asyncmy_errors
import httpx

from utils import format_hms, log_stage
from runtime_diagnostics import SyncDiagnostics

from faceit_client_async import (
    get_championship_matches_async,
    get_map_votes_async,
    get_match_details_async,
    get_match_stats_async,
    get_championship_teams_async,
)

import faceit_config
from division_naming import build_division_name
from api.services import team_status_service

from db_async import (
    DEFAULT_TEAM_AVATAR,
    connection,
    delete_stats_for_match_async,
    fetch_all,
    create_snapshot_ts_async,
    get_map_id_lookup_async,
    replace_map_votes_async,
    upsert_championship_async,
    upsert_map_catalog_async,
    upsert_maps_bulk_async,
    upsert_match_async,
    upsert_player_map_season_totals_bulk_async,
    upsert_player_championships_bulk_async,
    upsert_player_season_totals_bulk_async,
    upsert_player_stats_bulk_async,
    upsert_players_bulk_async,
    upsert_team_championships_bulk_async,
    upsert_team_map_season_totals_bulk_async,
    upsert_team_season_totals_bulk_async,
    upsert_team_stats_bulk_async,
    upsert_teams_bulk_async,
)

LOGGER = logging.getLogger(__name__)

PENDING_REFRESH_INTERVAL_SECONDS = 15 * 60  # base interval for pending match refresh
PENDING_UNCHANGED_REFRESH_INTERVAL_SECONDS = 60 * 60  # when pending state/activity is unchanged
PENDING_FORCE_REFRESH_INTERVAL_SECONDS = 6 * 60 * 60  # always refresh eventually as a safety net
TEAM_REFRESH_INTERVAL_SECONDS = 24 * 60 * 60  # throttle team refresh to once per day
FAR_FUTURE_MATCH_SKIP_THRESHOLD_SECONDS = 14 * 24 * 60 * 60  # skip already-known matches scheduled >2 weeks out
_FINAL_MATCH_STATUSES = {"finished", "closed", "over", "completed", "played"}
_NON_FINAL_MATCH_STATUSES = {
    "configured",
    "pending",
    "ready",
    "scheduled",
    "ongoing",
    "live",
    "in_progress",
    "started",
}

__all__ = [
    "ChampionshipSyncResult",
    "sync_championship_async",
    "sync_match_async",
    "update_single_match_async",
]

_DIVISION_BY_CHAMPIONSHIP: Dict[str, Dict[str, Any]] = {
    str(item.get("championship_id")): dict(item)
    for item in faceit_config.DIVISIONS
    if item.get("championship_id")
}

def _get_division_by_championship_id(championship_id: str) -> Dict[str, Any] | None:
    return _DIVISION_BY_CHAMPIONSHIP.get(str(championship_id))


def _canonical_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _canonical_json_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, list):
        canonical_items = [_canonical_json_value(item) for item in value]
        return sorted(
            canonical_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
        )
    if isinstance(value, set):
        return sorted(_canonical_json_value(item) for item in value)
    return value


def _compute_match_payload_hash(normalised: "NormalisedMatch") -> str:
    match_row = normalised.match_row
    payload = {
        "match": {
            "championship_id": match_row.get("championship_id"),
            "best_of": match_row.get("best_of"),
            "configured_at": match_row.get("configured_at"),
            "started_at": match_row.get("started_at"),
            "finished_at": match_row.get("finished_at"),
            "scheduled_at": match_row.get("scheduled_at"),
            "status": match_row.get("status"),
            "team1_id": match_row.get("team1_id"),
            "team2_id": match_row.get("team2_id"),
            "winner_team_id": match_row.get("winner_team_id"),
            "is_forfeit": match_row.get("is_forfeit"),
            "ignored_due_ban": match_row.get("ignored_due_ban"),
        },
        "teams": normalised.team_rows,
        "players": normalised.player_rows,
        "maps": normalised.map_rows,
        "map_votes": normalised.map_votes,
        "player_stats": normalised.player_stats,
        "team_stats": normalised.team_stats,
    }
    canonical = _canonical_json_value(payload)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class MatchContext:
    championship_id: str
    season: int
    division_num: int
    is_playoffs: bool
    banned_team_ids: Set[str]
    banned_lookup: Dict[str, Dict[str, Any]]


@dataclass(slots=True)
class ChampionshipSyncResult:
    championship_id: str
    division_name: str
    season: int
    division_num: int
    total_matches: int
    synced_match_ids: List[str]
    skipped_matches: int
    pending_matches: int
    fetch_elapsed: float
    process_elapsed: float
    total_elapsed: float


def safe_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    """Convert value to int, returning default (0) if None or conversion fails."""
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except Exception:
        return default


def normalize_finished_at(value: Any) -> Optional[int]:
    """Normalize finished_at to None when missing/zero to avoid false positives."""
    ts = safe_int(value, None)
    if not ts or ts <= 0:
        return None
    return ts


def _is_retryable_db_error(exc: Exception) -> bool:
    if isinstance(exc, asyncmy_errors.OperationalError):
        code = exc.args[0] if exc.args else 0
        return code in (1020, 1205, 1213)
    return False


def _is_played_match_state(status: str, finished_at: Optional[int]) -> bool:
    normalized_status = str(status or "").strip().lower()
    if normalized_status in _NON_FINAL_MATCH_STATUSES:
        return False
    if normalized_status in _FINAL_MATCH_STATUSES:
        return True
    return bool(finished_at)


def _match_list_activity_ts(item: Mapping[str, Any] | None) -> int:
    if not item:
        return 0
    return max(
        0,
        safe_int(item.get("finished_at"), 0) or 0,
        safe_int(item.get("started_at"), 0) or 0,
        safe_int(item.get("scheduled_at"), 0) or 0,
        safe_int(item.get("configured_at"), 0) or 0,
    )


def safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    """Convert value to float, returning default (0.0) if None or conversion fails."""
    if value is None:
        return default
    try:
        s = str(value).replace(",", ".").strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _normalise_faceit_stat_value(value: Any) -> Any:
    """Keep Faceit stats values but coerce numeric-looking strings to numbers."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value

    try:
        raw = str(value).strip()
    except Exception:
        return value

    if not raw:
        return None

    # Strip trailing percent sign and normalise decimal separator
    if raw.endswith("%"):
        raw = raw[:-1]
    candidate = raw.replace(",", ".")

    try:
        if re.fullmatch(r"[-+]?\d+", candidate):
            return int(candidate)
        return float(candidate)
    except Exception:
        return raw


def _normalise_player_stats(stats: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a JSON-ready dict that preserves Faceit keys and coerces values."""
    if not isinstance(stats, Mapping):
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in stats.items():
        cleaned[key] = _normalise_faceit_stat_value(value)
    return cleaned


def derive_slug_base(slug: str) -> str:
    """Strip playoff/runko suffixes from slug to get base slug."""
    if not slug:
        return ""
    base = slug.lower().strip()
    # Remove playoff-related suffixes
    base = re.sub(r'-(?:playoffs?|po|pudotuspelit).*$', '', base)
    base = re.sub(r'-(?:rk|runko|regular).*$', '', base)
    base = re.sub(r'-+', '-', base)
    base = base.strip('-')
    return base


async def find_parent_championship_id(
    slug: str,
    season: int,
    division_num: int,
    is_playoffs: bool
) -> Optional[str]:
    """Find parent championship ID for a playoff division.
    
    Returns championship_id of matching non-playoff division, or None.
    """
    if not is_playoffs:
        return None
    
    slug_base = derive_slug_base(slug)
    if not slug_base:
        return None
    
    # Query for matching non-playoff championship
    rows = await fetch_all(
        """
        SELECT championship_id
        FROM championships
        WHERE is_playoffs = 0
          AND season = %s
          AND division_num = %s
          AND slug LIKE %s
        LIMIT 1
        """,
        (season, division_num, f"{slug_base}%")
    )
    
    if rows:
        return str(rows[0]["championship_id"])
    
    return None


def _extract_rounds(stats_json: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    if not isinstance(stats_json, dict):
        return []
    rounds = stats_json.get("rounds") or stats_json.get("roundsStats") or []
    return rounds if isinstance(rounds, list) else []


def _is_bye_value(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() == "bye"


def _is_placeholder_team(team_id: Any, name: Any) -> bool:
    return _is_bye_value(team_id) or _is_bye_value(name)


def _derive_team_ids(details: Dict[str, Any], rounds: Sequence[Dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    teams_section = details.get("teams") if isinstance(details, dict) else {}
    faction1 = teams_section.get("faction1") or {}
    faction2 = teams_section.get("faction2") or {}
    f1_name = (faction1.get("name") or "").strip() or None
    f2_name = (faction2.get("name") or "").strip() or None
    f1_is_bye = _is_bye_value(f1_name)
    f2_is_bye = _is_bye_value(f2_name)

    seen: List[str] = []
    team1_id: Optional[str] = None
    team2_id: Optional[str] = None

    for rnd in rounds:
        for team in rnd.get("teams", []) or []:
            tid = team.get("team_id") or team.get("id") or team.get("faction_id")
            name = (team.get("name") or team.get("team") or "").strip() or None
            if _is_placeholder_team(tid, name):
                continue
            if tid and tid not in seen:
                seen.append(tid)
            if f1_name and name == f1_name and not team1_id and not f1_is_bye:
                team1_id = tid
            if f2_name and name == f2_name and not team2_id and not f2_is_bye:
                team2_id = tid

    if (team1_id is None or team2_id is None) and len(seen) >= 2:
        if team1_id is None:
            team1_id = seen[0]
        if team2_id is None:
            team2_id = next((item for item in seen if item != team1_id), seen[1])

    if team1_id is None:
        fallback = faction1.get("faction_id") or faction1.get("team_id")
        if fallback:
            team1_id = fallback
    if team2_id is None:
        fallback = faction2.get("faction_id") or faction2.get("team_id")
        if fallback:
            team2_id = fallback

    if _is_placeholder_team(team1_id, f1_name):
        team1_id = None
    if _is_placeholder_team(team2_id, f2_name):
        team2_id = None

    return team1_id, team2_id


def _normalize_team_ref(ref: Any, team1_id: Optional[str], team2_id: Optional[str]) -> Optional[str]:
    if ref is None:
        return None
    value = str(ref).strip()
    if not value:
        return None
    lower = value.lower()
    if _is_bye_value(value):
        return None
    if lower in {"faction1", "1", "team1"}:
        return team1_id
    if lower in {"faction2", "2", "team2"}:
        return team2_id
    return value


def _extract_map_rows_from_stats(
    match_id: str,
    rounds: Sequence[Dict[str, Any]],
    team1_id: Optional[str],
    team2_id: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, rnd in enumerate(rounds, start=1):
        rstats = rnd.get("round_stats") or {}
        map_name = rstats.get("Map") or rnd.get("map") or rnd.get("map_name")
        score_raw = (rstats.get("Score") or rstats.get("score") or "").strip()
        score_team1 = score_team2 = None
        if score_raw:
            parts = [p.strip() for p in score_raw.replace(":", "/").split("/") if p.strip()]
            if len(parts) == 2:
                score_team1 = safe_int(parts[0])
                score_team2 = safe_int(parts[1])
        winner = _normalize_team_ref(rstats.get("Winner") or rstats.get("winner"), team1_id, team2_id)

        rows.append(
            {
                "match_id": match_id,
                "round_index": idx,
                "map_name": map_name,
                "score_team1": score_team1,
                "score_team2": score_team2,
                "winner_team_id": winner,
            }
        )
    return rows


def _extract_map_rows_from_details(
    match_id: str,
    details: Dict[str, Any],
    team1_id: Optional[str],
    team2_id: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    status = str((details or {}).get("status") or "").lower()
    finished_at = normalize_finished_at((details or {}).get("finished_at"))
    if not _is_played_match_state(status, finished_at):
        return rows

    det = (details or {}).get("detailed_results")
    if isinstance(det, list) and det:
        for idx, item in enumerate(det, start=1):
            factions = item.get("factions") or {}
            s1 = safe_int((factions.get("faction1") or {}).get("score"))
            s2 = safe_int((factions.get("faction2") or {}).get("score"))
            winner = _normalize_team_ref(item.get("winner"), team1_id, team2_id)
            if s1 in {0, 1} and s2 in {0, 1}:
                if s1 == 1 and s2 == 0:
                    s1, s2 = 13, 0
                elif s2 == 1 and s1 == 0:
                    s1, s2 = 0, 13
            rows.append(
                {
                    "match_id": match_id,
                    "round_index": idx,
                    "map_name": "forfeit",
                    "score_team1": s1,
                    "score_team2": s2,
                    "winner_team_id": winner,
                }
            )
        return rows

    res = (details or {}).get("results") or {}
    score = res.get("score") or {}
    faction1 = safe_int(score.get("faction1"))
    faction2 = safe_int(score.get("faction2"))
    if faction1 is not None and faction2 is not None:
        total_maps = faction1 + faction2
        if total_maps > 0:
            winner = _normalize_team_ref(res.get("winner") or res.get("winner_team_id"), team1_id, team2_id)
            s1, s2 = (13, 0) if winner == team1_id else (0, 13)
            for idx in range(1, total_maps + 1):
                rows.append(
                    {
                        "match_id": match_id,
                        "round_index": idx,
                        "map_name": "forfeit",
                        "score_team1": s1,
                        "score_team2": s2,
                        "winner_team_id": winner,
                    }
                )
    return rows


def _expected_played_maps_from_details(details: Dict[str, Any] | None) -> Optional[int]:
    if not isinstance(details, dict):
        return None
    results = details.get("results") or {}
    score = results.get("score") or {}
    faction1 = safe_int(score.get("faction1"), None)
    faction2 = safe_int(score.get("faction2"), None)
    if faction1 is None or faction2 is None:
        return None
    total_maps = faction1 + faction2
    if total_maps <= 0:
        return None
    return total_maps


def _extract_player_rows(
    match_id: str,
    rounds: Sequence[Dict[str, Any]],
    team1_id: Optional[str],
    team2_id: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, rnd in enumerate(rounds, start=1):
        for team in rnd.get("teams", []) or []:
            tid = team.get("team_id") or team.get("id") or team.get("faction_id")
            name = (team.get("name") or team.get("team") or "").strip() or None
            if _is_placeholder_team(tid, name):
                continue
            for player in team.get("players", []) or []:
                ps_raw = player.get("player_stats") or player.get("stats") or {}
                ps = _normalise_player_stats(ps_raw)
                player_id = player.get("player_id") or player.get("id")
                rows.append(
                    {
                        "match_id": match_id,
                        "round_index": idx,
                        "player_id": player_id,
                        "team_id": tid,
                        "opponent_team_id": None,
                        "nickname": player.get("nickname") or player.get("name"),
                        "stats": ps,
                    }
                )
    for row in rows:
        team_id = row.get("team_id")
        if team_id and team1_id and team2_id:
            row["opponent_team_id"] = team2_id if team_id == team1_id else team1_id
        elif team_id:
            row["opponent_team_id"] = None
    return rows


def _extract_team_rows(
    rounds: Sequence[Dict[str, Any]],
    team1_id: Optional[str],
    team2_id: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, rnd in enumerate(rounds, start=1):
        team_entries = rnd.get("teams") or []
        if not isinstance(team_entries, list):
            continue
        for team in team_entries:
            tid = team.get("team_id") or team.get("id") or team.get("faction_id")
            name = (team.get("name") or team.get("team") or "").strip() or None
            if _is_placeholder_team(tid, name):
                continue
            if not tid:
                continue
            opponent = None
            if tid == team1_id:
                opponent = team2_id
            elif tid == team2_id:
                opponent = team1_id
            stats = team.get("team_stats") or {}
            rows.append(
                {
                    "round_index": idx,
                    "team_id": tid,
                    "opponent_team_id": opponent,
                    "final_score": safe_int(stats.get("Final Score")),
                    "first_half_score": safe_int(stats.get("First Half Score")),
                    "second_half_score": safe_int(stats.get("Second Half Score")),
                    "overtime_score": safe_int(stats.get("Overtime score")),
                    "headshot_pct": safe_float(stats.get("Team Headshots")),
                    "win": safe_int(stats.get("Team Win"), 0) == 1,
                }
            )
    return rows


def _collect_roster_players(details: Dict[str, Any]) -> List[Dict[str, Any]]:
    players: List[Dict[str, Any]] = []
    teams = (details or {}).get("teams") or {}
    for key in ("faction1", "faction2"):
        faction = teams.get(key) or {}
        for player in faction.get("roster", []) or []:
            pid = player.get("player_id") or player.get("id")
            if not pid:
                continue
            nickname = player.get("nickname") or player.get("name") or ""
            players.append(
                {
                    "player_id": pid,
                    "nickname": nickname,
                    "avatar": player.get("avatar") or player.get("avatar_url") or player.get("picture"),
                    "faceit_url": player.get("faceit_url") or player.get("url"),
                }
            )
    return players


def _collect_team_payloads(details: Dict[str, Any], banned_lookup: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    teams = (details or {}).get("teams") or {}
    for key in ("faction1", "faction2"):
        faction = teams.get(key) or {}
        team_id = faction.get("faction_id") or faction.get("team_id")
        if not team_id:
            continue
        info = banned_lookup.get(team_id, {})
        name = faction.get("name") or info.get("team_name")
        if _is_placeholder_team(team_id, name):
            continue
        out.append(
            {
                "team_id": team_id,
                "name": name,
                "avatar": faction.get("avatar") or info.get("avatar") or DEFAULT_TEAM_AVATAR,
            }
        )
    return out


def _normalize_db_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def _filter_stale_team_payloads(team_payloads: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not team_payloads:
        return []

    team_ids = [str(row.get("team_id")) for row in team_payloads if row.get("team_id")]
    if not team_ids:
        return []

    placeholders = ", ".join(["%s"] * len(team_ids))
    rows = await fetch_all(
        f"SELECT team_id, updated_at FROM teams WHERE team_id IN ({placeholders})",
        team_ids,
    )
    updated_lookup = {str(row.get("team_id")): _normalize_db_timestamp(row.get("updated_at")) for row in rows}
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=TEAM_REFRESH_INTERVAL_SECONDS)

    stale: List[Dict[str, Any]] = []
    for row in team_payloads:
        team_id = str(row.get("team_id"))
        updated_at = updated_lookup.get(team_id)
        if not updated_at or updated_at < cutoff:
            stale.append(row)
    return stale


async def _avatar_url_ok(
    url: str,
    client: httpx.AsyncClient,
    cache: Dict[str, bool],
) -> bool:
    if url in cache:
        return cache[url]
    ok = False
    try:
        resp = await client.head(url, follow_redirects=True)
        ok = resp.status_code < 400
        if not ok and resp.status_code in {403, 405, 429}:
            resp = await client.get(
                url,
                headers={"Range": "bytes=0-0"},
                follow_redirects=True,
            )
            ok = resp.status_code < 400
    except httpx.RequestError:
        ok = False
    cache[url] = ok
    return ok


async def _validate_team_avatars(team_payloads: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not team_payloads:
        return []
    cache: Dict[str, bool] = {}
    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=5.0) as client:
        async def validate(row: Dict[str, Any]) -> None:
            avatar = (row.get("avatar") or "").strip()
            if not avatar or avatar == DEFAULT_TEAM_AVATAR:
                row["avatar"] = DEFAULT_TEAM_AVATAR
                return
            async with sem:
                ok = await _avatar_url_ok(avatar, client, cache)
            if not ok:
                row["avatar"] = DEFAULT_TEAM_AVATAR

        await asyncio.gather(*(validate(row) for row in team_payloads))
    return list(team_payloads)


async def _build_championship_team_payloads(
    championship_id: str,
    status_entries: Sequence[Dict[str, Any]],
    *,
    validate_avatars: bool = False,
) -> List[Dict[str, Any]]:
    status_lookup = {entry.get("team_id"): entry for entry in status_entries if entry.get("team_id")}
    team_payloads: List[Dict[str, Any]] = []

    try:
        teams_api = await get_championship_teams_async(championship_id, limit=100)
    except Exception:
        teams_api = None
    if teams_api:
        for team in teams_api:
            team_id = team.get("team_id") or team.get("id")
            if not team_id:
                continue
            override = status_lookup.get(team_id, {})
            name = team.get("name") or team.get("nickname") or override.get("team_name")
            if _is_placeholder_team(team_id, name):
                continue
            avatar = team.get("avatar") or override.get("avatar") or DEFAULT_TEAM_AVATAR
            team_payloads.append(
                {
                    "team_id": team_id,
                    "name": name,
                    "avatar": avatar,
                }
            )
    else:
        team_payloads = [
            {
                "team_id": entry["team_id"],
                "name": entry.get("team_name"),
                "avatar": entry.get("avatar") or DEFAULT_TEAM_AVATAR,
            }
            for entry in status_entries
            if entry.get("team_id") and not _is_placeholder_team(entry.get("team_id"), entry.get("team_name"))
        ]

    stale_payloads = await _filter_stale_team_payloads(team_payloads)
    if not validate_avatars:
        return stale_payloads
    return await _validate_team_avatars(stale_payloads)


def _map_votes_from_democracy(match_id: str, demo_json: Dict[str, Any], team_lookup: Dict[str, str], best_of: int = 3) -> List[Dict[str, Any]]:
    """Parse map votes from Democracy API response.
    
    Args:
        match_id: The match ID
        demo_json: Democracy API response
        team_lookup: Mapping of faction names to team IDs
        best_of: Match format (2 for BO2, 3 for BO3)
    
    Returns:
        List of map vote dictionaries with proper status marking:
        - BO3 round 7: status='decider' (leftover map)
        - BO2 round 7: status='overflow' (7th map)
        - Other rounds: status from API ('drop' or 'pick')
    """
    payload = demo_json.get("payload") if isinstance(demo_json, dict) else None
    tickets = payload.get("tickets", []) if isinstance(payload, dict) else []
    votes: List[Dict[str, Any]] = []
    for ticket in tickets:
        if not isinstance(ticket, dict):
            continue
        if str(ticket.get("entity_type") or "").lower() != "map":
            continue
        # Each ticket has an "entities" array with individual map votes
        entities = ticket.get("entities", [])
        if not isinstance(entities, list):
            continue
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            round_num = safe_int(entity.get("round"))
            map_name = entity.get("guid")
            status = entity.get("status")
            faction = entity.get("selected_by")
            
            # Determine if this is decider (BO3 round 7) or overflow (BO2 round 7)
            if round_num == 7:
                if best_of == 3:
                    status = "decider"  # BO3: last map is the leftover decider
                elif best_of == 2:
                    status = "overflow"  # BO2: 7th pick is the overflow map
            
            team_id = None
            if faction:
                team_id = team_lookup.get(str(faction))
            votes.append(
                {
                    "match_id": match_id,
                    "map_name": map_name,
                    "status": status,
                    "selected_by_faction": faction,
                    "selected_by_team_id": team_id,
                    "round_num": round_num,
                }
            )
    return votes


@dataclass(slots=True)
class NormalisedMatch:
    context: MatchContext
    match_row: Dict[str, Any]
    team_rows: List[Dict[str, Any]]
    player_rows: List[Dict[str, Any]]
    map_rows: List[Dict[str, Any]]
    team_stats: List[Dict[str, Any]]
    player_stats: List[Dict[str, Any]]
    map_votes: List[Dict[str, Any]]
    affected_players: Set[str]
    affected_teams: Set[str]


def _build_normalised_match(
    ctx: MatchContext,
    details: Dict[str, Any] | None,
    stats: Dict[str, Any] | None,
    votes_json: Dict[str, Any] | None,
) -> NormalisedMatch:
    if not details:
        raise ValueError("Match details payload required")

    match_id = details.get("match_id")
    if not match_id:
        raise ValueError("Match payload missing match_id")

    status = str((details or {}).get("status") or "").lower()
    raw_finished_at = normalize_finished_at((details or {}).get("finished_at"))
    has_played = _is_played_match_state(status, raw_finished_at)

    rounds = _extract_rounds(stats)
    team1_id, team2_id = _derive_team_ids(details, rounds)

    map_rows: List[Dict[str, Any]] = []
    if has_played:
        map_rows = _extract_map_rows_from_stats(match_id, rounds, team1_id, team2_id)
        if not map_rows:
            map_rows = _extract_map_rows_from_details(match_id, details, team1_id, team2_id)

    for map_row in map_rows:
        s1 = map_row.get("score_team1") or 0
        s2 = map_row.get("score_team2") or 0
        is_forfeit = False
        if map_row.get("map_name") == "forfeit":
            is_forfeit = True
        elif (s1 + s2) == 0:
            is_forfeit = True
        map_row["is_forfeit"] = 1 if is_forfeit else 0

    player_rows = _extract_player_rows(match_id, rounds, team1_id, team2_id)
    team_stat_rows = _extract_team_rows(rounds, team1_id, team2_id)

    team_lookup = {
        "faction1": team1_id or "",
        "faction2": team2_id or "",
        str(team1_id or ""): team1_id or "",
        str(team2_id or ""): team2_id or "",
    }

    best_of = safe_int(details.get("best_of")) or 3
    map_votes = _map_votes_from_democracy(match_id, votes_json or {}, team_lookup, best_of)

    teams_payload = _collect_team_payloads(details, ctx.banned_lookup)
    roster_players = _collect_roster_players(details)

    payload_players = {item["player_id"]: item for item in roster_players if item.get("player_id")}
    for row in player_rows:
        pid = row.get("player_id")
        nickname = (row.get("nickname") or "").strip()
        if pid and nickname and pid not in payload_players:
            payload_players[pid] = {"player_id": pid, "nickname": nickname}

    finish_ts = raw_finished_at if has_played else None
    winner_team_id = None
    res = (details or {}).get("results") or {}
    if has_played:
        winner_team_id = _normalize_team_ref(res.get("winner"), team1_id, team2_id)
    scheduled_at = safe_int(details.get("scheduled_at"))
    configured_at = safe_int(details.get("configured_at"))
    started_at = safe_int(details.get("started_at"))

    match_is_forfeit = all(row.get("is_forfeit") for row in map_rows) if map_rows else False
    ignored_due_ban = 1 if (team1_id in ctx.banned_team_ids or team2_id in ctx.banned_team_ids) else 0
    last_seen_at = int(time.time())
    activity_candidates = [finish_ts, started_at, scheduled_at, configured_at, last_seen_at]
    activity_ts = max((int(val) for val in activity_candidates if val), default=last_seen_at)

    match_row = {
        "match_id": match_id,
        "championship_id": ctx.championship_id,
        "season": ctx.season,
        "division_num": ctx.division_num,
        "best_of": safe_int(details.get("best_of")),
        "round_number": safe_int(details.get("round")),
        "configured_at": configured_at,
        "started_at": started_at,
        "finished_at": finish_ts,
        "scheduled_at": scheduled_at,
        "status": details.get("status"),
        "last_seen_at": last_seen_at,
        "activity_ts": activity_ts,
        "team1_id": team1_id,
        "team2_id": team2_id,
        "winner_team_id": winner_team_id,
        "is_forfeit": 1 if match_is_forfeit else 0,
        "ignored_due_ban": ignored_due_ban,
    }

    affected_players: Set[str] = {row["player_id"] for row in player_rows if row.get("player_id")}
    affected_teams: Set[str] = set()
    if team1_id:
        affected_teams.add(team1_id)
    if team2_id:
        affected_teams.add(team2_id)

    return NormalisedMatch(
        context=ctx,
        match_row=match_row,
        team_rows=teams_payload,
        player_rows=list(payload_players.values()),
        map_rows=map_rows,
        team_stats=team_stat_rows,
        player_stats=player_rows,
        map_votes=map_votes,
        affected_players=affected_players,
        affected_teams=affected_teams,
    )


async def sync_match_async(
    championship_id: str,
    season: int,
    division_num: int,
    match_id: str,
    *,
    is_playoffs: bool,
    banned_lookup: Dict[str, Dict[str, Any]],
    require_complete_played_maps: bool = False,
    db_semaphore: asyncio.Semaphore | None = None,
    diagnostics: SyncDiagnostics | None = None,
) -> NormalisedMatch:
    overall_start = time.perf_counter()
    fetch_start = time.perf_counter()
    LOGGER.debug("Starting sync for match %s", match_id)
    ctx = MatchContext(
        championship_id=championship_id,
        season=season,
        division_num=division_num,
        is_playoffs=is_playoffs,
        banned_team_ids=set(banned_lookup.keys()),
        banned_lookup=banned_lookup,
    )

    details = await get_match_details_async(match_id)
    if not details:
        raise RuntimeError(f"Match {match_id} details missing")

    status = str(details.get("status") or "").lower()
    finish_ts = normalize_finished_at(details.get("finished_at"))
    has_played = _is_played_match_state(status, finish_ts)

    # For unplayed matches: skip stats/votes to reduce API calls
    # For played matches: fetch full stats and map voting history
    stats = None
    votes: Dict[str, Any] | None = None
    api_call_count = 1
    if has_played:
        LOGGER.debug("Match %s is played - fetching stats and votes", match_id)
        stats_task = asyncio.create_task(get_match_stats_async(match_id))
        votes_task = asyncio.create_task(get_map_votes_async(match_id))
        stats = await stats_task
        votes = await votes_task
        api_call_count += 2
    else:
        LOGGER.debug("Match %s not played - skipping stats/votes, updating basic info only", match_id)
        votes = {}
    fetch_elapsed = time.perf_counter() - fetch_start

    normalise_start = time.perf_counter()
    normalised = _build_normalised_match(ctx, details, stats, votes)
    normalise_elapsed = time.perf_counter() - normalise_start

    if require_complete_played_maps:
        expected_maps = _expected_played_maps_from_details(details)
        if expected_maps is not None:
            map_count = len(normalised.map_rows)
            status = str(normalised.match_row.get("status") or "").lower()
            finished_at = normalize_finished_at(normalised.match_row.get("finished_at"))
            is_played = _is_played_match_state(status, finished_at)
            if is_played and map_count < expected_maps:
                raise RuntimeError(
                    f"match_maps_incomplete: expected={expected_maps} got={map_count} match_id={match_id}"
                )

    current_task = asyncio.current_task()
    if current_task:
        try:
            current_task.set_name(f"match:{match_id}")
        except Exception:
            pass

    forfeit_lookup = {
        int(row["round_index"]): bool(row.get("is_forfeit"))
        for row in normalised.map_rows
        if row.get("round_index") is not None
    }
    normalised.match_row["payload_hash"] = _compute_match_payload_hash(normalised)

    persist_start = time.perf_counter()
    match_status = str(normalised.match_row.get("status") or "").lower()
    match_is_played = _is_played_match_state(match_status, normalize_finished_at(normalised.match_row.get("finished_at")))

    map_catalog_rows: list[dict[str, str]] = []
    try:
        voting = (details or {}).get("voting") or {}
        map_section = voting.get("map") if isinstance(voting, dict) else None
        entities = map_section.get("entities") if isinstance(map_section, dict) else None
        if isinstance(entities, list):
            seen_map_ids: set[str] = set()
            for ent in entities:
                if not isinstance(ent, dict):
                    continue
                map_id = ent.get("guid") or ent.get("game_map_id") or ent.get("class_name")
                if not map_id:
                    continue
                map_id = str(map_id).strip()
                if not map_id or map_id in seen_map_ids:
                    continue
                seen_map_ids.add(map_id)
                pretty = ent.get("name") or map_id
                image_sm = ent.get("image_sm") or ent.get("image") or ""
                image_lg = ent.get("image_lg") or image_sm or ""
                map_catalog_rows.append(
                    {
                        "map_id": map_id,
                        "pretty_name": str(pretty or map_id),
                        "image_sm": str(image_sm or ""),
                        "image_lg": str(image_lg or ""),
                    }
                )
    except Exception:
        LOGGER.debug("Failed to parse Faceit map images for match %s", match_id, exc_info=True)

    if db_semaphore is not None:
        await db_semaphore.acquire()
    try:
        match_championship_id = normalised.match_row.get("championship_id")
        payload_hash = normalised.match_row.get("payload_hash")
        existing_payload_hash: str | None = None
        snapshot_ts: int | None = None
        map_lookup: dict[int, int] = {}
        skip_heavy_persist = False
        async def _run_core_persist() -> None:
            nonlocal payload_hash, existing_payload_hash, snapshot_ts, map_lookup, skip_heavy_persist
            existing_payload_hash = None
            snapshot_ts = None
            map_lookup = {}
            skip_heavy_persist = False

            async with connection(label=f"match:{match_id}:core") as core_conn:
                async with core_conn.cursor() as cur:
                    await cur.execute("SELECT payload_hash FROM matches WHERE match_id = %s", (match_id,))
                    existing_row = await cur.fetchone()
                    if existing_row:
                        existing_payload_hash = str(existing_row[0] or "").strip() or None

                    await cur.execute(
                        "SELECT championship_id FROM championships WHERE championship_id = %s",
                        (match_championship_id,),
                    )
                    champ_exists = await cur.fetchone()

                if not champ_exists:
                    LOGGER.warning(
                        "Championship %s MISSING for match %s! Looking for alternative championship with S%d D%d playoffs=%d",
                        match_championship_id,
                        match_id,
                        ctx.season,
                        ctx.division_num,
                        1 if ctx.is_playoffs else 0,
                    )
                    async with core_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT championship_id FROM championships WHERE season = %s AND division_num = %s AND is_playoffs = %s LIMIT 1",
                            (ctx.season, ctx.division_num, 1 if ctx.is_playoffs else 0),
                        )
                        alternative = await cur.fetchone()

                    if alternative:
                        alternative_id = alternative[0]
                        LOGGER.info("Found alternative championship %s - remapping match %s", alternative_id, match_id)
                        normalised.match_row["championship_id"] = alternative_id
                        normalised.match_row["payload_hash"] = _compute_match_payload_hash(normalised)
                        payload_hash = normalised.match_row.get("payload_hash")
                    else:
                        LOGGER.error(
                            "No alternative championship found for S%d D%d playoffs=%d - match %s will fail FK constraint!",
                            ctx.season,
                            ctx.division_num,
                            1 if ctx.is_playoffs else 0,
                            match_id,
                        )

                if normalised.team_rows:
                    await upsert_teams_bulk_async(
                        normalised.team_rows,
                        conn=core_conn,
                        label=f"match:{match_id}:teams",
                    )
                    team_champ_rows = [
                        {
                            "team_id": row["team_id"],
                            "championship_id": normalised.match_row["championship_id"],
                            "team_name": row.get("name"),
                        }
                        for row in normalised.team_rows
                    ]
                    await upsert_team_championships_bulk_async(
                        team_champ_rows,
                        conn=core_conn,
                        label=f"match:{match_id}:team_champs",
                    )
                # Ensure every team FK referenced by this payload exists in `teams`.
                known_team_ids = {
                    str(row.get("team_id"))
                    for row in normalised.team_rows
                    if row.get("team_id")
                }
                referenced_team_ids: set[str] = set()
                for candidate in (
                    normalised.match_row.get("team1_id"),
                    normalised.match_row.get("team2_id"),
                    normalised.match_row.get("winner_team_id"),
                ):
                    if candidate:
                        referenced_team_ids.add(str(candidate))
                for row in normalised.map_rows:
                    tid = row.get("winner_team_id")
                    if tid:
                        referenced_team_ids.add(str(tid))
                for row in normalised.team_stats:
                    for key in ("team_id", "opponent_team_id"):
                        tid = row.get(key)
                        if tid:
                            referenced_team_ids.add(str(tid))
                for row in normalised.player_stats:
                    for key in ("team_id", "opponent_team_id"):
                        tid = row.get(key)
                        if tid:
                            referenced_team_ids.add(str(tid))

                missing_team_rows = [
                    {"team_id": tid, "name": None, "avatar": None}
                    for tid in sorted(referenced_team_ids - known_team_ids)
                ]
                if missing_team_rows:
                    LOGGER.warning(
                        "Match %s referenced %d team_id(s) missing from team payload; inserting placeholder team rows",
                        match_id,
                        len(missing_team_rows),
                    )
                    await upsert_teams_bulk_async(
                        missing_team_rows,
                        conn=core_conn,
                        label=f"match:{match_id}:teams-fk-guard",
                    )
                if normalised.player_rows:
                    await upsert_players_bulk_async(
                        normalised.player_rows,
                        conn=core_conn,
                        label=f"match:{match_id}:players",
                    )
                    player_champ_rows = [
                        {
                            "player_id": row["player_id"],
                            "championship_id": normalised.match_row["championship_id"],
                            "player_name": row.get("nickname") or row.get("name"),
                        }
                        for row in normalised.player_rows
                        if row.get("player_id")
                    ]
                    if player_champ_rows:
                        await upsert_player_championships_bulk_async(
                            player_champ_rows,
                            conn=core_conn,
                            label=f"match:{match_id}:player_champs",
                        )
                if map_catalog_rows:
                    for row in map_catalog_rows:
                        try:
                            await upsert_map_catalog_async(core_conn, row, commit=False)
                        except Exception:
                            LOGGER.debug("Failed to upsert maps_catalog entry for %s", row.get("map_id"), exc_info=True)

                await upsert_match_async(core_conn, normalised.match_row, commit=False)

                if match_is_played and payload_hash and existing_payload_hash == payload_hash:
                    skip_heavy_persist = True
                else:
                    if normalised.map_rows:
                        await upsert_maps_bulk_async(
                            normalised.match_row["match_id"],
                            ctx.season,
                            ctx.division_num,
                            normalised.map_rows,
                            allow_shrink=not match_is_played,
                            conn=core_conn,
                            label=f"match:{match_id}:maps",
                        )
                    elif not match_is_played:
                        # Keep unplayed/live matches mapless; clears stale synthetic rows.
                        async with core_conn.cursor() as cur:
                            await cur.execute("DELETE FROM maps WHERE match_id = %s", (match_id,))

                    map_lookup = await get_map_id_lookup_async(core_conn, match_id)
                    if normalised.match_row.get("finished_at"):
                        snapshot_ts = await create_snapshot_ts_async(
                            core_conn,
                            ctx.season,
                            ctx.division_num,
                            match_id=match_id,
                            label=f"match:{match_id}:snapshot",
                        )

        max_core_attempts = 3
        for attempt in range(1, max_core_attempts + 1):
            try:
                await _run_core_persist()
                break
            except Exception as exc:
                if attempt < max_core_attempts and _is_retryable_db_error(exc):
                    delay = 0.1 * (2 ** (attempt - 1))
                    LOGGER.warning(
                        "Retrying core persist for match %s after %s (attempt %d/%d, sleep %.2fs)",
                        match_id,
                        getattr(exc, "args", exc),
                        attempt,
                        max_core_attempts,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

        if not skip_heavy_persist:
            async def _run_stats_persist() -> None:
                async with connection(label=f"match:{match_id}:stats") as stats_conn:
                    await replace_map_votes_async(
                        match_id,
                        ctx.season,
                        ctx.division_num,
                        normalised.map_votes,
                        conn=stats_conn,
                        label=f"match:{match_id}:votes",
                    )

                    await delete_stats_for_match_async(
                        match_id,
                        conn=stats_conn,
                        label=f"match:{match_id}:delete-stats",
                    )

                    await upsert_player_stats_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        match_id,
                        map_lookup,
                        normalised.player_stats,
                        forfeit_lookup,
                        conn=stats_conn,
                        label=f"match:{match_id}:player-stats",
                    )

                    await upsert_team_stats_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        match_id,
                        map_lookup,
                        normalised.team_stats,
                        forfeit_lookup,
                        conn=stats_conn,
                        label=f"match:{match_id}:team-stats",
                    )

            max_stats_attempts = 5
            for attempt in range(1, max_stats_attempts + 1):
                try:
                    await _run_stats_persist()
                    break
                except Exception as exc:
                    if attempt < max_stats_attempts and _is_retryable_db_error(exc):
                        delay = 0.1 * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                        LOGGER.warning(
                            "Retrying stats persist for match %s after %s (attempt %d/%d, sleep %.2fs)",
                            match_id,
                            getattr(exc, "args", exc),
                            attempt,
                            max_stats_attempts,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    raise

            affected_team_ids = sorted({str(tid) for tid in normalised.affected_teams if tid})
            affected_player_ids = sorted({str(pid) for pid in normalised.affected_players if pid})
            touched_map_names = sorted(
                {
                    str(row.get("map_name")).strip()
                    for row in normalised.map_rows
                    if row.get("map_name")
                    and str(row.get("map_name")).strip()
                    and str(row.get("map_name")).strip().lower() != "forfeit"
                }
            )

            if match_is_played:
                if affected_team_ids:
                    await upsert_team_season_totals_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        affected_team_ids,
                        snapshot_ts=snapshot_ts,
                        label=f"match:{match_id}:team-season-bulk",
                    )
                    await upsert_team_map_season_totals_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        affected_team_ids,
                        map_names=touched_map_names,
                        label=f"match:{match_id}:team-map-bulk",
                    )
                if affected_player_ids:
                    await upsert_player_season_totals_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        affected_player_ids,
                        label=f"match:{match_id}:player-season-bulk",
                    )
                    await upsert_player_map_season_totals_bulk_async(
                        ctx.season,
                        ctx.division_num,
                        affected_player_ids,
                        label=f"match:{match_id}:player-map-bulk",
                    )
            else:
                LOGGER.debug(
                    "Skipping aggregate totals refresh for pending match %s (status=%s)",
                    match_id,
                    match_status or "unknown",
                )
        else:
            LOGGER.debug("Skipping stats/votes rewrite for match %s; payload hash unchanged", match_id)
    finally:
        if db_semaphore is not None:
            db_semaphore.release()

    if diagnostics:
        diagnostics.mark_progress("match", match_id)

    persist_elapsed = time.perf_counter() - persist_start
    total_elapsed = time.perf_counter() - overall_start

    log_stage(
        LOGGER,
        "fetch",
        fetch_elapsed,
        counts={"api_calls": api_call_count},
        prefix=f"match {match_id}",
    )
    log_stage(
        LOGGER,
        "normalise",
        normalise_elapsed,
        counts={
            "maps": len(normalised.map_rows),
            "player_stats": len(normalised.player_stats),
            "team_stats": len(normalised.team_stats),
        },
        prefix=f"match {match_id}",
    )
    log_stage(
        LOGGER,
        "persist",
        persist_elapsed,
        counts={
            "teams": len(normalised.team_rows),
            "players": len(normalised.player_rows),
            "maps": len(normalised.map_rows),
            "player_stats": len(normalised.player_stats),
            "team_stats": len(normalised.team_stats),
            "map_votes": len(normalised.map_votes),
        },
        prefix=f"match {match_id}",
    )
    LOGGER.debug("Match %s synced in %s", match_id, format_hms(total_elapsed))
    return normalised


async def sync_championship_async(
    championship_id: str,
    *,
    division: Mapping[str, Any] | None = None,
    full: bool = False,
    end_on_error: bool = False,
    db_semaphore: asyncio.Semaphore | None = None,
    max_match_concurrency: int = 1,
    validate_avatars: bool = False,
    diagnostics: SyncDiagnostics | None = None,
) -> ChampionshipSyncResult:
    start_time = time.perf_counter()
    force_all_matches = full
    division_info = division or _get_division_by_championship_id(championship_id)
    if not division_info:
        raise ValueError(f"Championship {championship_id} not found in DIVISIONS")

    season = division_info["season"]
    division_num = division_info["division_num"]
    division_name = build_division_name(season, division_num, division_info.get("is_playoffs"))
    
    LOGGER.info(
        "Starting sync for championship %s (%s, Season %d, Division %d)",
        championship_id,
        division_name,
        season,
        division_num,
    )
    slug = division_info.get("slug") or f"div{division_num}-s{season}"
    is_playoffs = bool(division_info.get("is_playoffs"))

    status_entries = await team_status_service.list_team_statuses(championship_id)
    banned_lookup = {entry["team_id"]: entry for entry in status_entries}
    team_payloads = await _build_championship_team_payloads(
        championship_id,
        status_entries,
        validate_avatars=validate_avatars,
    )

    # Find parent championship if this is a playoff division
    parent_championship_id = await find_parent_championship_id(slug, season, division_num, is_playoffs)

    async with connection() as conn:
        champ_row = {
            "championship_id": championship_id,
            "season": season,
            "division_num": division_num,
            "name": division_name,
            "is_playoffs": 1 if is_playoffs else 0,
            "slug": slug,
            "parent_championship_id": parent_championship_id,
        }
        
        await upsert_championship_async(conn, champ_row)

        if team_payloads:
            await upsert_teams_bulk_async(team_payloads, conn=conn)
            # Save historical team names/logos for this championship
            team_champ_rows = [
                {
                    "team_id": row["team_id"],
                    "championship_id": championship_id,
                    "team_name": row.get("name"),
                    "avatar": row.get("avatar") or DEFAULT_TEAM_AVATAR,
                }
                for row in team_payloads
            ]
            await upsert_team_championships_bulk_async(team_champ_rows, conn=conn)

    fetch_start_time = time.perf_counter()
    match_type = "all"
    if season < faceit_config.CURRENT_SEASON:
        match_type = "past"
    LOGGER.info("Fetching matches for championship %s type=%s", championship_id, match_type)
    matches = await get_championship_matches_async(championship_id, match_type=match_type)
    fetch_elapsed = time.perf_counter() - fetch_start_time
    
    # Filter out matches with 'bye' placeholder team_id
    filtered_matches = []
    for item in matches:
        if not item.get("match_id"):
            continue
        # Check if either team is a 'bye' placeholder
        teams = item.get("teams", {})
        faction1 = teams.get("faction1", {})
        faction2 = teams.get("faction2", {})
        
        f1_id = faction1.get("faction_id") or faction1.get("team_id")
        f1_name = faction1.get("name")
        f2_id = faction2.get("faction_id") or faction2.get("team_id")
        f2_name = faction2.get("name")
        
        # Skip if either team is a 'bye' placeholder
        if _is_placeholder_team(f1_id, f1_name) or _is_placeholder_team(f2_id, f2_name):
            LOGGER.info("Skipping match %s with 'bye' placeholder team (f1: %s/%s, f2: %s/%s)",
                       item.get("match_id"), f1_id, f1_name, f2_id, f2_name)
            continue
        
        filtered_matches.append(item)

    # Fallback: populate team_championships from match list if championship teams endpoint is empty
    # (common for playoffs before teams are exposed in the teams API).
    if filtered_matches:
        seen_team_ids: set[str] = set()
        match_team_payloads: List[Dict[str, Any]] = []
        for item in filtered_matches:
            for payload in _collect_team_payloads(item, banned_lookup):
                team_id = payload.get("team_id")
                if not team_id or team_id in seen_team_ids:
                    continue
                seen_team_ids.add(team_id)
                match_team_payloads.append(payload)
        if match_team_payloads:
            await upsert_teams_bulk_async(
                match_team_payloads,
                label=f"champ:{championship_id}:matchlist-teams",
            )
            match_team_champ_rows = [
                {
                    "team_id": row["team_id"],
                    "championship_id": championship_id,
                    "team_name": row.get("name"),
                }
                for row in match_team_payloads
            ]
            await upsert_team_championships_bulk_async(
                match_team_champ_rows,
                label=f"champ:{championship_id}:matchlist-team-champs",
            )
    
    match_summaries = {
        str(item.get("match_id")): item
        for item in filtered_matches
        if item.get("match_id")
    }
    match_ids = list(match_summaries.keys())
    total_matches = len(match_ids)
    if len(matches) > len(filtered_matches):
        LOGGER.info("Filtered out %d matches with 'bye' teams (from %d total)",
                   len(matches) - len(filtered_matches), len(matches))

    existing_rows = await fetch_all(
        """
        SELECT match_id, finished_at, status, last_seen_at, activity_ts
        FROM matches
        WHERE championship_id = %s
        """,
        (championship_id,),
    )
    existing_lookup = {str(row["match_id"]): row for row in existing_rows if row.get("match_id")}
    LOGGER.debug("Found %d existing matches in database", len(existing_lookup))
    if existing_lookup:
        sample_keys = list(existing_lookup.keys())[:3]
        for key in sample_keys:
            row = existing_lookup[key]
            LOGGER.debug(
                "Sample existing match: %s, finished_at=%s, status='%s'",
                key,
                row.get("finished_at"),
                row.get("status"),
            )

    synced: List[str] = []
    skipped_matches = 0
    skipped_far_future = 0
    pending_matches = 0
    deferred_pending = 0
    LOGGER.info(
        "Processing %d matches, force_all_matches=%s, existing_lookup has %d entries",
        total_matches,
        force_all_matches,
        len(existing_lookup),
    )

    matches_to_sync: List[str] = []
    process_start_time = time.perf_counter()
    now_ts = time.time()
    for match_id in match_ids:
        if not match_id:
            continue
        LOGGER.debug("Evaluating match %s", match_id)
        if not force_all_matches:
            existing = existing_lookup.get(str(match_id))
            if existing:
                existing_finished = normalize_finished_at(existing.get("finished_at"))
                existing_status = str(existing.get("status") or "").lower()
                existing_activity = safe_int(existing.get("activity_ts"), 0) or 0
                summary = match_summaries.get(str(match_id), {})
                summary_status = str(summary.get("status") or "").lower()
                summary_finished = normalize_finished_at(summary.get("finished_at"))
                summary_activity = _match_list_activity_ts(summary)
                LOGGER.debug(
                    "Match %s: existing_finished=%s, existing_status='%s', existing_activity=%s, summary_status='%s', summary_activity=%s",
                    match_id,
                    existing_finished,
                    existing_status,
                    existing_activity,
                    summary_status,
                    summary_activity,
                )
                
                # Only skip if match is fully played/finished
                if _is_played_match_state(existing_status, existing_finished):
                    LOGGER.debug(
                        "Skipping already played match %s (finished_at=%s, status=%s)",
                        match_id,
                        existing_finished,
                        existing_status,
                    )
                    skipped_matches += 1
                    continue

                # Skip already-known matches that are still more than 2 weeks in the future;
                # --full bypasses this entire block so full syncs always process them.
                summary_scheduled_at = safe_int(summary.get("scheduled_at"), 0)
                if summary_scheduled_at and (summary_scheduled_at - now_ts) > FAR_FUTURE_MATCH_SKIP_THRESHOLD_SECONDS:
                    LOGGER.debug(
                        "Skipping far-future match %s (scheduled_at=%s, in %s)",
                        match_id,
                        summary_scheduled_at,
                        format_hms(summary_scheduled_at - now_ts),
                    )
                    skipped_far_future += 1
                    skipped_matches += 1
                    continue

                else:
                    last_seen_at = safe_int(existing.get("last_seen_at"))
                    if last_seen_at:
                        age_seconds = now_ts - last_seen_at
                        summary_says_played = _is_played_match_state(summary_status, summary_finished)
                        pending_unchanged = (
                            not summary_says_played
                            and summary_status == existing_status
                            and summary_activity > 0
                            and existing_activity > 0
                            and summary_activity <= existing_activity
                        )
                        refresh_interval = (
                            PENDING_UNCHANGED_REFRESH_INTERVAL_SECONDS
                            if pending_unchanged
                            else PENDING_REFRESH_INTERVAL_SECONDS
                        )
                        if age_seconds < refresh_interval and age_seconds < PENDING_FORCE_REFRESH_INTERVAL_SECONDS:
                            deferred_pending += 1
                            pending_matches += 1
                            LOGGER.debug(
                                "Deferring refresh for pending match %s; last seen %s ago (< %s, unchanged=%s)",
                                match_id,
                                format_hms(age_seconds),
                                format_hms(refresh_interval),
                                pending_unchanged,
                            )
                            continue

                    LOGGER.debug("Match %s exists but not finished - scheduling refresh", match_id)
                    pending_matches += 1
        matches_to_sync.append(str(match_id))

    match_concurrency = max(1, int(max_match_concurrency))
    match_sem = asyncio.Semaphore(match_concurrency)

    async def _sync_one(match_id: str) -> str:
        max_match_attempts = 5
        for attempt in range(1, max_match_attempts + 1):
            try:
                async with match_sem:
                    await sync_match_async(
                        championship_id,
                        season,
                        division_num,
                        match_id,
                        is_playoffs=is_playoffs,
                        banned_lookup=banned_lookup,
                        db_semaphore=db_semaphore,
                        diagnostics=diagnostics,
                    )
                return match_id
            except Exception as exc:
                if attempt < max_match_attempts and _is_retryable_db_error(exc):
                    delay = 0.25 * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    LOGGER.warning(
                        "Retrying match sync for %s after %s (attempt %d/%d, sleep %.2fs)",
                        match_id,
                        getattr(exc, "args", exc),
                        attempt,
                        max_match_attempts,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    if matches_to_sync:
        worker_count = min(match_concurrency, len(matches_to_sync))
        next_index = 0
        first_exc: Exception | None = None
        index_lock = asyncio.Lock()

        async def _next_match_id() -> str | None:
            nonlocal next_index
            async with index_lock:
                if next_index >= len(matches_to_sync):
                    return None
                match_id = matches_to_sync[next_index]
                next_index += 1
                return match_id

        async def _worker() -> None:
            nonlocal first_exc
            while True:
                if end_on_error and first_exc is not None:
                    return
                next_match = await _next_match_id()
                if not next_match:
                    return
                try:
                    synced_id = await _sync_one(next_match)
                    synced.append(synced_id)
                except Exception as exc:
                    if end_on_error:
                        if first_exc is None:
                            first_exc = exc
                        return
                    LOGGER.exception("Failed to sync match %s: %s", next_match, exc)

        await asyncio.gather(*(asyncio.create_task(_worker()) for _ in range(worker_count)))
        if first_exc is not None:
            raise first_exc
    
    process_elapsed = time.perf_counter() - process_start_time
    total_elapsed = time.perf_counter() - start_time
    LOGGER.info(
        "Championship %s (%s) summary: total=%d, synced=%d, skipped=%d, pending=%d",
        championship_id,
        division_name,
        total_matches,
        len(synced),
        skipped_matches,
        pending_matches,
    )
    if skipped_far_future:
        LOGGER.info(
            "Skipped %d far-future match(es) (scheduled >%s out; use --full to sync them)",
            skipped_far_future,
            format_hms(FAR_FUTURE_MATCH_SKIP_THRESHOLD_SECONDS),
        )
    if deferred_pending:
        LOGGER.info(
            "Deferred %d pending match(es) refreshed within %s",
            deferred_pending,
            format_hms(PENDING_REFRESH_INTERVAL_SECONDS),
        )
    log_stage(
        LOGGER,
        "fetch",
        fetch_elapsed,
        counts={"matches": total_matches},
        prefix=f"championship {championship_id}",
    )
    log_stage(
        LOGGER,
        "process",
        process_elapsed,
        counts={
            "synced_matches": len(synced),
            "skipped_matches": skipped_matches,
            "pending_matches": pending_matches,
        },
        prefix=f"championship {championship_id}",
    )
    LOGGER.info(
        "Championship %s completed in %s",
        championship_id,
        format_hms(total_elapsed),
    )
    if diagnostics:
        diagnostics.mark_progress("championship", championship_id)
    return ChampionshipSyncResult(
        championship_id=championship_id,
        division_name=division_name,
        season=season,
        division_num=division_num,
        total_matches=total_matches,
        synced_match_ids=synced,
        skipped_matches=skipped_matches,
        pending_matches=pending_matches,
        fetch_elapsed=fetch_elapsed,
        process_elapsed=process_elapsed,
        total_elapsed=total_elapsed,
    )


async def update_single_match_async(
    match_id: str,
    *,
    validate_avatars: bool = False,
    require_complete_played_maps: bool = False,
    diagnostics: SyncDiagnostics | None = None,
) -> Optional[str]:
    division = _get_division_by_championship_id(match_id)
    if division:
        raise ValueError("update_single_match_async expects a match_id, not a championship_id")

    details = await get_match_details_async(match_id)
    if not details:
        LOGGER.warning("Cannot refresh match %s; details not found", match_id)
        return None

    # Check if match has 'bye' placeholder teams - skip if so
    teams = details.get("teams", {})
    faction1 = teams.get("faction1", {})
    faction2 = teams.get("faction2", {})
    f1_id = faction1.get("faction_id") or faction1.get("team_id")
    f1_name = faction1.get("name")
    f2_id = faction2.get("faction_id") or faction2.get("team_id")
    f2_name = faction2.get("name")
    
    if _is_placeholder_team(f1_id, f1_name) or _is_placeholder_team(f2_id, f2_name):
        LOGGER.info("Skipping match %s with 'bye' placeholder team (f1: %s/%s, f2: %s/%s)",
                   match_id, f1_id, f1_name, f2_id, f2_name)
        return None

    championship_id = details.get("competition_id") or details.get("championship_id")
    if not championship_id:
        raise RuntimeError(f"Match {match_id} lacks competition id")

    division = _get_division_by_championship_id(str(championship_id))
    if not division:
        raise RuntimeError(f"Championship {championship_id} not configured")

    season = division["season"]
    division_num = division["division_num"]
    slug = division.get("slug") or f"div{division_num}-s{season}"
    is_playoffs = bool(division.get("is_playoffs"))

    status_entries = await team_status_service.list_team_statuses(championship_id)
    banned_lookup = {entry["team_id"]: entry for entry in status_entries}
    team_payloads = await _build_championship_team_payloads(
        championship_id,
        status_entries,
        validate_avatars=validate_avatars,
    )

    # Find parent championship if this is a playoff division
    parent_championship_id = await find_parent_championship_id(slug, season, division_num, is_playoffs)

    async with connection() as conn:
        champ_row = {
            "championship_id": championship_id,
            "season": season,
            "division_num": division_num,
            "name": build_division_name(season, division_num, is_playoffs),
            "is_playoffs": 1 if is_playoffs else 0,
            "slug": slug,
            "parent_championship_id": parent_championship_id,
        }
        await upsert_championship_async(conn, champ_row)

        if team_payloads:
            await upsert_teams_bulk_async(team_payloads, conn=conn)
            # Save historical team names for this championship
            team_champ_rows = [
                {
                    "team_id": row["team_id"],
                    "championship_id": championship_id,
                    "team_name": row.get("name"),
                }
                for row in team_payloads
            ]
            await upsert_team_championships_bulk_async(team_champ_rows, conn=conn)

    await sync_match_async(
        championship_id,
        season,
        division_num,
        match_id,
        is_playoffs=is_playoffs,
        banned_lookup=banned_lookup,
        require_complete_played_maps=require_complete_played_maps,
        diagnostics=diagnostics,
    )
    return championship_id
