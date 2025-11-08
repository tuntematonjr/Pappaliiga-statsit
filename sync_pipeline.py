from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from utils import format_hms, log_stage

from division_overrides import combined_status_teams, load_division_overrides
from faceit_client_async import (
    get_championship_matches_async,
    get_map_votes_async,
    get_match_details_async,
    get_match_stats_async,
)

import faceit_config

from db_async import connection, readonly_connection, fetch_all
from db_ops_async import (
    DEFAULT_TEAM_AVATAR,
    clear_obsolete_maps_async,
    delete_stats_for_match_async,
    get_division_snapshot_ts_async,
    get_map_id_lookup_async,
    replace_map_votes_async,
    upsert_championship_async,
    upsert_map_catalog_async,
    upsert_maps_bulk_async,
    upsert_match_async,
    upsert_player_map_season_totals_async,
    upsert_player_season_totals_async,
    upsert_player_stats_bulk_async,
    upsert_players_bulk_async,
    upsert_team_map_season_totals_async,
    upsert_team_season_totals_async,
    upsert_team_stats_bulk_async,
    upsert_teams_bulk_async,
)

LOGGER = logging.getLogger(__name__)

PENDING_REFRESH_INTERVAL_SECONDS = 15 * 60  # avoid re-fetching pending matches more than every 15 minutes

__all__ = [
    "ChampionshipSyncResult",
    "sync_championship_async",
    "sync_match_async",
    "update_single_match_async",
]


@dataclass(slots=True)
class MatchContext:
    championship_id: str
    season: int
    division_num: int
    slug: str
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


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except Exception:
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        s = str(value).replace(",", ".").strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


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


def _derive_team_ids(details: Dict[str, Any], rounds: Sequence[Dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    teams_section = details.get("teams") if isinstance(details, dict) else {}
    faction1 = teams_section.get("faction1") or {}
    faction2 = teams_section.get("faction2") or {}
    f1_name = (faction1.get("name") or "").strip() or None
    f2_name = (faction2.get("name") or "").strip() or None

    seen: List[str] = []
    team1_id: Optional[str] = None
    team2_id: Optional[str] = None

    for rnd in rounds:
        for team in rnd.get("teams", []) or []:
            tid = team.get("team_id") or team.get("id") or team.get("faction_id")
            if tid and tid not in seen:
                seen.append(tid)
            name = (team.get("name") or team.get("team") or "").strip() or None
            if f1_name and name == f1_name and not team1_id:
                team1_id = tid
            if f2_name and name == f2_name and not team2_id:
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

    return team1_id, team2_id


def _normalize_team_ref(ref: Any, team1_id: Optional[str], team2_id: Optional[str]) -> Optional[str]:
    if ref is None:
        return None
    value = str(ref).lower()
    if value in {"faction1", "1", "team1"}:
        return team1_id
    if value in {"faction2", "2", "team2"}:
        return team2_id
    return str(ref)


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
        total_maps = max(faction1, faction2)
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
            for player in team.get("players", []) or []:
                ps = player.get("player_stats") or player.get("stats") or {}
                player_id = player.get("player_id") or player.get("id")
                rows.append(
                    {
                        "match_id": match_id,
                        "round_index": idx,
                        "player_id": player_id,
                        "team_id": tid,
                        "opponent_team_id": None,
                        "nickname": player.get("nickname") or player.get("name"),
                        "kills": safe_int(ps.get("Kills")),
                        "deaths": safe_int(ps.get("Deaths")),
                        "assists": safe_int(ps.get("Assists")),
                        "kd": safe_float(ps.get("K/D Ratio")),
                        "kr": safe_float(ps.get("K/R Ratio")),
                        "adr": safe_float(ps.get("ADR")),
                        "hs_pct": safe_float(ps.get("Headshots %") or ps.get("HS %")),
                        "mvps": safe_int(ps.get("MVPs")),
                        "sniper_kills": safe_int(ps.get("Sniper Kills")),
                        "utility_damage": safe_int(ps.get("Utility Damage")),
                        "enemies_flashed": safe_int(ps.get("Enemies Flashed")),
                        "flash_count": safe_int(ps.get("Flash Count") or ps.get("Flashbangs Thrown")),
                        "flash_successes": safe_int(ps.get("Flash Successes") or ps.get("Successful Flashes")),
                        "mk_2k": safe_int(ps.get("Double Kills")),
                        "mk_3k": safe_int(ps.get("Triple Kills")),
                        "mk_4k": safe_int(ps.get("Quadro Kills")),
                        "mk_5k": safe_int(ps.get("Penta Kills")),
                        "clutch_kills": safe_int(ps.get("Clutch Kills")),
                        "cl_1v1_attempts": safe_int(ps.get("1v1Count") or ps.get("1v1 Attempts")),
                        "cl_1v1_wins": safe_int(ps.get("1v1Wins") or ps.get("1v1 Wins")),
                        "cl_1v2_attempts": safe_int(ps.get("1v2Count") or ps.get("1v2 Attempts")),
                        "cl_1v2_wins": safe_int(ps.get("1v2Wins") or ps.get("1v2 Wins")),
                        "entry_count": safe_int(ps.get("Entry Count") or ps.get("Entry Duels")),
                        "entry_wins": safe_int(ps.get("Entry Wins")),
                        "pistol_kills": safe_int(ps.get("Pistol Kills")),
                        "damage": safe_int(ps.get("Damage")),
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
            players.append({"player_id": pid, "nickname": nickname})
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
        out.append(
            {
                "team_id": team_id,
                "name": faction.get("name") or info.get("team_name"),
                "avatar": faction.get("avatar") or info.get("avatar") or DEFAULT_TEAM_AVATAR,
            }
        )
    return out


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

    rounds = _extract_rounds(stats)
    team1_id, team2_id = _derive_team_ids(details, rounds)

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

    finish_ts = safe_int(details.get("finished_at"))
    winner_team_id = None
    res = (details or {}).get("results") or {}
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
    slug: str,
    is_playoffs: bool,
    banned_lookup: Dict[str, Dict[str, Any]],
) -> NormalisedMatch:
    overall_start = time.perf_counter()
    fetch_start = time.perf_counter()
    LOGGER.debug("Starting sync for match %s", match_id)
    ctx = MatchContext(
        championship_id=championship_id,
        season=season,
        division_num=division_num,
        slug=slug,
        is_playoffs=is_playoffs,
        banned_team_ids=set(banned_lookup.keys()),
        banned_lookup=banned_lookup,
    )

    details = await get_match_details_async(match_id)
    if not details:
        raise RuntimeError(f"Match {match_id} details missing")

    status = str(details.get("status") or "").lower()
    finish_ts = safe_int(details.get("finished_at"))
    has_played = bool(finish_ts) or status in {"finished", "closed", "over", "completed"}

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

    forfeit_lookup = {
        int(row["round_index"]): bool(row.get("is_forfeit"))
        for row in normalised.map_rows
        if row.get("round_index") is not None
    }

    persist_start = time.perf_counter()

    # FK safety check: Verify championship exists
    # If missing, try to find an existing championship with matching (season, div, playoffs) and use that ID
    match_championship_id = normalised.match_row.get("championship_id")
    
    # Use a single transaction for everything
    async with connection() as conn:
        # Check if championship exists
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT championship_id FROM championships WHERE championship_id = %s",
                (match_championship_id,)
            )
            champ_exists = await cur.fetchone()
        
        if not champ_exists:
            # Championship missing! Try to find an existing championship with same (season, div, playoffs)
            LOGGER.warning(
                "Championship %s MISSING for match %s! Looking for alternative championship with S%d D%d playoffs=%d",
                match_championship_id,
                match_id,
                ctx.season,
                ctx.division_num,
                1 if ctx.is_playoffs else 0,
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT championship_id FROM championships WHERE season = %s AND division_num = %s AND is_playoffs = %s LIMIT 1",
                    (ctx.season, ctx.division_num, 1 if ctx.is_playoffs else 0)
                )
                alternative = await cur.fetchone()
            
            if alternative:
                alternative_id = alternative[0]
                LOGGER.info("Found alternative championship %s - remapping match %s", alternative_id, match_id)
                # Update the match row to use the alternative championship ID
                normalised.match_row["championship_id"] = alternative_id
            else:
                LOGGER.error("No alternative championship found for S%d D%d playoffs=%d - match %s will fail FK constraint!",
                           ctx.season, ctx.division_num, 1 if ctx.is_playoffs else 0, match_id)
                # Let it fail with FK error so we can debug
        
        # Ensure teams and players exist first to satisfy FK constraints from matches/maps/stats
        if normalised.team_rows:
            await upsert_teams_bulk_async(conn, normalised.team_rows)
        if normalised.player_rows:
            await upsert_players_bulk_async(conn, normalised.player_rows)

        # Now insert/update the match which references teams
        await upsert_match_async(conn, normalised.match_row)

        if normalised.map_rows:
            await upsert_maps_bulk_async(
                conn,
                normalised.match_row["match_id"],
                ctx.season,
                ctx.division_num,
                normalised.map_rows,
            )

        map_lookup = await get_map_id_lookup_async(conn, match_id)
        await replace_map_votes_async(conn, match_id, ctx.season, ctx.division_num, normalised.map_votes)

        snapshot_ts = await get_division_snapshot_ts_async(conn, ctx.season, ctx.division_num)

        await delete_stats_for_match_async(conn, match_id, snapshot_ts=snapshot_ts)

        await upsert_player_stats_bulk_async(
            conn,
            ctx.season,
            ctx.division_num,
            match_id,
            map_lookup,
            normalised.player_stats,
            forfeit_lookup,
        )

        await upsert_team_stats_bulk_async(
            conn,
            ctx.season,
            ctx.division_num,
            match_id,
            map_lookup,
            normalised.team_stats,
            forfeit_lookup,
        )

        await clear_obsolete_maps_async(
            conn,
            match_id,
            [row["round_index"] for row in normalised.map_rows],
        )

        affected_team_ids: Set[str] = {
            str(tid)
            for tid in normalised.affected_teams
            if tid
        }
        team_map_updates: dict[str, set[str]] = defaultdict(set)
        if affected_team_ids:
            for map_row in normalised.map_rows:
                raw_map = (map_row.get("map_name") or "").strip()
                if raw_map:
                    map_key = raw_map
                else:
                    round_index = map_row.get("round_index")
                    map_key = f"map_{round_index}" if round_index is not None else "unknown"
                for tid in affected_team_ids:
                    team_map_updates[tid].add(map_key)

        for team_id in affected_team_ids:
            await upsert_team_season_totals_async(
                conn,
                ctx.season,
                ctx.division_num,
                team_id,
                snapshot_ts=snapshot_ts,
            )
            for map_key in team_map_updates.get(team_id, ()):
                await upsert_team_map_season_totals_async(
                    conn,
                    ctx.season,
                    ctx.division_num,
                    team_id,
                    map_name=map_key,
                    snapshot_ts=snapshot_ts,
                )

        for player_id in normalised.affected_players:
            await upsert_player_season_totals_async(
                conn,
                ctx.season,
                ctx.division_num,
                player_id,
                snapshot_ts=snapshot_ts,
            )
            await upsert_player_map_season_totals_async(
                conn,
                ctx.season,
                ctx.division_num,
                player_id,
                snapshot_ts=snapshot_ts,
            )

        # Upsert map images provided by Faceit into maps_catalog so html_gen can use Faceit assets
        try:
            voting = (details or {}).get("voting") or {}
            map_section = voting.get("map") if isinstance(voting, dict) else None
            entities = map_section.get("entities") if isinstance(map_section, dict) else None
            if isinstance(entities, list):
                for ent in entities:
                    if not isinstance(ent, dict):
                        continue
                    map_id = ent.get("guid") or ent.get("game_map_id") or ent.get("class_name")
                    pretty = ent.get("name") or map_id
                    image_sm = ent.get("image_sm") or ent.get("image") or ""
                    image_lg = ent.get("image_lg") or image_sm or ""
                    if map_id:
                        try:
                            await upsert_map_catalog_async(
                                conn,
                                {
                                    "map_id": map_id,
                                    "pretty_name": pretty,
                                    "image_sm": image_sm,
                                    "image_lg": image_lg,
                                },
                            )
                        except Exception:
                            LOGGER.debug("Failed to upsert maps_catalog entry for %s", map_id, exc_info=True)
        except Exception:
            LOGGER.debug("Failed to process Faceit map images for match %s", match_id, exc_info=True)

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
    LOGGER.info("Match %s synced in %s", match_id, format_hms(total_elapsed))
    return normalised


async def sync_championship_async(
    championship_id: str,
    *,
    division: Mapping[str, Any] | None = None,
    full: bool = False,
    force_matches: bool = False,
    overrides: Mapping[str, dict[str, List[dict[str, str]]]] | None = None,
    end_on_error: bool = False,
) -> ChampionshipSyncResult:
    start_time = time.perf_counter()
    force_all_matches = full or force_matches
    division_info = division or next((d for d in faceit_config.DIVISIONS if d["championship_id"] == championship_id), None)
    if not division_info:
        raise ValueError(f"Championship {championship_id} not found in DIVISIONS")

    season = division_info["season"]
    division_num = division_info["division_num"]
    division_name = division_info.get("name", f"Division {division_num}")
    
    LOGGER.info(
        "Starting sync for championship %s (%s, Season %d, Division %d)",
        championship_id,
        division_name,
        season,
        division_num,
    )
    slug = division_info.get("slug") or f"div{division_num}-s{season}"
    is_playoffs = bool(division_info.get("is_playoffs"))

    override_source = overrides if overrides is not None else load_division_overrides()
    status_entries = combined_status_teams(championship_id, override_source)
    banned_lookup = {entry["team_id"]: entry for entry in status_entries}

    # Find parent championship if this is a playoff division
    parent_championship_id = await find_parent_championship_id(slug, season, division_num, is_playoffs)

    async with connection() as conn:
        await upsert_championship_async(
            conn,
            {
                "championship_id": championship_id,
                "season": season,
                "division_num": division_num,
                "name": division_info.get("name") or slug,
                "is_playoffs": 1 if is_playoffs else 0,
                "slug": slug,
                "parent_championship_id": parent_championship_id,
            },
        )
        team_payloads = [
            {
                "team_id": entry["team_id"],
                "name": entry.get("team_name"),
                "avatar": entry.get("avatar") or DEFAULT_TEAM_AVATAR,
            }
            for entry in status_entries
            if entry.get("team_id")
        ]
        if team_payloads:
            await upsert_teams_bulk_async(conn, team_payloads)

    fetch_start_time = time.perf_counter()
    matches = await get_championship_matches_async(championship_id, match_type="all")
    fetch_elapsed = time.perf_counter() - fetch_start_time
    match_ids = [item.get("match_id") for item in matches if item.get("match_id")]
    total_matches = len(match_ids)

    existing_rows = await fetch_all(
        """
        SELECT match_id, finished_at, status, last_seen_at
        FROM matches
        WHERE championship_id = %s
        """,
        (championship_id,),
    )
    existing_lookup = {str(row["match_id"]): row for row in existing_rows if row.get("match_id")}
    LOGGER.info("Found %d existing matches in database", len(existing_lookup))
    if existing_lookup:
        # Log first few for debugging
        sample_keys = list(existing_lookup.keys())[:3]
        for key in sample_keys:
            row = existing_lookup[key]
            LOGGER.info("Sample existing match: %s, finished_at=%s, status='%s'", 
                       key, row.get("finished_at"), row.get("status"))

    synced: List[str] = []
    skipped_matches = 0
    pending_matches = 0
    deferred_pending = 0
    LOGGER.info(
        "Processing %d matches, force_all_matches=%s, existing_lookup has %d entries",
        total_matches,
        force_all_matches,
        len(existing_lookup),
    )
    
    process_start_time = time.perf_counter()
    for match_id in match_ids:
        if not match_id:
            continue
        LOGGER.info("Processing match %s", match_id)
        if not force_all_matches:
            existing = existing_lookup.get(str(match_id))
            if existing:
                existing_finished = existing.get("finished_at")
                existing_status = str(existing.get("status") or "").lower()
                LOGGER.info("Match %s: existing_finished=%s, existing_status='%s'", 
                           match_id, existing_finished, existing_status)
                
                # Only skip if match is fully played/finished
                if existing_finished or existing_status in {"finished", "closed", "over", "completed", "cancelled"}:
                    LOGGER.info("Skipping already played match %s (finished_at=%s, status=%s)", 
                               match_id, existing_finished, existing_status)
                    skipped_matches += 1
                    continue
                else:
                    last_seen_at = safe_int(existing.get("last_seen_at"))
                    if last_seen_at:
                        age_seconds = time.time() - last_seen_at
                        if age_seconds < PENDING_REFRESH_INTERVAL_SECONDS:
                            deferred_pending += 1
                            pending_matches += 1
                            LOGGER.info(
                                "Deferring refresh for pending match %s; last seen %s ago (< %s)",
                                match_id,
                                format_hms(age_seconds),
                                format_hms(PENDING_REFRESH_INTERVAL_SECONDS),
                            )
                            continue

                    LOGGER.info("Match %s exists but not finished - will update basic info only", match_id)
                    pending_matches += 1
            else:
                LOGGER.info("Match %s: not found in existing_lookup, will sync", match_id)
        try:
            await sync_match_async(
                championship_id,
                season,
                division_num,
                match_id,
                slug=slug,
                is_playoffs=is_playoffs,
                banned_lookup=banned_lookup,
            )
            synced.append(match_id)
        except Exception as exc:  # pragma: no cover - logged for visibility
            LOGGER.exception("Failed to sync match %s: %s", match_id, exc)
            if end_on_error:
                raise
    
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


async def update_single_match_async(match_id: str) -> Optional[str]:
    division = next((d for d in faceit_config.DIVISIONS if d.get("championship_id") == match_id), None)
    if division:
        raise ValueError("update_single_match_async expects a match_id, not a championship_id")

    details = await get_match_details_async(match_id)
    if not details:
        LOGGER.warning("Cannot refresh match %s; details not found", match_id)
        return None

    championship_id = details.get("competition_id") or details.get("championship_id")
    if not championship_id:
        raise RuntimeError(f"Match {match_id} lacks competition id")

    division = next((d for d in faceit_config.DIVISIONS if d["championship_id"] == championship_id), None)
    if not division:
        raise RuntimeError(f"Championship {championship_id} not configured")

    season = division["season"]
    division_num = division["division_num"]
    slug = division.get("slug") or f"div{division_num}-s{season}"
    is_playoffs = bool(division.get("is_playoffs"))

    overrides = load_division_overrides()
    status_entries = combined_status_teams(championship_id, overrides)
    banned_lookup = {entry["team_id"]: entry for entry in status_entries}

    # Find parent championship if this is a playoff division
    parent_championship_id = await find_parent_championship_id(slug, season, division_num, is_playoffs)

    async with connection() as conn:
        await upsert_championship_async(
            conn,
            {
                "championship_id": championship_id,
                "season": season,
                "division_num": division_num,
                "name": division.get("name") or slug,
                "is_playoffs": 1 if is_playoffs else 0,
                "slug": slug,
                "parent_championship_id": parent_championship_id,
            },
        )
        team_payloads = [
            {
                "team_id": entry["team_id"],
                "name": entry.get("team_name"),
                "avatar": entry.get("avatar") or DEFAULT_TEAM_AVATAR,
            }
            for entry in status_entries
            if entry.get("team_id")
        ]
        if team_payloads:
            await upsert_teams_bulk_async(conn, team_payloads)

    await sync_match_async(
        championship_id,
        season,
        division_num,
        match_id,
        slug=slug,
        is_playoffs=is_playoffs,
        banned_lookup=banned_lookup,
    )
    return championship_id
