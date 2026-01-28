# sync.py
# Championship-centric sync for Pappaliiga (CS2).
# - Reads divisions from faceit_config.DIVISIONS (JSON-backed)
# - Upserts championships
# - Fetches matches (+ details), map veto history, and (best-effort) per-map/team/player stats
# All comments in English by design.

from __future__ import annotations
import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import sqlite3
import logging
from logging.handlers import RotatingFileHandler

from faceit_config import DIVISIONS, CURRENT_SEASON
from faceit_client import (
    list_championship_matches, get_match_details, get_match_stats, get_democracy_history
)
from db import (
    get_conn, init_db,
    upsert_championship, upsert_match,
    upsert_team,
    upsert_maps, upsert_map_votes,
    upsert_player_stats,
    upsert_map_catalog, add_map_to_season_pool,
    upsert_players_bulk,
)

_SCORE_RE = re.compile(r"^\s*(\d+)\s*[/\:]\s*(\d+)\s*$")

# Configure logging with rotation (max 5 MB per file, keep 3 backups)
logFile = "sync.log"
handler = RotatingFileHandler(logFile, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,  # was DEBUG
    handlers=[handler],
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

# ---- helpers ---------------------------------------------------------------

def safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return default

def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        # Faceit may occasionally return "1,23"; normalize it to use a dot
        s = str(v).replace(",", ".")
        return float(s)
    except Exception:
        return default

def _is_bye_id(x: Optional[str]) -> bool:
    return str(x or "").lower() == "bye"

def _is_bye_match_summary(m: dict) -> bool:
    return _is_bye_id(m.get("team1_id")) or _is_bye_id(m.get("team2_id"))

def _is_bye_match_details(details: dict) -> bool:
    teams = (details or {}).get("teams") or {}
    f1 = teams.get("faction1") or {}
    f2 = teams.get("faction2") or {}
    return _is_bye_id(f1.get("faction_id")) or _is_bye_id(f2.get("faction_id"))

def _map_tickets_from_democracy(demo_json: dict) -> list[dict]:
    payload = demo_json.get("payload") if isinstance(demo_json, dict) else None
    tickets = payload.get("tickets", []) if isinstance(payload, dict) else []
    return [tk for tk in tickets
            if isinstance(tk, dict) and str(tk.get("entity_type") or "").lower() == "map"]

# --- progress bar with ETA --------------------------------------------------

def _fmt_hms(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:d}:{s:02d}"

def _progress_bar(prefix: str, i: int, total: int, start_ts: float, skipped: int = 0, width: int = 32) -> None:
    """
    In-place progress bar with ETA and skipped counter.
      Example:
        Div1 — All [########------------] 12/100 (12%) | skipped 5 | elapsed 0:25 | ETA 2:56
    """
    i = max(0, min(i, total))
    pct = 0 if total <= 0 else int(100 * i / total)
    fill = 0 if total <= 0 else int(width * i / total)
    bar = "#" * fill + "-" * (width - fill)

    elapsed = max(0.0, time.time() - start_ts)
    rate = (i / elapsed) if elapsed > 0 else 0.0
    remaining = ((total - i) / rate) if rate > 0 else 0.0
    msg = (
        f"{prefix} [{bar}] {i}/{total} ({pct}%)"
        f" | skipped {skipped}"
        f" | elapsed {_fmt_hms(elapsed)}"
        f" | ETA {_fmt_hms(remaining)}"
    )
    print("\r" + msg, end="", file=sys.stdout, flush=True)
    if i >= total:
        print("", file=sys.stdout, flush=True)  # newline at end

# ---- transformers for stats payload ---------------------------------------

def _persist_map_catalog_from_details(con: sqlite3.Connection, details: dict, season: int) -> None:
    voting = (details or {}).get("voting") or {}
    msec = voting.get("map") or {}
    entities = msec.get("entities") or []

    def _pretty_for(ent: dict, map_id: str) -> str:
        raw = (ent.get("name") or "").strip()
        mid = (map_id or "").lower()
        if raw.lower() == "dust2" or "dust2" in mid:
            return "Dust II"
        if raw:
            return raw
        slug = (map_id or "").replace("de_", "").replace("_", " ").strip()
        return slug.title() if slug else map_id

    for ent in entities:
        map_id = ent.get("class_name") or ent.get("game_map_id") or ent.get("guid") or ""
        if not map_id:
            continue
        pretty = _pretty_for(ent, map_id)
        img_sm = ent.get("image_sm") or ""
        img_lg = ent.get("image_lg") or ""

        row = {
            "map_id": map_id.lower(),
            "pretty_name": pretty,
            "image_sm": img_sm,
            "image_lg": img_lg,
        }
        upsert_map_catalog(con, row)
        add_map_to_season_pool(con, season, row["map_id"])

def _extract_player_rows(match_id: str, rounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, r in enumerate(rounds, start=1):
        for t in (r.get("teams") or []):
            tid = t.get("team_id") or t.get("id") or t.get("faction_id")
            tname = (t.get("team_stats") or {}).get("Team") or t.get("name") or t.get("team")
            for p in (t.get("players") or []):
                ps = p.get("player_stats") or p.get("stats") or {}

                # Multikill mapping (Faceit keys -> our columns)
                mk_2k = safe_int(ps.get("Double Kills"), 0)
                mk_3k = safe_int(ps.get("Triple Kills"), 0)
                mk_4k = safe_int(ps.get("Quadro Kills"), 0)
                mk_5k = safe_int(ps.get("Penta Kills"), 0)

                rows.append({
                    "match_id": match_id,
                    "round_index": idx,
                    "player_id": p.get("player_id") or p.get("id"),
                    "nickname": p.get("nickname") or p.get("name"),
                    "team_id": tid,
                    "kills": safe_int(ps.get("Kills"), 0),
                    "deaths": safe_int(ps.get("Deaths"), 0),
                    "assists": safe_int(ps.get("Assists"), 0),
                    "kd": safe_float(ps.get("K/D Ratio"), 0.0),
                    "kr": safe_float(ps.get("K/R Ratio"), 0.0),
                    "adr": safe_float(ps.get("ADR"), 0.0),
                    "hs_pct": safe_float(ps.get("Headshots %") or ps.get("HS %"), 0.0),
                    "mvps": safe_int(ps.get("MVPs"), 0),
                    "sniper_kills": safe_int(ps.get("Sniper Kills"), 0),
                    "utility_damage": safe_int(ps.get("Utility Damage"), 0),
                    "enemies_flashed": safe_int(ps.get("Enemies Flashed"), 0),
                    "flash_count": safe_int(ps.get("Flash Count") or ps.get("Flashbangs Thrown"), 0),
                    "flash_successes": safe_int(ps.get("Flash Successes") or ps.get("Successful Flashes"), 0),
                    "mk_2k": mk_2k,
                    "mk_3k": mk_3k,
                    "mk_4k": mk_4k,
                    "mk_5k": mk_5k,
                    "clutch_kills": safe_int(ps.get("Clutch Kills"), 0),
                    "cl_1v1_attempts": safe_int(ps.get("1v1Count") or ps.get("1v1 Attempts"), 0),
                    "cl_1v1_wins": safe_int(ps.get("1v1Wins") or ps.get("1v1 Wins"), 0),
                    "cl_1v2_attempts": safe_int(ps.get("1v2Count") or ps.get("1v2 Attempts"), 0),
                    "cl_1v2_wins": safe_int(ps.get("1v2Wins") or ps.get("1v2 Wins"), 0),
                    "entry_count": safe_int(ps.get("Entry Count") or ps.get("Entry Duels"), 0),
                    "entry_wins": safe_int(ps.get("Entry Wins"), 0),
                    "pistol_kills": safe_int(ps.get("Pistol Kills"), 0),
                    "damage": safe_int(ps.get("Damage"), 0),
                })
    return rows


def _extract_map_rows_from_stats(match_id: str, rounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, r in enumerate(rounds, start=1):
        rs = r.get("round_stats") or {}
        name = rs.get("Map") or r.get("map") or r.get("map_name") or None

        s1 = s2 = None
        score = (rs.get("Score") or rs.get("score") or "").strip()
        if score:
            m = _SCORE_RE.match(score)
            if m:
                s1 = safe_int(m.group(1), None)
                s2 = safe_int(m.group(2), None)

        rows.append({
            "match_id": match_id,
            "round_index": idx,
            "map_name": name,
            "score_team1": s1,
            "score_team2": s2,
            "winner_team_id": rs.get("Winner") or rs.get("winner"),  # normalized later
        })
    return rows

def _extract_map_rows_from_details(match_id: str, details: Dict[str, Any],
                                   team1_id: Optional[str], team2_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build placeholder map rows for forfeits when round stats are missing.
    We convert Faceit `detailed_results` 1–0 / 0–1 map wins into 13–0 / 0–13,
    and always set map_name to 'forfeit' so downstream aggregations can skip them.
    If only `results.score` exists (e.g., 2–0), create that many 'forfeit' maps.
    """
    rows: List[Dict[str, Any]] = []

    # Case A: detailed_results (preferred)
    det = (details or {}).get("detailed_results")
    if isinstance(det, list) and det:
        for idx, item in enumerate(det, start=1):
            factions = item.get("factions") or {}
            s1 = safe_int((factions.get("faction1") or {}).get("score"))
            s2 = safe_int((factions.get("faction2") or {}).get("score"))
            w_raw = item.get("winner")

            # Normalize 1–0 to 13–0
            if s1 is not None and s2 is not None:
                if {s1, s2} == {0, 1}:
                    s1, s2 = (13, 0) if s1 == 1 else (0, 13)

            rows.append({
                "match_id": match_id,
                "round_index": idx,
                "map_name": "forfeit",
                "score_team1": s1,
                "score_team2": s2,
                "winner_team_id": _normalize_team_ref(w_raw, team1_id, team2_id),
            })
        return rows

    # Case B: only results.score (e.g., 2–0)
    res = (details or {}).get("results") or {}
    score = res.get("score") or {}
    m1 = safe_int(score.get("faction1"), None)
    m2 = safe_int(score.get("faction2"), None)
    if m1 is not None and m2 is not None:
        best = max(m1, m2)
        if best > 0:
            w_raw = res.get("winner") or res.get("winner_team_id")
            w_team = _normalize_team_ref(w_raw, team1_id, team2_id)
            # Direction 13–0 for the winner across all maps
            s1, s2 = (13, 0) if (w_team == team1_id) else (0, 13)
            for idx in range(1, best + 1):
                rows.append({
                    "match_id": match_id,
                    "round_index": idx,
                    "map_name": "forfeit",
                    "score_team1": s1,
                    "score_team2": s2,
                    "winner_team_id": w_team,
                })
    return rows

def _extract_rounds_from_stats(stats_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Accept non-dict safely; return [] if shape not recognized."""
    if not isinstance(stats_json, dict):
        return []
    rounds = stats_json.get("rounds") or stats_json.get("roundsStats") or []
    return rounds if isinstance(rounds, list) else []

def _derive_team_ids(details: Dict[str, Any], rounds: List[Dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    """
    Return (team1_id, team2_id) by checking, in order:
      1) rounds[*].teams[*].team_id when stats are present
      2) a name match against the team names in the rounds payload
      3) FALLBACK: details.teams.faction*.faction_id to work without stats
    """
    f1_name = ((details.get("teams") or {}).get("faction1") or {}).get("name")
    f2_name = ((details.get("teams") or {}).get("faction2") or {}).get("name")

    seen_ids: list[str] = []
    t1_id = None
    t2_id = None

    for r in rounds or []:
        for t in (r.get("teams") or []):
            tid = t.get("team_id") or t.get("id") or t.get("faction_id")
            if tid and tid not in seen_ids:
                seen_ids.append(tid)
            tname = t.get("name") or t.get("team")
            if f1_name and tname and tname == f1_name and not t1_id:
                t1_id = tid
            if f2_name and tname and tname == f2_name and not t2_id:
                t2_id = tid

    # If the name match failed but there are two teams in the rounds payload
    if (t1_id is None or t2_id is None) and len(seen_ids) >= 2:
        if t1_id is None:
            t1_id = seen_ids[0]
        if t2_id is None:
            t2_id = next((x for x in seen_ids if x != t1_id), seen_ids[1])

    # Final fallback: pull details.teams.faction*.faction_id directly when stats are missing
    if t1_id is None or t2_id is None:
        t1_fid = (((details.get("teams") or {}).get("faction1") or {}).get("faction_id")) or None
        t2_fid = (((details.get("teams") or {}).get("faction2") or {}).get("faction_id")) or None
        if t1_id is None and t1_fid:
            t1_id = t1_fid
        if t2_id is None and t2_fid:
            t2_id = t2_fid

    return t1_id, t2_id

def _normalize_team_ref(ref: Any, team1_id: Optional[str], team2_id: Optional[str]) -> Optional[str]:
    """
    Convert 'faction1'/'faction2'/'1'/'2'/'team1'/'team2' into the real team_id.
    If ref is already an ID, return it unchanged.
    """
    if ref is None:
        return None
    s = str(ref).lower()
    if s in ("faction1", "1", "team1"):
        return team1_id
    if s in ("faction2", "2", "team2"):
        return team2_id
    return str(ref)

# Skip matches already finished in the database (saves API quota)?
# Can be overridden with --force flag
SKIP_FINISHED_IN_DB = True  

def _db_match_snapshot(con: sqlite3.Connection, match_id: str) -> dict:
    """
    Single snapshot for skip logic with fewer roundtrips:
    - 1x SELECT from matches
    - 1x SELECT over maps to get both "has_any_map" and "has_forfeit_map"
    - 1x EXISTS for player_stats
    """
    row = con.execute(
        """SELECT status, scheduled_at, started_at, finished_at, team1_id, team2_id
           FROM matches WHERE match_id=?""",
        (match_id,)
    ).fetchone()
    exists = bool(row)
    status = (row["status"] or "").lower() if row else None
    sched  = row["scheduled_at"] if row else None
    start  = row["started_at"]   if row else None
    finish = row["finished_at"]  if row else None
    t1     = row["team1_id"]     if row else None
    t2     = row["team2_id"]     if row else None

    maps_row = con.execute(
        "SELECT COUNT(*) AS c, MAX(CASE WHEN map_name='forfeit' THEN 1 ELSE 0 END) AS has_ff "
        "FROM maps WHERE match_id=?",
        (match_id,)
    ).fetchone()
    has_any_map = (maps_row["c"] or 0) > 0
    has_ff_map  = bool(maps_row["has_ff"])

    has_ps = bool(con.execute(
        "SELECT 1 FROM player_stats WHERE match_id=? LIMIT 1", (match_id,)
    ).fetchone())

    return {
        "exists": exists,
        "status": status, "scheduled_at": sched, "started_at": start, "finished_at": finish,
        "team1_id": t1, "team2_id": t2,
        "has_any_map": has_any_map, "has_player_stats": has_ps, "has_forfeit_map": has_ff_map,
    }


def _target_kind_from_status(item: dict) -> str:
    """
    Map Faceit status values into handling buckets.
      - 'finished' / 'closed' / 'played' → 'past' (fetch stats)
      - everything else ('ongoing', 'live', 'upcoming', 'scheduled', etc.) → 'upcoming'
    """
    st = str(item.get("status") or "").lower()
    past_statuses = {"finished", "closed", "played"}
    return "past" if st in past_statuses else "upcoming"

def _list_matches_all(championship_id: str) -> list[dict]:
    """
    Fetch every match in one call (type=all), set _target_kind, and pull the core fields.
    """
    items = list_championship_matches(championship_id, match_type="all") or []
    out: list[dict] = []
    for it in items:
        teams = it.get("teams") or {}
        f1 = teams.get("faction1") or {}
        f2 = teams.get("faction2") or {}
        out.append({
            "_raw": it,  # keep the raw payload in case we need it later
            "_target_kind": _target_kind_from_status(it),
            "match_id": it.get("match_id") or it.get("id"),
            "status": (it.get("status") or "").lower(),
            "scheduled_at": safe_int(it.get("scheduled_at")),
            "started_at": safe_int(it.get("started_at")),
            "finished_at": safe_int(it.get("finished_at")),
            "team1_id": f1.get("faction_id"),
            "team1_name": f1.get("name"),
            "team2_id": f2.get("faction_id"),
            "team2_name": f2.get("name"),
            "team1_avatar": f1.get("avatar"),
            "team2_avatar": f2.get("avatar"),
            "team1_roster": f1.get("roster") or [],
            "team2_roster": f2.get("roster") or [],
        })
    return out

def _sync_division_one_pass(con: sqlite3.Connection, champ_row: dict, force: bool = False) -> None:
    """
    One pass over all matches (type=all). Ongoing are handled like scheduled.
    Optimized to:
      - Use SAVEPOINT/RELEASE per match, commit once per division (reduces fsyncs)
      - Throttle progress bar updates (<=1 Hz) to cut stdout overhead
      - Use single DB snapshot query for skip logic
    """
    fetch_id = champ_row.get("_fetch_championship_id") or champ_row["championship_id"]
    matches = _list_matches_all(fetch_id)
    div_title = champ_row.get("name") or champ_row.get("slug") or f"Div{champ_row.get('division_num','?')}-S{champ_row.get('season','?')}"
    title = f"{div_title} — All"
    total = len(matches)
    if total == 0:
        _progress_bar(title, 0, 0, time.time(), skipped=0)
        return

    seen: set[str] = set()
    skipped = 0
    start_ts = time.time()
    last_print = 0.0  # throttle progress updates

    for i, m in enumerate(matches, start=1):
        mid = m.get("match_id")
        if not mid or mid in seen:
            # Throttled progress update
            if (i == total) or (time.time() - last_print > 1.0):
                _progress_bar(title, i, total, start_ts, skipped)
                last_print = time.time()
            continue

        # Early skip: BYE
        if _is_bye_match_summary(m):
            logging.info("[skip] bye match %s (%s vs %s)", mid, m.get("team1_name"), m.get("team2_name"))
            seen.add(mid); skipped += 1
            if (i == total) or (time.time() - last_print > 1.0):
                _progress_bar(title, i, total, start_ts, skipped)
                last_print = time.time()
            continue

        # Single DB snapshot for all skip checks
        snap = _db_match_snapshot(con, mid)

        # Skip finished+complete (maps+either player_stats or a 'forfeit' map)
        if not force and SKIP_FINISHED_IN_DB and (snap["status"] in {"finished", "played", "closed"}) and (
            snap["has_player_stats"] or (snap["has_any_map"] and snap["has_forfeit_map"])
        ):
            seen.add(mid); skipped += 1
            if (i == total) or (time.time() - last_print > 1.0):
                _progress_bar(title, i, total, start_ts, skipped)
                last_print = time.time()
            continue

        # Non-past summary unchanged vs DB header → skip
        tgt = m.get("_target_kind") or "upcoming"
        if tgt != "past" and snap["exists"] and not force:
            unchanged = (
                (snap["status"] or "") == (m.get("status") or "").lower() and
                (snap["scheduled_at"] or None) == (m.get("scheduled_at") or None) and
                (snap["started_at"]   or None) == (m.get("started_at")   or None) and
                (snap["finished_at"]  or None) == (m.get("finished_at")  or None) and
                (snap["team1_id"]     or None) == (m.get("team1_id")     or None) and
                (snap["team2_id"]     or None) == (m.get("team2_id")     or None)
            )
            if unchanged:
                seen.add(mid); skipped += 1
                if (i == total) or (time.time() - last_print > 1.0):
                    _progress_bar(title, i, total, start_ts, skipped)
                    last_print = time.time()
                continue

        # Persist with per-match SAVEPOINT; commit will be done once per division
        seen.add(mid)
        try:
            con.execute("SAVEPOINT match_tx")
            summary = m if tgt != "past" else None
            persist_match(con, champ_row, mid, kind=tgt, summary=summary)
            con.execute("RELEASE SAVEPOINT match_tx")
        except Exception as e:
            logging.warning("sync (all) %s failed: %s", mid, e)
            try:
                con.execute("ROLLBACK TO SAVEPOINT match_tx")
            except Exception:
                pass  # ignore nested rollback errors

        # Throttled progress update
        if (i == total) or (time.time() - last_print > 1.0):
            _progress_bar(title, i, total, start_ts, skipped)
            last_print = time.time()

    # Single commit per division pass
    try:
        con.commit()
    except Exception as e:
        logging.warning("division commit failed: %s", e)

def persist_match(con: sqlite3.Connection, champ_row: Dict[str, Any], match_id: str, kind: str, summary: Optional[Dict[str, Any]] = None) -> None:
    details: Dict[str, Any] = {}
    f1: Dict[str, Any] = {}
    f2: Dict[str, Any] = {}

    if kind != "past" and isinstance(summary, dict) and _is_bye_match_summary(summary):
        logging.info("[skip] bye (summary) %s", match_id)
        return

    if kind != "past" and isinstance(summary, dict):
        details = summary.get("_raw") or {}
        f1 = {"name": summary.get("team1_name"), "avatar": summary.get("team1_avatar"), "roster": summary.get("team1_roster") or []}
        f2 = {"name": summary.get("team2_name"), "avatar": summary.get("team2_avatar"), "roster": summary.get("team2_roster") or []}
    else:
        details = get_match_details(match_id) or {}
        if _is_bye_match_details(details):
            logging.info("[skip] bye (details) %s", match_id)
            return
        teams_d = details.get("teams") or {}
        f1 = teams_d.get("faction1") or {}
        f2 = teams_d.get("faction2") or {}
        _persist_map_catalog_from_details(con, details, season=champ_row["season"])

    # STATS
    stats = {}
    rounds = []
    forfeit_like = False
    if kind == "past":
        try:
            stats = get_match_stats(match_id) or {}
        except Exception as e:
            logging.info("[skip] stats %s -> %s", match_id, e)
            stats = {}
        rounds = _extract_rounds_from_stats(stats)

    has_detailed = isinstance(details.get("detailed_results"), list) and len(details.get("detailed_results") or []) > 0
    has_score    = bool(((details.get("results") or {}).get("score") or {}))
    forfeit_like = (kind == "past" and not rounds and (has_detailed or has_score))

    demo_json = {}
    if kind == "past" and not forfeit_like:
        try:
            demo_json = get_democracy_history(match_id) or {}
        except Exception:
            demo_json = {}

    if kind == "past" and not rounds and not (has_detailed or has_score):
        upsert_match(con, {
            "match_id": match_id,
            "championship_id": champ_row["championship_id"],
            "status": "not_found",
            "is_forfeit": 0,
        })
        return

    # Team IDs (fallback to details.teams.* if needed)
    team1_id, team2_id = _derive_team_ids(details or {}, rounds)

    # Winner normalization
    winner_raw = None
    try:
        res = (details.get("results") or {})
        winner_raw = res.get("winner") or res.get("winner_team_id")
    except Exception:
        pass
    winner_team_id = _normalize_team_ref(winner_raw, team1_id, team2_id)

    # Upsert teams (names & avatars live only in teams)
    if team1_id or (summary and summary.get("team1_name")) or f1.get("name"):
        upsert_team(con, {"team_id": team1_id, "name": (summary.get("team1_name") if summary else f1.get("name")), "avatar": (summary.get("team1_avatar") if summary else f1.get("avatar")), "updated_at": None})
    if team2_id or (summary and summary.get("team2_name")) or f2.get("name"):
        upsert_team(con, {"team_id": team2_id, "name": (summary.get("team2_name") if summary else f2.get("name")), "avatar": (summary.get("team2_avatar") if summary else f2.get("avatar")), "updated_at": None})

    # Bulk upsert rosters (players)
    roster_players = []
    if kind != "past" and summary:
        for pr in (summary.get("team1_roster") or []):
            roster_players.append({"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})
        for pr in (summary.get("team2_roster") or []):
            roster_players.append({"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})
    else:
        for fac in (f1, f2):
            for pr in (fac.get("roster") or []):
                roster_players.append({"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})
    if roster_players:
        uniq = {}
        for p in roster_players:
            pid = p.get("player_id")
            if pid:
                uniq[pid] = p
        upsert_players_bulk(con, list(uniq.values()))

    configured_at = safe_int(
        (details.get("configured_at") if isinstance(details, dict) else None) \
        or (summary.get("_raw", {}).get("configured_at") if summary else None)
    , None)

    m = {
        "match_id": match_id,
        "championship_id": champ_row["championship_id"],
        "configured_at": configured_at,
        "round": safe_int(details.get("round"), None),
        "best_of": safe_int(details.get("best_of"), None),
        "started_at":  safe_int(summary.get("started_at")  if summary else details.get("started_at"),  None),
        "finished_at": safe_int(summary.get("finished_at") if summary else details.get("finished_at"), None),
        "scheduled_at":safe_int(summary.get("scheduled_at")if summary else details.get("scheduled_at"),None),
        "status": (summary.get("status") if summary else details.get("status") or "").lower() or None,
        "last_seen_at": int(time.time()),
        "team1_id":   team1_id or (summary.get("team1_id") if summary else None),
        "team2_id":   team2_id or (summary.get("team2_id") if summary else None),
        "winner_team_id": winner_team_id,
        "is_forfeit": 1 if forfeit_like else 0,
    }
    upsert_match(con, m)

    if kind != "past":
        return

    # MAPS
    map_rows = _extract_map_rows_from_stats(match_id, rounds)
    for r in map_rows:
        r["winner_team_id"] = _normalize_team_ref(r.get("winner_team_id"), team1_id, team2_id)

    # Democracy
    if not forfeit_like:
        votes, picks = [], []
        try:
            for ticket in _map_tickets_from_democracy(demo_json):
                for ent in (ticket.get("entities") or []):
                    if not isinstance(ent, dict):
                        continue
                    status = (ent.get("status") or "").lower()
                    sel = ent.get("selected_by")
                    name = ent.get("guid") or ent.get("game_map_id") or ent.get("class_name") or ent.get("name")
                    rnd = ent.get("round")
                    votes.append({
                        "round_num": rnd,
                        "map_name": name,
                        "status": "pick" if status == "selected" else status,
                        "selected_by_faction": sel,
                        "selected_by_team_id": _normalize_team_ref(sel, team1_id, team2_id),
                    })
                    if status in ("pick", "selected", "decider") and name:
                        order_key = rnd if isinstance(rnd, int) else 10**9
                        picks.append((order_key, name))
        except Exception:
            votes, picks = [], []

        if votes:
            votes.sort(key=lambda x: (x.get("round_num") is None, x.get("round_num"), x.get("map_name") or ""))
            pick_like = sum(1 for v in votes if (v.get("status") or "") in ("pick","selected","decider"))
            last = votes[-1]
            if pick_like >= 3:
                last["status"] = "decider"
            else:
                last["status"] = "overflow"
                last["selected_by_team_id"] = None
                last["selected_by_faction"] = None
            upsert_map_votes(con, match_id, votes)

        if picks:
            picks.sort(key=lambda x: x[0])
            names_in_order = []
            for _, nm in picks:
                if nm and nm not in names_in_order:
                    names_in_order.append(nm)
            for idx, name in enumerate(names_in_order, start=1):
                if idx - 1 < len(map_rows):
                    if not map_rows[idx - 1].get("map_name"):
                        map_rows[idx - 1]["map_name"] = name
                else:
                    map_rows.append({
                        "match_id": match_id,
                        "round_index": idx,
                        "map_name": name,
                        "score_team1": None,
                        "score_team2": None,
                        "winner_team_id": None,
                    })

        if not any(r.get("map_name") for r in map_rows):
            try:
                picks2 = ((details.get("voting") or {}).get("map") or {}).get("pick") or []
            except Exception:
                picks2 = []
            for idx, name in enumerate(picks2, start=1):
                if idx - 1 < len(map_rows):
                    if not map_rows[idx - 1].get("map_name"):
                        map_rows[idx - 1]["map_name"] = name
                else:
                    map_rows.append({
                        "match_id": match_id,
                        "round_index": idx,
                        "map_name": name,
                        "score_team1": None,
                        "score_team2": None,
                        "winner_team_id": None,
                    })

    if forfeit_like:
        map_rows = _extract_map_rows_from_details(match_id, details, team1_id, team2_id)

    if map_rows:
        upsert_maps(con, match_id, map_rows)

    # PLAYER STATS
    player_rows = _extract_player_rows(match_id, rounds)
    # Upsert any players referenced by the stats to avoid FK failures
    if player_rows:
        players_from_stats = []
        for r in player_rows:
            pid = r.get("player_id")
            if pid:
                players_from_stats.append({"player_id": pid, "nickname": r.get("nickname") or "", "updated_at": None})
        if players_from_stats:
            # Bulk upsert is idempotent and uses ON CONFLICT to avoid duplicates
            upsert_players_bulk(con, players_from_stats)

    for r in player_rows:
        r.pop("nickname", None)  # varmistus
        r["team_id"] = _normalize_team_ref(r.get("team_id"), team1_id, team2_id)
    player_rows = [r for r in player_rows if r.get("team_id")]
    if player_rows:
        upsert_player_stats(con, match_id, player_rows)

# ---- main sync --------------------------------------------------------------

def main(db_path: str, division_num: int = None, season: int = None, all_seasons: bool = False, playoffs_only: bool = False, clean: bool = False, vacuum: bool = False, cleanup_orphans: bool = False) -> None:
    con = get_conn(db_path)
    try:
        init_db(con)
        
        # If --cleanup-orphans, remove unused teams and players
        if cleanup_orphans:
            # Delete orphaned teams
            orphaned_teams = con.execute("""
                DELETE FROM teams WHERE team_id NOT IN (
                    SELECT DISTINCT team_id FROM (
                        SELECT team1_id as team_id FROM matches WHERE team1_id IS NOT NULL
                        UNION
                        SELECT team2_id FROM matches WHERE team2_id IS NOT NULL
                    )
                )
            """)
            teams_deleted = orphaned_teams.rowcount
            
            # Delete orphaned players
            orphaned_players = con.execute("""
                DELETE FROM players WHERE player_id NOT IN (
                    SELECT DISTINCT player_id FROM player_stats WHERE player_id IS NOT NULL
                )
            """)
            players_deleted = orphaned_players.rowcount
            
            # Delete old map pool data
            map_pool_deleted = con.execute("""
                DELETE FROM map_pool_seasons WHERE season NOT IN (
                    SELECT DISTINCT season FROM championships
                )
            """)
            pool_deleted = map_pool_deleted.rowcount
            
            con.commit()
            print(f">> [CLEANUP] Removed {teams_deleted} orphaned teams, {players_deleted} orphaned players, {pool_deleted} old map pool entries")
            return
        
        # If --vacuum, optimize database and exit
        if vacuum:
            db_path_obj = Path(db_path)
            size_before = db_path_obj.stat().st_size if db_path_obj.exists() else 0
            print(f">> [VACUUM] Starting optimization... (current size: {size_before / 1024 / 1024:.2f} MB)")
            con.execute("VACUUM")
            con.commit()
            size_after = db_path_obj.stat().st_size
            saved = size_before - size_after
            print(f">> [VACUUM] Complete! New size: {size_after / 1024 / 1024:.2f} MB (saved {saved / 1024 / 1024:.2f} MB)")
            return
        
        # If --clean, delete all championships not in divisions.json and exit
        if clean:
            valid_champ_ids = {d["championship_id"] for d in DIVISIONS}
            cursor = con.execute("SELECT championship_id FROM championships WHERE championship_id NOT IN ({})".format(
                ",".join("?" * len(valid_champ_ids)) if valid_champ_ids else "NULL"
            ), list(valid_champ_ids) if valid_champ_ids else [])
            to_delete = [row[0] for row in cursor.fetchall()]
            if to_delete:
                placeholders = ",".join("?" * len(to_delete))
                con.execute(f"DELETE FROM championships WHERE championship_id IN ({placeholders})", to_delete)
                con.commit()
                print(f">> [CLEAN] Deleted {len(to_delete)} championship(s) not in divisions.json")
            else:
                print(f">> [CLEAN] Database already matches divisions.json")
            return

        # Upsert championships from faceit_config.DIVISIONS
        champs = []
        for d in DIVISIONS:
            # Season filtering
            d_season = int(d.get("season", 0))
            if all_seasons:
                pass  # include all seasons
            elif season is not None:
                if d_season != season:
                    continue  # skip seasons not matching the filter
            else:
                if d_season < CURRENT_SEASON:
                    continue  # skip older seasons (unless --season or --all-seasons is set)
            
            # Division filtering
            if division_num is not None and d.get("division_num") != division_num:
                continue  # skip divisions not matching the filter
            
            # Playoff filtering
            if playoffs_only and not d.get("is_playoffs"):
                continue  # skip regular divisions when only playoffs requested
            
            row = upsert_championship(con, {
                "championship_id": d["championship_id"],
                "season": d["season"],
                "division_num": d["division_num"],
                "name": d["name"],
                "is_playoffs": d.get("is_playoffs", 0),
                "slug": d["slug"],
            })
            # Always fetch matches from the division list ID (even if DB has a legacy ID)
            row["_fetch_championship_id"] = d["championship_id"]
            champs.append(row)

        # Walk through every division in a single pass per division
        for c in champs:
            _sync_division_one_pass(con, c, force=args.force)

        print(">> [OK] Sync valmis")
    finally:
        # Sulje yhteys aina lopuksi
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sync Pappaliiga data into SQLite (championship-centric).")
    p.add_argument("--db", default=str(Path(__file__).with_name("pappaliiga.db")),
                   help="SQLite path (default: pappaliiga.db next to this file)")
    p.add_argument("--div", type=int, metavar="N",
                   help="Sync only division N (e.g., --div 1 for Division 1)")
    p.add_argument("--season", type=int, metavar="N",
                   help="Sync only season N (e.g., --season 11); requires --div or --all-seasons to be useful")
    p.add_argument("--all-seasons", action="store_true",
                   help="Sync all seasons (default: only current season)")
    p.add_argument("--force", action="store_true",
                   help="Force re-sync all matches, bypassing optimization checks (slower but complete refresh)")
    p.add_argument("--po", dest="playoffs_only", action="store_true",
                   help="Sync only playoffs (use with --div to sync playoff division only)")
    p.add_argument("--clean", action="store_true",
                   help="Delete all championships from DB not in divisions.json before syncing")
    p.add_argument("--cleanup-orphans", action="store_true",
                   help="Remove orphaned teams, players, and old map pool data")
    p.add_argument("--vacuum", action="store_true",
                   help="Optimize database file size by reclaiming unused space (run after --clean)")
    args = p.parse_args()
    main(args.db, args.div, args.season, args.all_seasons, args.playoffs_only, args.clean, args.vacuum, args.cleanup_orphans)
