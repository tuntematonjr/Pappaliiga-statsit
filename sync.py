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
from db import upsert_player
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
    upsert_map_catalog, add_map_to_season_pool
)

# Configure logging
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
        # Faceit saattaa välillä antaa "1,23" → normalisoidaan pisteeseen
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

def _persist_map_catalog_from_details(con: sqlite3.Connection, details: dict, season: int, game: str = "cs2") -> None:
    """
    Read voting.map.entities from match details and persist maps_catalog + map_pool_seasons.
    """
    voting = (details or {}).get("voting") or {}
    msec = voting.get("map") or {}
    entities = msec.get("entities") or []

    def _pretty_for(ent: dict, map_id: str) -> str:
        # 1) Faceitin nimi
        raw = (ent.get("name") or "").strip()
        # 2) Erikoiskaunistukset
        mid = (map_id or "").lower()
        if raw.lower() == "dust2" or "dust2" in mid:
            return "Dust II"
        # 3) Fallback: slug -> Title
        if raw:
            return raw
        slug = (map_id or "").replace("de_", "").replace("_", " ").strip()
        return slug.title() if slug else map_id

    for ent in entities:
        # Prefer stable id in order: class_name, game_map_id, guid
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
            "game": game,
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
                    "team_name": tname,
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
            m = re.match(r"^\s*(\d+)\s*[/\:]\s*(\d+)\s*$", score)
            if m:
                s1 = safe_int(m.group(1), None)
                s2 = safe_int(m.group(2), None)

        rows.append({
            "match_id": match_id,
            "round_index": idx,
            "map_name": name,
            "score_team1": s1,
            "score_team2": s2,
            "winner_team_id": rs.get("Winner") or rs.get("winner"),  # normalisoidaan myöhemmin
        })
    return rows

def _extract_map_rows_from_details(match_id: str, details: Dict[str, Any],
                                   team1_id: Optional[str], team2_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Luo maps-rivit match_details-datasta, kun stats puuttuu (luovutus, peruttu tms).
    Tukee sekä detailed_results (useita karttoja) että results.score (yksi rivi).
    """
    rows: List[Dict[str, Any]] = []

    # 1) Uudempi: detailed_results (lista per kartta; sisältää voittajan ja pisteet)
    det = details.get("detailed_results")
    if isinstance(det, list) and det:
        for idx, item in enumerate(det, start=1):
            factions = item.get("factions") or {}
            s1 = safe_int((factions.get("faction1") or {}).get("score"))
            s2 = safe_int((factions.get("faction2") or {}).get("score"))
            w  = item.get("winner")  # 'faction1' / 'faction2' tai jo id
            w_norm = _normalize_team_ref(w, team1_id, team2_id)

            rows.append({
                "match_id": match_id,
                "round_index": idx,
                "map_name": None,     # usein ei ole nimeä luovutuksissa
                "score_team1": s1,
                "score_team2": s2,
                "winner_team_id": w_norm,
            })

    # 2) Fallback: results.score (vain kokonaiskarttojen lukumäärä, esim. BO2: 2–0)
    if not rows:
        results = details.get("results", {}) or {}
        score   = results.get("score", {}) or {}
        s1 = safe_int(score.get("faction1"))
        s2 = safe_int(score.get("faction2"))
        w  = results.get("winner") or details.get("winner_team_id")
        w_norm = _normalize_team_ref(w, team1_id, team2_id)

        if s1 is not None and s2 is not None:
            rows.append({
                "match_id": match_id,
                "round_index": 1,
                "map_name": None,
                "score_team1": s1,
                "score_team2": s2,
                "winner_team_id": w_norm,
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
    Palauta (team1_id, team2_id) käyttäen:
      1) rounds[*].teams[*].team_id (jos statsit on)
      2) nimi-matchi rounds-datan tiiminimistä
      3) FALLBACK: details.teams.faction*.faction_id (toimii ilman statseja)
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

    # Jos nimi-match ei onnistunut, mutta roundsissa on 2 tiimiä
    if (t1_id is None or t2_id is None) and len(seen_ids) >= 2:
        if t1_id is None:
            t1_id = seen_ids[0]
        if t2_id is None:
            t2_id = next((x for x in seen_ids if x != t1_id), seen_ids[1])

    # UUSI: varmistus ilman statseja — poimi suoraan details.teams.faction*.faction_id
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
    Muunna 'faction1'/'faction2'/'1'/'2'/'team1'/'team2' → oikea team_id.
    Jos ref on jo ID, palautetaan sellaisenaan.
    """
    if ref is None:
        return None
    s = str(ref).lower()
    if s in ("faction1", "1", "team1"):
        return team1_id
    if s in ("faction2", "2", "team2"):
        return team2_id
    return str(ref)

# Skipataanko kannassa jo valmiiksi finished-matsit (säästää API:a)?
SKIP_FINISHED_IN_DB = True

def _is_finished_in_db(con: sqlite3.Connection, match_id: str) -> bool:
    row = con.execute("SELECT status FROM matches WHERE match_id=?", (match_id,)).fetchone()
    if not row:
        return False
    return (row["status"] or "").lower() in {"finished", "played", "closed"}

def _has_full_stats(con: sqlite3.Connection, match_id: str) -> bool:
    """Return True if both maps and player_stats exist for this match_id."""
    r1 = con.execute("SELECT 1 FROM maps WHERE match_id=? LIMIT 1", (match_id,)).fetchone()
    r2 = con.execute("SELECT 1 FROM player_stats WHERE match_id=? LIMIT 1", (match_id,)).fetchone()
    return bool(r1 and r2)

def _is_header_unchanged_by_summary(con: sqlite3.Connection, match_id: str, summary: dict) -> bool:
    """
    Compare only fields we get from the list endpoint ('summary').
    If all equal in DB, we can skip without hitting details/stats.
    """
    row = con.execute(
        """SELECT status, scheduled_at, started_at, finished_at, team1_id, team2_id
           FROM matches WHERE match_id=?""",
        (match_id,)
    ).fetchone()
    if not row:
        return False

    def s_int(x): 
        try: return int(x)
        except: return None

    old_status = (row["status"] or "").lower()
    new_status = (summary.get("status") or "").lower()

    return (
        old_status == new_status and
        (row["scheduled_at"] or None) == s_int(summary.get("scheduled_at")) and
        (row["started_at"]   or None) == s_int(summary.get("started_at"))   and
        (row["finished_at"]  or None) == s_int(summary.get("finished_at"))  and
        (row["team1_id"]     or None) == (summary.get("team1_id") or None)  and
        (row["team2_id"]     or None) == (summary.get("team2_id") or None)
    )

def _target_kind_from_status(item: dict) -> str:
    """
    Map Faceit status → käsittelyluokka.
      - 'finished' / 'closed' / 'played' → 'past' (haetaan statsit)
      - muut ('ongoing', 'live', 'upcoming', 'scheduled', tms.) → 'upcoming'
    """
    st = str(item.get("status") or "").lower()
    past_statuses = {"finished", "closed", "played"}
    return "past" if st in past_statuses else "upcoming"

def _list_matches_all(championship_id: str) -> list[dict]:
    """
    Hae kaikki matsit kerralla (type=all), leimaa _target_kind ja nosta ydinkentät mukaan.
    """
    items = list_championship_matches(championship_id, match_type="all") or []
    out: list[dict] = []
    for it in items:
        teams = it.get("teams") or {}
        f1 = teams.get("faction1") or {}
        f2 = teams.get("faction2") or {}
        out.append({
            "_raw": it,  # talteen jos tarvitsee myöhemmin
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

def _sync_division_one_pass(con: sqlite3.Connection, champ_row: dict) -> None:
    """
    One pass over all matches (type=all). Ongoing are handled like scheduled.
    Commit once per match. Skip logic:
      - If DB already has status='finished' AND full stats exist → skip (no API calls)
      - If summary shows no header changes vs DB for non-past → skip
      - NEW: If either faction is 'bye' → skip hard (no DB writes)
    """
    matches = _list_matches_all(champ_row["championship_id"])
    div_title = champ_row.get("name") or champ_row.get("slug") or f"Div{champ_row.get('division_num','?')}-S{champ_row.get('season','?')}"
    title = f"{div_title} — All"
    total = len(matches)
    if total == 0:
        _progress_bar(title, 0, 0, time.time(), skipped=0)
        return

    seen: set[str] = set()
    skipped = 0
    start_ts = time.time()
    for i, m in enumerate(matches, start=1):
        mid = m.get("match_id")
        if not mid or mid in seen:
            _progress_bar(title, i, total, start_ts, skipped); continue

        # NEW: Skip BYE matches early to avoid any DB writes or API calls.
        if _is_bye_match_summary(m):
            logging.info("[skip] bye match %s (%s vs %s)", mid, m.get("team1_name"), m.get("team2_name"))
            seen.add(mid); skipped += 1
            _progress_bar(title, i, total, start_ts, skipped)
            continue

        # 1) Finished in DB + stats present -> skip entirely
        if SKIP_FINISHED_IN_DB and _is_finished_in_db(con, mid) and _has_full_stats(con, mid):
            seen.add(mid); skipped += 1
            _progress_bar(title, i, total, start_ts, skipped)
            continue

        # 2) Non-past summary that hasn't changed (status/schedule/teams unchanged) -> skip
        tgt = m.get("_target_kind") or "upcoming"
        if tgt != "past" and _is_header_unchanged_by_summary(con, mid, m):
            seen.add(mid); skipped += 1
            _progress_bar(title, i, total, start_ts, skipped)
            continue

        # 3) Process (insert/update). For non-past, use summary to avoid details call.
        seen.add(mid)
        try:
            summary = m if tgt != "past" else None
            persist_match(con, champ_row, mid, kind=tgt, summary=summary)
            con.commit()  # one commit per match
        except Exception as e:
            logging.warning("sync (all) %s failed: %s", mid, e)
            try:
                con.rollback()
            except Exception:
                pass

        _progress_bar(title, i, total, start_ts, skipped)

def persist_match(con: sqlite3.Connection, champ_row: Dict[str, Any], match_id: str, kind: str, summary: Optional[Dict[str, Any]] = None) -> None:
    """
    Tallentaa match headerin aina.
    - kind == 'past' → hakee myös statsit + mapit.
    - kind != 'past' → ei hae statseja; jos 'summary' annettu (listavastauksesta),
      käytetään siitä tiimit/ajat/status ilman get_match_details -kutsua.
    """
    details: Dict[str, Any] = {}
    f1: Dict[str, Any] = {}
    f2: Dict[str, Any] = {}
    game_name: Optional[str] = None

    if kind != "past" and isinstance(summary, dict) and _is_bye_match_summary(summary):
        logging.info("[skip] bye (summary) %s", match_id)
        return

    if kind != "past" and isinstance(summary, dict):
        # Käytä list endpointin tietoja suoraan
        details = summary.get("_raw") or {}
        f1 = {"name": summary.get("team1_name"), "avatar": summary.get("team1_avatar")}
        f2 = {"name": summary.get("team2_name"), "avatar": summary.get("team2_avatar")}
        game_name = (details.get("game") or None)
    else:
        # Past → tarvitaan aina tarkemmat detailit + statsit
        details = get_match_details(match_id) or {}
        # Skip BYE
        if _is_bye_match_details(details):
            logging.info("[skip] bye (details) %s", match_id)
            return
        teams_d = details.get("teams") or {}
        f1 = teams_d.get("faction1") or {}
        f2 = teams_d.get("faction2") or {}
        game_field = details.get("game")
        game_name = game_field.get("name") if isinstance(game_field, dict) else (game_field if isinstance(game_field, str) else None)
        # Katalogi/pooli talteen
        _persist_map_catalog_from_details(con, details, season=champ_row["season"], game=champ_row.get("game") or "cs2")

        # Detect forfeit-like finished match:
        # Only treat as forfeit if 'detailed_results' exists, has numeric scores for BOTH
        # sides on EVERY map, and on each map one of the scores is exactly 0.
        det = details.get("detailed_results") or []
        forfeit_like = False
        if isinstance(det, list) and det:
            numeric_pairs = []
            for it in det:
                if not isinstance(it, dict):
                    continue
                fac = it.get("factions") or {}
                s1 = safe_int((fac.get("faction1") or {}).get("score"), None)
                s2 = safe_int((fac.get("faction2") or {}).get("score"), None)
                if s1 is None or s2 is None:
                    numeric_pairs = []
                    break
                numeric_pairs.append((s1, s2))
            if numeric_pairs and all(a == 0 or b == 0 for (a, b) in numeric_pairs):
                forfeit_like = True

    # STATS vain finished/past
    stats = {}
    rounds = []
    if kind == "past":
        try:
            stats = get_match_stats(match_id) or {}
        except Exception as e:
            logging.info("[skip] stats %s -> %s", match_id, e)
            stats = {}
    rounds = _extract_rounds_from_stats(stats)

    # Statsien jälkeen: päättele forfeit yksinkertaisesti siitä, pelattiinko karttoja (rounds).
    has_detailed = isinstance(details.get("detailed_results"), list) and len(details.get("detailed_results") or []) > 0
    has_score    = bool(((details.get("results") or {}).get("score") or {}))

    if kind == "past" and not rounds:
        # Ei raundeja statsissa → jos detai­leissa ei ole minkäänlaista tulostietoa, bailaa "not_found".
        if not has_detailed and not has_score:
            upsert_match(con, {
                "match_id": match_id,
                "championship_id": champ_row["championship_id"],
                "status": "not_found",
            })
            return

    # Forfeit-tulkinta: ei raundeja, mutta detalhesissa on tulosta → luovutus/tekninen voitto
    forfeit_like = (kind == "past" and not rounds and (has_detailed or has_score))


    # Team-id:t myös ilman statseja (fallback details.teams.faction*.faction_id)
    team1_id, team2_id = _derive_team_ids(details or {}, rounds)

    # Winner (normalize 'faction1'/'faction2' -> team_id)
    winner_raw = None
    try:
        res = (details.get("results") or {})
        winner_raw = res.get("winner") or res.get("winner_team_id")
    except Exception:
        pass
    winner_team_id = _normalize_team_ref(winner_raw, team1_id, team2_id)

    # Upsert MATCH header
    comp_name = (details.get("competition_name") if isinstance(details, dict) else None) \
                or (summary.get("_raw", {}).get("competition_name") if summary else None)

    configured_at = safe_int(
        (details.get("configured_at") if isinstance(details, dict) else None) \
        or (summary.get("_raw", {}).get("configured_at") if summary else None)
    , None)

    m = {
        "match_id": match_id,
        "championship_id": champ_row["championship_id"],
        "competition_name": comp_name,
        "configured_at": configured_at,

        "game": game_name or champ_row.get("game") or "cs2",
        "round": safe_int(details.get("round"), None),
        "best_of": safe_int(details.get("best_of"), None),

        "started_at":  safe_int(summary.get("started_at")  if summary else details.get("started_at"),  None),
        "finished_at": safe_int(summary.get("finished_at") if summary else details.get("finished_at"), None),
        "scheduled_at":safe_int(summary.get("scheduled_at")if summary else details.get("scheduled_at"),None),
        "status": (summary.get("status") if summary else details.get("status") or "").lower() or None,
        "last_seen_at": int(time.time()),

        "team1_id":   team1_id or (summary.get("team1_id") if summary else None),
        "team1_name": (summary.get("team1_name") if summary else f1.get("name")),
        "team2_id":   team2_id or (summary.get("team2_id") if summary else None),
        "team2_name": (summary.get("team2_name") if summary else f2.get("name")),
        "winner_team_id": winner_team_id,
    }

    # Teams talteen (avatarit)
    if m["team1_id"] or m["team1_name"]:
        upsert_team(con, {"team_id": m["team1_id"], "name": m["team1_name"], "avatar": (summary.get("team1_avatar") if summary else f1.get("avatar")), "updated_at": None})
    if m["team2_id"] or m["team2_name"]:
        upsert_team(con, {"team_id": m["team2_id"], "name": m["team2_name"], "avatar": (summary.get("team2_avatar") if summary else f2.get("avatar")), "updated_at": None})

    # Rosterit
    if kind != "past" and summary:
        for pr in (summary.get("team1_roster") or []):
            upsert_player(con, {"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})
        for pr in (summary.get("team2_roster") or []):
            upsert_player(con, {"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})
    else:
        for fac in (f1, f2):
            for pr in (fac.get("roster") or []):
                upsert_player(con, {"player_id": pr.get("player_id"), "nickname": pr.get("nickname") or "", "updated_at": None})

    # Header aina talteen
    upsert_match(con, m)

    # upcoming/ongoing → stop here
    if kind != "past":
        return

    # Democracy (voi olla 404 → ok). Skip totally for forfeits to avoid 404 noise.
    demo_json = None
    if not forfeit_like:
        try:
            demo_json = get_democracy_history(match_id)
        except Exception:
            demo_json = None

    # MAPS round_statsista
    map_rows = _extract_map_rows_from_stats(match_id, rounds)
    for r in map_rows:
        r["winner_team_id"] = _normalize_team_ref(r.get("winner_team_id"), team1_id, team2_id)

    # Picks democracy → details.voting
    picks = []
    try:
        payload = demo_json.get("payload") if isinstance(demo_json, dict) else None
        tickets = payload.get("tickets", []) if isinstance(payload, dict) else []
        cand = []
        for tk in tickets:
            if not isinstance(tk, dict):
                continue
            if str(tk.get("entity_type") or "").lower() != "map":
                continue
            for ent in (tk.get("entities") or []):
                if not isinstance(ent, dict):
                    continue
                status = str(ent.get("status") or "").lower()
                if status not in ("pick", "decider", "selected"):
                    continue
                name = (ent.get("guid") or ent.get("game_map_id") or ent.get("class_name") or ent.get("name"))
                rnd = ent.get("round")
                order_key = rnd if isinstance(rnd, int) else 10**9
                if name:
                    cand.append((order_key, name))
        cand.sort(key=lambda x: x[0])
        for _, n in cand:
            if n not in picks:
                picks.append(n)
    except Exception:
        picks = []

    if picks:
        for idx, name in enumerate(picks, start=1):
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

    # Fallback: details.detailed_results → LUOVUTUS/13–0 jne
    if not map_rows:
        map_rows = _extract_map_rows_from_details(match_id, details, team1_id, team2_id)
        if map_rows:
            logging.info("[forfeit] %s: inserted %d map rows from detailed_results/results.score", match_id, len(map_rows))

    if map_rows:
        upsert_maps(con, match_id, map_rows)

    # Democracy / map_votes
    try:
        votes = []
        payload = demo_json.get("payload") if isinstance(demo_json, dict) else None
        tickets = payload.get("tickets", []) if isinstance(payload, dict) else []
        for ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            if str(ticket.get("entity_type") or "").lower() != "map":
                continue
            for ent in (ticket.get("entities") or []):
                if not isinstance(ent, dict):
                    continue
                sel = ent.get("selected_by")
                status = (ent.get("status") or "").lower()
                votes.append({
                    "round_num": ent.get("round"),
                    "map_name":  ent.get("guid") or ent.get("game_map_id") or ent.get("class_name") or ent.get("name"),
                    "status":    "pick" if status == "selected" else status,
                    "selected_by_faction": sel,
                    "selected_by_team_id": _normalize_team_ref(sel, team1_id, team2_id),
                })

        if votes:
            votes.sort(key=lambda x: (x.get("round_num") is None, x.get("round_num"), x.get("map_name") or ""))
            if len(votes) >= 7:
                pick_like = sum(1 for v in votes if (v.get("status") or "") in ("pick", "selected", "decider"))
                last = votes[-1]
                if pick_like >= 3:
                    last["status"] = "decider"
                else:
                    last["status"] = "overflow"
                    last["selected_by_team_id"] = None
                    last["selected_by_faction"] = None
            upsert_map_votes(con, match_id, votes)
    except Exception:
        pass

    # TEAM & PLAYER STATS
    player_rows = _extract_player_rows(match_id, rounds)
    for r in player_rows:
        r["team_id"] = _normalize_team_ref(r.get("team_id"), team1_id, team2_id)
    player_rows = [r for r in player_rows if r.get("team_id")]

    uniq_players: dict[str, str] = {}
    for r in player_rows:
        pid = r.get("player_id")
        if pid:
            uniq_players[pid] = r.get("nickname") or ""
    for pid, nick in uniq_players.items():
        upsert_player(con, {"player_id": pid, "nickname": nick, "updated_at": None})

    if player_rows:
        upsert_player_stats(con, match_id, player_rows)

# ---- main sync --------------------------------------------------------------

def main(db_path: str) -> None:
    con = get_conn(db_path)
    try:
        init_db(con)

        # Upsert championships from faceit_config.DIVISIONS
        champs = []
        for d in DIVISIONS:
            if int(d.get("season", 0)) < CURRENT_SEASON:
                continue  # skip older seasons
            row = upsert_championship(con, {
                "championship_id": d["championship_id"],
                "season": d["season"],
                "division_num": d["division_num"],
                "name": d["name"],
                "game": d.get("game", "cs2"),
                "is_playoffs": d.get("is_playoffs", 0),
                "slug": d["slug"],
            })
            champs.append(row)

        # Käy kaikki divisioonat läpi yhdellä passilla / divisioona
        for c in champs:
            _sync_division_one_pass(con, c)

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
    args = p.parse_args()
    main(args.db)
