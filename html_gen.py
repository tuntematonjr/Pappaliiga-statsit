# Generate one HTML per division with team summaries, player tables, and map stats.
from pathlib import Path
import os
import sys
import shutil
import asyncio
import aiofiles
import aiofiles.os
import argparse
from collections import defaultdict
from typing import Optional
from faceit_config import DIVISIONS, TOOL_VERSION, CURRENT_SEASON
from html import escape
import hashlib, tempfile, re
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from stats_utils import weighted_percentile, weighted_median
from division_overrides import load_division_overrides, banned_teams_for_division

from db import (
    get_map_art, normalize_map_id,
    get_division_generated_ts,
    query,
)
from async_db import (
    AsyncConnectionPool,
    query_async,
    get_teams_in_championship_async,
    compute_team_summary_data_async,
    compute_champ_map_avgs_data_async,
    compute_champ_thresholds_data_async,
    get_division_generated_ts_async,
    compute_player_table_data_async,
    compute_player_deltas_async,
    compute_champ_player_summary_async,
    compute_champ_map_summary_data_async,
)


# --- HTML/template versioning ---
HTML_TEMPLATE_VERSION = 9

# Parse command line arguments
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate HTML statistics for Pappaliiga divisions')
    parser.add_argument('--force', '-f', action='store_true', help='Force regeneration of all files')
    parser.add_argument('--div', type=int, help='Generate only specific division number (1-based)')
    parser.add_argument('--all-seasons', action='store_true', help='Generate all seasons (default: current season only)')
    return parser.parse_args(argv)


def _set_runtime_args(parsed: argparse.Namespace) -> None:
    global args, FORCE_REGEN
    args = parsed
    FORCE_REGEN = parsed.force


# Default arguments used when the module is imported (e.g., for testing)
args = argparse.Namespace(force=False, div=None, all_seasons=False)
FORCE_REGEN = args.force

HELSINKI_TZ = ZoneInfo("Europe/Helsinki")
_GENVER_RE = re.compile(r"<!--\s*GENVER:(\d+) LAST_MATCH:(\d+)\s*-->", re.IGNORECASE)
_GENMETA_RE = re.compile(
    r"<!--\s*GENVER:(?P<ver>\d+) LAST_MATCH:(?P<last>\d+)\s*-->",
    re.IGNORECASE,
)

# runtime values set when page_start() is called
CURRENT_GEN_TS: int = 0
CURRENT_LAST_MATCH: int = 0
DB_PATH = str(Path(__file__).with_name("pappaliiga.db"))
OUT_DIR = Path(__file__).with_name("docs")
_DIVISION_OVERRIDES = load_division_overrides()

UNIFIED_HEAD = """<!doctype html>
<html lang=\"fi\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=1200, initial-scale=0, maximum-scale=1, user-scalable=yes\"/>
<title>{title}</title>
<!-- Externalized CSS/JS for maintainability and performance -->
<link rel=\"stylesheet\" href=\"styles.css\">
<script defer src=\"app.js\"></script>

</head>
<body class=\"{page_class}\">\n"""

HTML_FOOT = """
</body>
</html>
"""

def page_start(title: str, page_class: str = "", last_match_ts: Optional[int] = None) -> str:
    """Return the page head with an embedded GENVER token.
    Optionally include LAST_MATCH epoch (used for skip-checking and displaying update time).
    """
    global CURRENT_GEN_TS, CURRENT_LAST_MATCH
    lm = int(last_match_ts) if last_match_ts else 0
    token = f"<!-- GENVER:{HTML_TEMPLATE_VERSION} LAST_MATCH:{lm} -->"
    CURRENT_LAST_MATCH = lm
    return token + "\n" + UNIFIED_HEAD.replace("{title}", title).replace("{page_class}", page_class)

def topbar(show_back_to_index: bool):
    back = '<a class="btn btn-ghost" href="index.html">← Takaisin indexiin</a>' if show_back_to_index else ""
    return f"""
    <div class="container">
      <div class="topbar">
        <div class="brand">
          <a href="https://armafinland.fi" target="_blank">
            <img src="https://armafinland.fi/css/gfx/armafin-logo-200px.png" 
                alt="AFI logo" 
                title="ArmaFinland netti sivut"
                class="logo promo-logo"/>
                
          </a>

          <span>AFI - Unofficial Pappaliiga CS Stats v{TOOL_VERSION}</span>

          <a href="https://pappaliiga.fi" target="_blank">
            <img src="https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png" 
                alt="Pappaliiga logo" 
                title="Pappaliiga netti sivut"
                class="logo promo-logo"/>
                
          </a>
        </div>
        {back}
      </div>
    </div>
    """


def page_end():
    return "</body></html>"


# Tooltip text for Rating1 column
TOOLTIP_RATING1 = (
    "Rating1 ≈ HLTV 1.0:\n"
    "  ( KR/0.679 + SURV/0.317 + ADR/79.9 ) / 3\n"
    "Missä:\n"
    "  KR   = Kills per Round (kills / rounds)\n"
    "  SURV = Survived per Round = 1 - (deaths / rounds)\n"
    "  ADR  = Average Damage per Round\n"
    "Baselinet on kalibroitu niin, että ~1.00 ≈ sarjan keskitason suoritus."
)

# ------------------------------
# helpers
# ------------------------------

def format_ts(ts: int | None) -> str:
    """
    Convert UTC epoch → Europe/Helsinki local time string.
    Returns '—' if ts is None/0/empty.
    """
    if not ts:
        return "—"
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(HELSINKI_TZ)
    return dt.strftime("%d.%m.%Y %H:%M")


def _format_banned_team_summary(team: dict) -> str:
    """Return escaped team name for banners/navigation."""
    return escape(team.get("team_name") or team.get("team_id") or "-")

def _fs_mtime(path: Path) -> int:
    try:
        return int(path.stat().st_mtime)
    except FileNotFoundError:
        return 0

def esc_title(s: str) -> str:
    # Strip single quotes and replace line breaks with HTML-safe markers
    return (s or "").replace("'", "").replace("\n", "&#10;")

def _render_card_header(section_type: str, title: str, hint: str = "Click to expand", open_attr: str = " open") -> list[str]:
    """Helper function to generate card header HTML lines"""
    return [
        f'<details class="card {section_type}-section"{open_attr}>',
        '  <summary>',
        f'    <div class="card-head {section_type}">',
        '      <span class="chev">›</span>',
        f'      <span class="title">{title}</span>',
        f'      <span class="hint">{hint}</span>',
        '    </div>',
        '  </summary>',
        '  <div class="card-content">'
    ]

def _render_card_footer() -> list[str]:
    """Helper function to generate card footer HTML lines"""
    return [
        '  </div>',  # /card-content
        '</details>'  # /section
    ]

def _render_sortable_th(table_id: str, col_idx: int, title: str, label: str, numeric: bool = True) -> str:
    """Helper function to generate sortable table header"""
    return f'<th data-sortable onclick="sortTable(\'{table_id}\',{col_idx},{str(numeric).lower()})" title="{title}">{label}</th>'

def _read_embedded_version(path: str) -> int:
    """
    Reads the file and looks for <!-- GENVER:x --> token anywhere.
    Returns int x or 0 if not found.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
        m = _GENVER_RE.search(data)
        if not m:
            return 0
        return int(m.group(1))
    except FileNotFoundError:
        return 0


def _read_embedded_meta(path: str) -> tuple[int, int, int]:
    """Return (ver, generated_at, last_match) from embedded GENVER token in file."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
        m = _GENMETA_RE.search(data)
        if not m:
            return (0, 0, 0)
        ver = int(m.group('ver') or 0)
        last = int(m.group('last') or 0)
        return (ver, 0, last)
    except FileNotFoundError:
        return (0, 0, 0)

def compute_champ_player_summary(con, division_id: int, min_rounds: int = 40, min_flashes: int = 10):
    """
    Division summary + leaders (optimized to reduce DB roundtrips):
      - join teams/maps/rounds in a single CTE query
      - keep the rest of the calculations unchanged
    """
    # Main player-centric dataset, same as before
    rows = query(con, """
      SELECT
        ps.player_id,
        COALESCE(MAX(pl.nickname), '') AS nick,
        MAX(t.name) AS team_name,

        -- summat
        SUM(ps.kills)                       AS kills,
        SUM(ps.deaths)                      AS deaths,
        SUM(ps.assists)                     AS assists,
        SUM(COALESCE(ps.utility_damage,0))  AS util_total,
        SUM(COALESCE(ps.enemies_flashed,0)) AS flashed_total,
        SUM(COALESCE(ps.flash_count,0))     AS flash_cnt_total,
        SUM(COALESCE(ps.entry_wins,0))      AS entry_wins,
        SUM(COALESCE(ps.entry_count,0))     AS entry_count,
        SUM(COALESCE(ps.cl_1v1_wins,0))     AS c11_wins,
        SUM(COALESCE(ps.cl_1v1_attempts,0)) AS c11_atts,
        SUM(COALESCE(ps.cl_1v2_wins,0))     AS c12_wins,
        SUM(COALESCE(ps.cl_1v2_attempts,0)) AS c12_atts,

        -- kierrokset painotuksiin
        SUM(mp.score_team1 + mp.score_team2)                             AS rounds,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.adr,0) )    AS adr_weighted,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.kr,0) )     AS kr_weighted

      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id AND m.is_forfeit = 0
      JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      LEFT JOIN players pl ON pl.player_id = ps.player_id
      LEFT JOIN teams   t  ON t.team_id   = ps.team_id
      WHERE m.championship_id = ?
      GROUP BY ps.player_id
    """, (division_id,))

    # --- Combined aggregates via a single query ---
    agg = query(con, """
      WITH
      team_ids AS (
        SELECT team1_id AS tid FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
        UNION
        SELECT team2_id AS tid FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
      ),
      rounds_cte AS (
        SELECT SUM(mp.score_team1 + mp.score_team2) AS total_rounds
        FROM maps mp JOIN matches m ON m.match_id=mp.match_id
        WHERE m.championship_id=? AND m.is_forfeit = 0
      ),
      maps_cte AS (
        SELECT COUNT(*) AS maps_cnt
        FROM maps mp JOIN matches m ON m.match_id=mp.match_id
        WHERE m.championship_id=? AND m.is_forfeit = 0
      )
      SELECT
        (SELECT COUNT(*) FROM team_ids)                       AS teams,
        (SELECT maps_cnt FROM maps_cte)                       AS maps,
        (SELECT total_rounds FROM rounds_cte)                 AS rounds
    """, (division_id, division_id, division_id, division_id))
    teams       = int((agg[0]["teams"] or 0)) if agg else 0
    maps_cnt    = int((agg[0]["maps"]  or 0)) if agg else 0
    total_rounds= int((agg[0]["rounds"] or 0)) if agg else 0

    # --- Distributions and leaders as before ---
    kd_vals, kd_w = [], []
    adr_vals, adr_w = [], []
    kr_vals,  kr_w  = [], []
    surv_vals, surv_w = [], []
    r1_vals, r1_w   = [], []

    leaders_pool = []
    totals_kills = []
    totals_deaths = []

    for r in rows:
        nick = r["nick"] or r["player_id"]
        team = r.get("team_name") or "-"
        rounds = r["rounds"] or 0

        kills   = r["kills"] or 0
        deaths  = r["deaths"] or 0
        assists = r["assists"] or 0

        adr = (r["adr_weighted"] / rounds) if rounds else 0.0
        kr  = (kills / rounds) if rounds else 0.0
        kd  = (kills / deaths) if deaths else float(kills)

        deaths_pr = (deaths / rounds) if rounds else 0.0
        survival_pct = max(0.0, 1.0 - deaths_pr) * 100.0
        surv_ratio = survival_pct / 100.0
        rating1 = ((kr / 0.679) + (surv_ratio / 0.317) + (adr / 79.9)) / 3.0 if rounds else 0.0

        if rounds > 0:
            kd_vals.append(kd);              kd_w.append(rounds)
            adr_vals.append(adr);            adr_w.append(rounds)
            kr_vals.append(kr);              kr_w.append(rounds)
            surv_vals.append(survival_pct);  surv_w.append(rounds)
            r1_vals.append(rating1);         r1_w.append(rounds)

        totals_kills.append( (nick, team, kills) )
        totals_deaths.append((nick, team, deaths))

        if rounds >= min_rounds:
            udpr = (r["util_total"] or 0) / rounds
            flashed_pr = (r["flashed_total"] or 0) / rounds
            assist_pr  = assists / rounds

            ewin = r["entry_wins"] or 0
            eatt = r["entry_count"] or 0
            entry_wr = (100.0 * ewin / eatt) if eatt >= 10 else -1.0

            c11w = r["c11_wins"] or 0; c11a = r["c11_atts"] or 0
            c12w = r["c12_wins"] or 0; c12a = r["c12_atts"] or 0
            c_wins = c11w + c12w
            c_atts = c11a + c12a
            clutch_wr = (100.0 * c_wins / c_atts) if c_atts >= 10 else -1.0

            flashed_total = r["flashed_total"] or 0
            flash_cnt_total = r["flash_cnt_total"] or 0
            enemies_per_flash = (flashed_total / flash_cnt_total) if (flash_cnt_total >= min_flashes and rounds >= min_rounds) else -1.0

            leaders_pool.append({
                "nick": nick, "team": team, "rounds": rounds,
                "kd": kd, "adr": adr, "kr": kr,
                "udpr": udpr,
                "enemies_per_flash": enemies_per_flash,
                "assist_pr":  assist_pr,
                "entry_wr":   entry_wr,
                "clutch_wr":  clutch_wr,
            })

    def _wperc(vals, w, p):
        return weighted_percentile(vals, w, p) if vals else 0.0

    kd_p50, kd_p25, kd_p75 = _wperc(kd_vals, kd_w, 50), _wperc(kd_vals, kd_w, 25), _wperc(kd_vals, kd_w, 75)
    adr_p50, adr_p25, adr_p75 = _wperc(adr_vals, adr_w, 50), _wperc(adr_vals, adr_w, 25), _wperc(adr_vals, adr_w, 75)
    kr_p50,  kr_p25,  kr_p75  = _wperc(kr_vals,  kr_w, 50),  _wperc(kr_vals,  kr_w, 25),  _wperc(kr_vals,  kr_w, 75)
    surv_p50, surv_p25, surv_p75 = _wperc(surv_vals, surv_w, 50), _wperc(surv_vals, surv_w, 25), _wperc(surv_vals, surv_w, 75)
    r1_p50,   r1_p25,   r1_p75   = _wperc(r1_vals,   r1_w,   50), _wperc(r1_vals,   r1_w,   25), _wperc(r1_vals,   r1_w,   75)

    def _best(metric):
        if not leaders_pool:
            return ("-", "-", 0.0)
        valid = [x for x in leaders_pool if x[metric] is not None and x[metric] >= 0]
        if not valid:
            return ("-", "-", 0.0)
        b = max(valid, key=lambda x: x[metric])
        return (b["nick"], b["team"], b[metric])

    top_frg_total     = max(totals_kills,  key=lambda x: x[2]) if totals_kills  else ("-", "-", 0)
    most_deaths_total = max(totals_deaths, key=lambda x: x[2]) if totals_deaths else ("-", "-", 0)

    leaders = {
        "top_frg_total":     top_frg_total,
        "most_deaths_total": most_deaths_total,
        "adr":        _best("adr"),
        "kd":         _best("kd"),
        "kr":         _best("kr"),
        "udpr":       _best("udpr"),
        "enemies_per_flash": _best("enemies_per_flash"),
        "assist_pr":  _best("assist_pr"),
        "entry_wr":   _best("entry_wr"),
        "clutch_wr":  _best("clutch_wr"),
    }

    return {
        "players": len(rows),
        "teams": teams,
        "maps": maps_cnt,
        "rounds": total_rounds,
        "kd_p50": kd_p50, "kd_p25": kd_p25, "kd_p75": kd_p75,
        "adr_p50": adr_p50, "adr_p25": adr_p25, "adr_p75": adr_p75,
        "kr_p50": kr_p50,  "kr_p25": kr_p25,  "kr_p75": kr_p75,
        "surv_p50": surv_p50, "surv_p25": surv_p25, "surv_p75": surv_p75,
        "r1_p50": r1_p50,   "r1_p25": r1_p25,   "r1_p75": r1_p75,
        "leaders": leaders,
    }

async def _index_card_stats_async(pool: AsyncConnectionPool, championship_id: str) -> tuple[int, int, int]:
    """
    Async version of _index_card_stats.
    Returns (teams, played, total) for the index card.
    - teams: unique teams (team1_id ∪ team2_id)
    - played: matches finished (finished_at IS NOT NULL OR status='finished')
    - total: all matches in the database
    """
    r = await query_async(pool, """
      SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN finished_at IS NOT NULL OR LOWER(COALESCE(status,''))='finished'
                 THEN 1 ELSE 0 END) AS played
      FROM matches
      WHERE championship_id=?
    """, (championship_id,))
    total = int((r[0]["total"] or 0)) if r else 0
    played = int((r[0]["played"] or 0)) if r else 0

    teams = await query_async(pool, """
      SELECT COUNT(*) AS c FROM (
        SELECT team1_id AS tid FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
        UNION
        SELECT team2_id AS tid FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
      )
    """, (championship_id, championship_id))
    team_cnt = int((teams[0]["c"] or 0)) if teams else 0

    return (team_cnt, played, total)

async def _calculate_comprehensive_stats_async(pool: AsyncConnectionPool, divisions: list[dict]) -> dict:
    """
    Calculate comprehensive statistics across all seasons and divisions.
    Returns detailed stats for display on the index page.
    """
    stats = {
        "current_season": CURRENT_SEASON,
        "previous_season": CURRENT_SEASON - 1,
        "total_divisions": 0,
        "total_regular_teams": 0,  # Only regular season teams
        "total_regular_players": 0,  # Only regular season players
        "total_matches_played": 0,
        "total_matches_total": 0,
        "total_maps_played": 0,
        "total_rounds_played": 0,
        "total_kills": 0,
        "total_deaths": 0,
        "season_progress": 0.0,
        "seasons_data": {},
        "active_divisions": 0,
        "playoff_divisions": 0,
    }
    
    # Group divisions by season for analysis
    by_season = {}
    for div in divisions:
        season = int(div.get("season", 0))
        by_season.setdefault(season, []).append(div)
    
    # Calculate stats for each season
    for season, season_divs in by_season.items():
        num_regular_divs = len([d for d in season_divs if not d.get("is_playoffs", 0)])
        num_playoff_divs = len([d for d in season_divs if d.get("is_playoffs", 0)])
        season_stats = {
            "divisions": num_regular_divs,
            "playoff_divisions": num_playoff_divs,
            "teams": 0,  # Unique teams in regular season only
            "players": 0,  # Unique players in regular season only
            "matches_played": 0,
            "matches_total": 0,
            "playoffs_matches_played": 0,
            # Estimate playoffs as 7 matches per regular division (ignore PO divisions)
            "playoffs_matches_total": num_regular_divs * 7,
            "regular_matches_played": 0,
            "regular_matches_total": 0,
            "maps_played": 0,
            "rounds_played": 0,
            "kills": 0,
            "deaths": 0,
            "completion_rate": 0.0,
            "playoffs_completion_rate": 0.0,
            "regular_completion_rate": 0.0
        }
        
        # Calculate unique teams and players for regular season only in this season
        regular_season_divs = [d for d in season_divs if not d.get("is_playoffs", 0)]
        if regular_season_divs:
            # Get unique teams from regular season divisions
            regular_championship_ids = [d["championship_id"] for d in regular_season_divs]
            placeholders = ','.join(['?' for _ in regular_championship_ids])
            
            teams_result = await query_async(pool, f"""
                SELECT COUNT(DISTINCT team_id) as team_count
                FROM (
                    SELECT team1_id as team_id FROM matches WHERE championship_id IN ({placeholders}) AND team1_id IS NOT NULL
                    UNION
                    SELECT team2_id as team_id FROM matches WHERE championship_id IN ({placeholders}) AND team2_id IS NOT NULL
                )
            """, regular_championship_ids + regular_championship_ids)
            season_stats["teams"] = int((teams_result[0]["team_count"] or 0)) if teams_result else 0
            
            # Get unique players from regular season divisions
            players_result = await query_async(pool, f"""
                SELECT COUNT(DISTINCT ps.player_id) as player_count
                FROM player_stats ps
                JOIN matches m ON ps.match_id = m.match_id
                WHERE m.championship_id IN ({placeholders}) AND ps.player_id IS NOT NULL
            """, regular_championship_ids)
            season_stats["players"] = int((players_result[0]["player_count"] or 0)) if players_result else 0
        
        # Calculate aggregated stats for this season
        for div in season_divs:
            teams, played, total = await _index_card_stats_async(pool, div["championship_id"])
            
            # Get map count for this division (exclude forfeits)
            maps_result = await query_async(pool, """
                SELECT COUNT(*) as map_count
                FROM maps mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.championship_id = ? 
                  AND m.is_forfeit = 0
                  AND mp.map_name IS NOT NULL 
                  AND mp.map_name != ''
            """, (div["championship_id"],))
            map_count = int((maps_result[0]["map_count"] or 0)) if maps_result else 0
            
            # Get kills, deaths, and rounds for this division
            stats_result = await query_async(pool, """
                SELECT 
                    SUM(ps.kills) as total_kills,
                    SUM(ps.deaths) as total_deaths
                FROM player_stats ps
                JOIN matches m ON ps.match_id = m.match_id
                WHERE m.championship_id = ?
            """, (div["championship_id"],))
            kills = int((stats_result[0]["total_kills"] or 0)) if stats_result else 0
            deaths = int((stats_result[0]["total_deaths"] or 0)) if stats_result else 0

            # Only count rounds for real played maps (exclude forfeits, include walkovers)
            rounds_result = await query_async(pool, """
                SELECT SUM(mp.score_team1 + mp.score_team2) as real_rounds
                FROM maps mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.championship_id = ?
                  AND m.is_forfeit = 0
                  AND mp.score_team1 IS NOT NULL 
                  AND mp.score_team2 IS NOT NULL
            """, (div["championship_id"],))
            rounds = int((rounds_result[0]["real_rounds"] or 0)) if rounds_result else 0
            
            # Add to season totals
            season_stats["maps_played"] += map_count
            season_stats["rounds_played"] += rounds
            season_stats["kills"] += kills
            season_stats["deaths"] += deaths
            
            # Always estimate playoff matches as 7 per division (8 teams, single-elimination)
            if div.get("is_playoffs", 0):
                season_stats["playoffs_matches_played"] += played
            else:
                season_stats["regular_matches_played"] += played
                season_stats["regular_matches_total"] += total

            season_stats["matches_played"] += played
            season_stats["matches_total"] += total
        
        # Calculate completion rates
        # Use estimated playoff matches for completion rate
        total_matches_estimated = season_stats["regular_matches_total"] + season_stats["playoffs_matches_total"]
        total_played = season_stats["regular_matches_played"] + season_stats["playoffs_matches_played"]
        if total_matches_estimated > 0:
            season_stats["completion_rate"] = (total_played / total_matches_estimated) * 100

        if season_stats["regular_matches_total"] > 0:
            season_stats["regular_completion_rate"] = (season_stats["regular_matches_played"] / season_stats["regular_matches_total"]) * 100

        if season_stats["playoffs_matches_total"] > 0:
            season_stats["playoffs_completion_rate"] = (season_stats["playoffs_matches_played"] / season_stats["playoffs_matches_total"]) * 100
        
        stats["seasons_data"][season] = season_stats
        
        # Add to totals (excluding playoffs for teams/players, but teams/players will be calculated globally later)
        stats["total_divisions"] += season_stats["divisions"]
        # NOTE: total_regular_teams and total_regular_players will be calculated globally to avoid double-counting
        stats["total_matches_played"] += season_stats["regular_matches_played"]  # Only regular season matches
        stats["total_matches_total"] += season_stats["regular_matches_total"]
        stats["total_maps_played"] += season_stats["maps_played"]
        stats["total_rounds_played"] += season_stats["rounds_played"]
        stats["total_kills"] += season_stats["kills"]
        stats["total_deaths"] += season_stats["deaths"]
        stats["playoff_divisions"] += season_stats["playoff_divisions"]
    
    # Calculate current season progress
    current_season_data = stats["seasons_data"].get(stats["current_season"], {})
    if current_season_data.get("matches_total", 0) > 0:
        stats["season_progress"] = current_season_data["completion_rate"]
    
    # Count active divisions (current season only)
    current_season_divs = by_season.get(stats["current_season"], [])
    stats["active_divisions"] = len([d for d in current_season_divs if not d.get("is_playoffs", 0)])
    
    # Calculate global unique teams and players across ALL regular seasons to avoid double-counting
    all_regular_championships = []
    for season, season_divs in by_season.items():
        regular_divs = [d for d in season_divs if not d.get("is_playoffs", 0)]
        all_regular_championships.extend([d["championship_id"] for d in regular_divs])
    
    if all_regular_championships:
        # Get unique teams across ALL regular season divisions
        placeholders = ','.join(['?' for _ in all_regular_championships])
        global_teams_result = await query_async(pool, f"""
            SELECT COUNT(DISTINCT team_id) as team_count
            FROM (
                SELECT team1_id as team_id FROM matches WHERE championship_id IN ({placeholders}) AND team1_id IS NOT NULL
                UNION
                SELECT team2_id as team_id FROM matches WHERE championship_id IN ({placeholders}) AND team2_id IS NOT NULL
            )
        """, all_regular_championships + all_regular_championships)
        stats["total_regular_teams"] = int((global_teams_result[0]["team_count"] or 0)) if global_teams_result else 0
        
        # Get unique players across ALL regular season divisions
        global_players_result = await query_async(pool, f"""
            SELECT COUNT(DISTINCT ps.player_id) as player_count
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE m.championship_id IN ({placeholders}) AND ps.player_id IS NOT NULL
        """, all_regular_championships)
        stats["total_regular_players"] = int((global_players_result[0]["player_count"] or 0)) if global_players_result else 0
    else:
        stats["total_regular_teams"] = 0
        stats["total_regular_players"] = 0
    
    return stats

## (legacy helper removed) maybe_render_index

async def render_index_pure_async(pool: AsyncConnectionPool, divisions: list[dict]) -> str:
    """
    Async version of render_index - pure async implementation.
    """
    # Calculate comprehensive statistics
    stats = await _calculate_comprehensive_stats_async(pool, divisions)
    
    # Group divisions by season
    by_season: dict[int, list[dict]] = {}
    for div in divisions:
        s = int(div.get("season") or 0)
        by_season.setdefault(s, []).append(div)

    html = []
    html.append(page_start("AFI - Pappaliiga — Index", "is-index"))
    html.append(topbar(show_back_to_index=False))

    # Enhanced Hero + container start
    html.append(f"""
    <div class="container">
        <!-- Enhanced Hero Section -->
        <section class="hero-enhanced">
            <div class="hero-card afi-card">
                <h1>Armafinland</h1>
                <p>
                    Yhteisö on avoin kaikille pelaajille ja ryhmille, jotka haluavat kokeilla taktista pelaamista myös Arma-sarjan peleissä. Pelaamme Arma 3 ja Arma Reforger, sekä järjestämme kansainvälisiä TvT-tehtäviä, joissa painotetaan realismia, joukkuepeliä ja yhteistoimintaa. Pelien ulkopuolella meno on rentoa ja mutkatonta, mutta pelissä otetaan tehtävät tosissaan.
                </p>
                <div class="hero-cta">
                    <a class="btn btn-primary" href="https://armafinland.fi/discord" title="Liity Armafinland Discordiin">Liity AFI Discord</a>
                    <a class="btn" href="https://armafinland.fi/" title="Lue lisää yhteisöstä">Lue lisää</a>
                </div>
            </div>

            <div class="hero-card pappa-card">
                <h1>Pappaliiga</h1>
                <p>
                    Pappaliigan tarkoituksena on tarjota varttuneemmalle väelle mahdollisuus kilpapelaamiseen; tosissaan ja `ei niin tosissaan`.
                </p>
                <div class="hero-cta">
                    <a class="btn btn-primary" href="https://discord.gg/qbySKpAYch" title="Liity Pappaliigan Discordiin">Liity Pappaliiga Discord</a>
                    <a class="btn" href="https://pappaliiga.fi/" title="Lue lisää">Lue lisää</a>
                </div>
            </div>
        </section>

        <!-- Combined All-Seasons Statistics -->
        <section class="stats-overview">
            <h2 class="section-title">Kaikki Kaudet Yhteensä</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-icon">🎲</div>
                    <div class="stat-value">{stats["total_divisions"]}</div>
                    <div class="stat-label">Divisioonaa</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">👥</div>
                    <div class="stat-value">{stats["total_regular_teams"]}</div>
                    <div class="stat-label">Joukkuetta</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">👤</div>
                    <div class="stat-value">{stats["total_regular_players"]}</div>
                    <div class="stat-label">Pelaajaa</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">⚔️</div>
                    <div class="stat-value">{stats["total_matches_played"]}</div>
                    <div class="stat-label">Ottelua Pelattu</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">🗺️</div>
                    <div class="stat-value">{stats["total_maps_played"]}</div>
                    <div class="stat-label">Karttaa Pelattu</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">🔄</div>
                    <div class="stat-value">{stats["total_rounds_played"]}</div>
                    <div class="stat-label">Kierrosta Pelattu</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">💀</div>
                    <div class="stat-value">{stats["total_kills"]}</div>
                    <div class="stat-label">Tappoja</div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">☠️</div>
                    <div class="stat-value">{stats["total_deaths"]}</div>
                    <div class="stat-label">Kuolemia</div>
                </div>
            </div>
        </section>

        <!-- Season Selector -->
        <section class="season-selector">
            <h2 class="section-title">Valitse Kausi</h2>
            <div class="season-tabs">""")
    
    # Generate season tabs
    for season in sorted(by_season.keys(), reverse=True):
        season_data = stats["seasons_data"].get(season, {})
        is_current = season == stats["current_season"]
        is_completed = season_data.get("completion_rate", 0) >= 95
        
        tab_classes = ["season-tab"]
        if is_current:
            tab_classes.append("active")
        if is_completed and not is_current:
            tab_classes.append("completed")
        
        html.append(f"""
                <div class="{' '.join(tab_classes)}" data-season="{season}">
                    Season {season}
                    {' (Käynnissä)' if is_current else ' (Loppunut)' if is_completed else ''}
                </div>""")
    
    # Get current season data for the dynamic season overview
    current_season_data = stats["seasons_data"].get(stats["current_season"], {})
    current_season = stats["current_season"]
    
    # Calculate percentages for progress bars
    regular_progress = current_season_data.get("regular_completion_rate", 0)
    playoffs_progress = current_season_data.get("playoffs_completion_rate", 0)
    overall_progress = current_season_data.get("completion_rate", 0)
    
    html.append(f"""
            </div>

            <!-- Dynamic Season Overview (JS will fill stats from data attributes) -->
            <div id="dynamic-season-overview" class="stats-overview" style="margin-top: 32px;">
                <h2 class="section-title" id="season-overview-title">Season {current_season} Yleiskatsaus</h2>
                <div class="season-stats" id="season-overview-stats">
                    <div class="stat-card">
                        <div class="stat-icon">🎲</div>
                        <div class="stat-value" id="overview-divisions"></div>
                        <div class="stat-label">Divisioonaa</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">👥</div>
                        <div class="stat-value" id="overview-teams"></div>
                        <div class="stat-label">Joukkuetta</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">👤</div>
                        <div class="stat-value" id="overview-players"></div>
                        <div class="stat-label">Pelaajaa</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">⚔️</div>
                        <div class="stat-value" id="overview-matches"></div>
                        <div class="stat-label">Ottelua Pelattu</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">🗺️</div>
                        <div class="stat-value" id="overview-maps"></div>
                        <div class="stat-label">Karttaa Pelattu</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">🔄</div>
                        <div class="stat-value" id="overview-rounds"></div>
                        <div class="stat-label">Kierrosta Pelattu</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">💀</div>
                        <div class="stat-value" id="overview-kills"></div>
                        <div class="stat-label">Tappoja</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">☠️</div>
                        <div class="stat-value" id="overview-deaths"></div>
                        <div class="stat-label">Kuolemia</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-icon">📊</div>
                        <div class="stat-value" id="overview-progress"></div>
                        <div class="season-progress">
                            <div class="season-progress-section">
                                <div class="season-progress-label">Runkosarja</div>
                                <div class="season-progress-bar">
                                    <div class="season-progress-fill" id="overview-regular-bar"></div>
                                </div>
                                <div class="season-progress-text" id="overview-regular-text"></div>
                            </div>
                            <div class="season-progress-section">
                                <div class="season-progress-label">Playoffs</div>
                                <div class="season-progress-bar">
                                    <div class="season-progress-fill" id="overview-playoffs-bar"></div>
                                </div>
                                <div class="season-progress-text" id="overview-playoffs-text"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Season Content Sections -->""")
    
    # Generate season content sections with enhanced statistics
    for season in sorted(by_season.keys(), reverse=True):
        season_data = stats["seasons_data"].get(season, {})
        is_current = season == stats["current_season"]
        content_classes = ["season-content"]
        if is_current:
            content_classes.append("active")
        
        progress_pct = season_data.get("completion_rate", 0)
        regular_progress = season_data.get("regular_completion_rate", 0)
        playoffs_progress = season_data.get("playoffs_completion_rate", 0)
        
        html.append(f"""
          <div class="{' '.join(content_classes)}" data-season-content="{season}" 
              data-divisions="{season_data.get('divisions', 0)}"
              data-teams="{season_data.get('teams', 0)}"
              data-players="{season_data.get('players', 0)}"
              data-matches-played="{season_data.get('matches_played', 0)}"
              data-matches-total="{season_data.get('matches_total', 0)}"
              data-maps-played="{season_data.get('maps_played', 0)}"
              data-rounds-played="{season_data.get('rounds_played', 0)}"
              data-kills="{season_data.get('kills', 0)}"
              data-deaths="{season_data.get('deaths', 0)}"
              data-playoff-divisions="{season_data.get('playoff_divisions', 0)}"
              data-playoffs-matches-played="{season_data.get('playoffs_matches_played', 0)}"
              data-playoffs-matches-total="{season_data.get('playoffs_matches_total', 0)}"
              data-progress="{progress_pct:.1f}"
              data-regular-progress="{regular_progress:.1f}"
              data-playoffs-progress="{playoffs_progress:.1f}">
                
                <div class="season-stats">
                    <!-- Removed duplicate per-season stat cards -->
                </div>
            </div>""")
    
    html.append("""
        </section>""")

    # Render divisions per season (updated structure with season containers)
    for season in sorted(by_season.keys(), reverse=True):
        # Sort divisions by division number in ascending order
        divs = sorted(by_season[season], key=lambda d: int(d.get("division_num") or 0))
        
        # Get season statistics
        season_data = stats["seasons_data"].get(season, {})
        season_completion = season_data.get("completion_rate", 0)
        is_current = season == stats["current_season"]
        
        # Season divisions container
        container_classes = ["season-divisions"]
        if is_current:
            container_classes.append("active")
        
        html.append(f"""
        <div class="{' '.join(container_classes)}" data-season-divisions="{season}">
            <h2 class="divisions-title">Season {season} Divisioonat <span class="season-meta">({season_data.get("divisions", 0)} divisioonaa, {season_completion:.0f}% pelattu)</span></h2>
            <div class="divisions-grid">""")
        
        for i, div in enumerate(divs):
            # Trim division name to remove season info
            raw_name = div.get("name", "Division")
            trimmed_name = re.sub(r"\s*S\d+$", "", raw_name).strip()
            title = esc_title(trimmed_name)
            slug = (div.get("slug") or "").strip()
            href = f"{slug}.html" if slug else "index.html"

            # --- async database calls ---
            ts_epoch = await get_division_generated_ts_async(pool, div["championship_id"])
            updated_str = format_ts(ts_epoch)  # returns '—' when None
            gen_tooltip = ''
            gen_note = ''
            try:
                if CURRENT_GEN_TS:
                    gen_tooltip = f' title="Generoitu: {format_ts(CURRENT_GEN_TS)}"'
                    gen_note = f'<div class="gen-note">(generoitu {format_ts(CURRENT_GEN_TS)})</div>'
            except NameError:
                gen_tooltip = ''
                gen_note = ''

            # Baseline stats for the summary card
            teams, played, total = await _index_card_stats_async(pool, div["championship_id"])

            # Calculate progress percentage
            progress_pct = int((played / total * 100)) if total > 0 else 0

            # Determine tier and icon based on division number
            div_num = int(div.get("division_num") or 0)
            is_playoffs = div.get("is_playoffs", 0)

            # First determine the base tier info for this division number
            if "master" in title.lower() or "mestaruus" in title.lower() or div_num == 0:
                base_tier_class = "tier-master"
                base_tier_icon = "👑"
            elif div_num == 1:
                base_tier_class = "tier-div1"
                base_tier_icon = "🥇"
            elif div_num == 2:
                base_tier_class = "tier-div2"
                base_tier_icon = "🥈"
            elif div_num == 3:
                base_tier_class = "tier-div3"
                base_tier_icon = "🥉"
            else:
                base_tier_class = "tier-regular"
                base_tier_icon = ""  # Regular tier has no icon

            # Apply playoff modifications if needed
            if is_playoffs:
                tier_class = "tier-playoffs"
                tier_icon = base_tier_icon  # Use same icon as the division
            else:
                tier_class = base_tier_class
                tier_icon = base_tier_icon

            # Add season indicator for older seasons
            season_indicator = ""
            is_finished = False
            if season != stats["current_season"]:
                if season_completion >= 95:
                    # Add generation time tooltip to finished badge
                    season_indicator = f'<div class="season-indicator completed"{gen_tooltip}>✓ Taputeltu loppuun</div>'
                    is_finished = True
                else:
                    season_indicator = f'<div class="season-indicator archived">📁 Arkistoitu</div>'

            # Only show tier badge if there's an icon
            tier_badge = f'<div class="tier-badge">{tier_icon}</div>' if tier_icon else ''

            html.append(f"""
            <a class="division-card {tier_class}" href="{href}" title="{title}">
                <div class="card-header">
                    {tier_badge}
                    <h3>{title}</h3>
                </div>
                <div class="card-stats">
                    <div class="stat-row">
                        <span class="stat-icon">👥</span>
                        <span>{teams} joukkuetta</span>
                    </div>
                    <div class="progress-section">
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {progress_pct}%"></div>
                        </div>
                        <span class="progress-text">{played}/{total} ottelua pelattu</span>
                    </div>
                </div>
                <div class="card-footer">
                    {season_indicator if is_finished else f'<div class="update-time"{gen_tooltip}>Data päivitetty {updated_str}</div>'}
                    {'' if is_finished else gen_note}
                </div>
            </a>
            """)
        html.append("</div>")  # /divisions-grid
        html.append("</div>")  # /season-divisions

    html.append("""
        <div class="footer">
            Made by Tuntematon & Cultti from Armafinland
        </div>
    </div> <!-- container -->
    
    <script>
    // Season switching functionality
    document.addEventListener('DOMContentLoaded', function() {
        const seasonTabs = document.querySelectorAll('.season-tab');
        const seasonDivisions = document.querySelectorAll('.season-divisions');
        const seasonContents = document.querySelectorAll('.season-content');
        
        // Elements to update in the dynamic overview
        const overviewTitle = document.getElementById('season-overview-title');
        const overviewDivisions = document.getElementById('overview-divisions');
        const overviewTeams = document.getElementById('overview-teams');
        const overviewPlayers = document.getElementById('overview-players');
        const overviewMatches = document.getElementById('overview-matches');
        const overviewMaps = document.getElementById('overview-maps');
        const overviewRounds = document.getElementById('overview-rounds');
        const overviewKills = document.getElementById('overview-kills');
        const overviewDeaths = document.getElementById('overview-deaths');
        const overviewProgress = document.getElementById('overview-progress');
        
        seasonTabs.forEach(tab => {
            tab.addEventListener('click', function() {
                const selectedSeason = this.getAttribute('data-season');
                
                // Update tab states
                seasonTabs.forEach(t => t.classList.remove('active'));
                this.classList.add('active');
                
                // Update division sections
                seasonDivisions.forEach(div => {
                    div.classList.remove('active');
                    if (div.getAttribute('data-season-divisions') === selectedSeason) {
                        div.classList.add('active');
                    }
                });
                
                // Find the corresponding season content data
                const seasonContent = document.querySelector(`[data-season-content="${selectedSeason}"]`);
                if (seasonContent && overviewTitle) {
                    // Update dynamic overview with the selected season's data
                    overviewTitle.textContent = `Season ${selectedSeason} Yleiskatsaus`;
                    
                    if (overviewDivisions) overviewDivisions.textContent = seasonContent.getAttribute('data-divisions') || '0';
                    if (overviewTeams) overviewTeams.textContent = seasonContent.getAttribute('data-teams') || '0';
                    if (overviewPlayers) overviewPlayers.textContent = seasonContent.getAttribute('data-players') || '0';
                    if (overviewMatches) overviewMatches.textContent = seasonContent.getAttribute('data-matches-played') || '0';
                    if (overviewMaps) overviewMaps.textContent = seasonContent.getAttribute('data-maps-played') || '0';
                    if (overviewRounds) overviewRounds.textContent = seasonContent.getAttribute('data-rounds-played') || '0';
                    if (overviewKills) overviewKills.textContent = seasonContent.getAttribute('data-kills') || '0';
                    if (overviewDeaths) overviewDeaths.textContent = seasonContent.getAttribute('data-deaths') || '0';
                    
                    // Update progress bar
                    const regularProgress = parseFloat(seasonContent.getAttribute('data-regular-progress') || '0');
                    const playoffsProgress = parseFloat(seasonContent.getAttribute('data-playoffs-progress') || '0');
                    const totalProgress = parseFloat(seasonContent.getAttribute('data-progress') || '0');
                    
                    if (overviewProgress) overviewProgress.textContent = Math.round(totalProgress) + '%';
                    
                    // Update progress bars
                    const regularProgressBar = document.querySelector('.season-progress .season-progress-section:first-child .season-progress-fill');
                    const playoffsProgressBar = document.querySelector('.season-progress .season-progress-section:last-child .season-progress-fill');
                    const regularProgressText = document.querySelector('.season-progress .season-progress-section:first-child .season-progress-text');
                    const playoffsProgressText = document.querySelector('.season-progress .season-progress-section:last-child .season-progress-text');
                    
                    if (regularProgressBar) regularProgressBar.style.width = regularProgress + '%';
                    if (playoffsProgressBar) playoffsProgressBar.style.width = playoffsProgress + '%';
                    
                    const matchesPlayed = seasonContent.getAttribute('data-matches-played') || '0';
                    const matchesTotal = seasonContent.getAttribute('data-matches-total') || '0';
                    const playoffDivisions = seasonContent.getAttribute('data-playoff-divisions') || '0';
                    
                    // Calculate regular season matches (approximate)
                    const regularMatchesTotal = parseInt(matchesTotal) - parseInt(playoffDivisions) * 7; // Assuming 7 playoff matches per division
                    const regularMatchesPlayed = Math.min(parseInt(matchesPlayed), regularMatchesTotal);
                    
                    if (regularProgressText) {
                        regularProgressText.textContent = `${regularMatchesPlayed} / ${regularMatchesTotal} ottelua`;
                    }
                    
                    if (playoffsProgressText) {
                        const playoffMatchesPlayed = parseInt(matchesPlayed) - regularMatchesPlayed;
                        const playoffMatchesTotal = parseInt(playoffDivisions) * 7;
                        playoffsProgressText.textContent = `${Math.max(0, playoffMatchesPlayed)} / ${playoffMatchesTotal} ottelua`;
                    }
                }
            });
        });
    });
    </script>
    """)

    html.append(page_end())
    return "\n".join(html)

# ------------------------------
# Async Rendering Functions
# ------------------------------
async def render_division_async(pool: AsyncConnectionPool, div: dict) -> None:
    """Pure async division renderer that generates division HTML files concurrently.

    Uses async team rendering helpers for true concurrency and performance.
    Preserves exact HTML structure while eliminating sync dependencies.
    """
    import time
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{div['slug']}.html"

    # Smarter skip: DB last_seen / GENVER
    do_render, reason = await should_render_division_async(pool, div, str(out_path))
    if not FORCE_REGEN:
        if do_render:
            print(f"[render] {out_path} ({reason})")
        else:
            print(f"[skip] {out_path} ({reason})")
            return

    start = time.perf_counter()
    if FORCE_REGEN:
        print(f"[timer] START division {div['slug']} at {start:.2f}s (force render - force param set)")
    else:
        print(f"[timer] START division {div['slug']} at {start:.2f}s")
    # Pure async rendering using new team helpers
    await _render_division_pure_async(pool, div)
    end = time.perf_counter()
    print(f"[timer] END division {div['slug']} at {end:.2f}s (duration: {end-start:.2f}s)")

async def _render_division_pure_async(pool: AsyncConnectionPool, div: dict) -> Path:
    """
    Core async division rendering logic using team helpers for concurrency.
    """
    # Import async team rendering functions
    from async_db import render_team_card_async
    
    overrides = _DIVISION_OVERRIDES
    banned_entries = banned_teams_for_division(div["championship_id"], overrides)
    banned_ids = {entry.get("team_id") for entry in banned_entries if entry.get("team_id")}

    # Get teams and thresholds concurrently
    teams_task = get_teams_in_championship_async(pool, div["championship_id"])
    thresholds_task = compute_champ_thresholds_data_async(pool, div["championship_id"], banned_ids)
    map_avgs_task = compute_champ_map_avgs_data_async(pool, div["championship_id"], banned_ids)
    div_summary_task = compute_champ_player_summary_async(
        pool, div["championship_id"], min_rounds=20, excluded_team_ids=banned_ids
    )
    map_summary_task = compute_champ_map_summary_data_async(pool, div["championship_id"], banned_ids)
    timestamp_task = get_division_generated_ts_async(pool, div["championship_id"])

    teams, thresholds, div_avgs, div_summary, map_summary, ts_epoch = await asyncio.gather(
        teams_task, thresholds_task, map_avgs_task, div_summary_task, map_summary_task, timestamp_task
    )

    banned_lookup = {item["team_id"]: item for item in banned_entries}

    augmented: list[dict] = []
    existing_ids: set[str] = set()
    for team in teams:
        team_id = team.get("team_id")
        if team_id:
            existing_ids.add(team_id)
        info = banned_lookup.get(team_id)
        if info:
            team = dict(team)
            team["is_banned"] = True
            team["ban_reason"] = info.get("reason")
            team["banned_at"] = info.get("banned_at")
            team["ban_note"] = info.get("note")
            if info.get("team_name") and not (team.get("team_name")):
                team["team_name"] = info["team_name"]
            if info.get("avatar") and not team.get("avatar"):
                team["avatar"] = info["avatar"]
        augmented.append(team)

    for info in banned_entries:
        tid = info.get("team_id")
        if tid and tid not in existing_ids:
            augmented.append({
                "team_id": tid,
                "team_name": info.get("team_name") or tid,
                "avatar": info.get("avatar") or "",
                "is_banned": True,
                "ban_reason": info.get("reason"),
                "banned_at": info.get("banned_at"),
                "ban_note": info.get("note"),
            })

    teams = sorted(
        augmented,
        key=lambda t: (t.get("team_name") or t.get("team_id") or "").lower()
    )

    for team in teams:
        base_name = team.get("team_name") or team.get("team_id") or "-"
        if team.get("is_banned"):
            team["display_name"] = f"{base_name} (BANNED)"
        else:
            team["display_name"] = base_name

    div_summary["teams"] = len({t.get("team_id") for t in teams if t.get("team_id")})

    # Format timestamp
    ts_str = format_ts(ts_epoch) if ts_epoch else "—"
    
    # Start building HTML
    html = []
    title = f"{esc_title(div['name'])} (Season {div['season']}) — Pappaliiga Stats"
    html.append(page_start(title, "is-division", last_match_ts=ts_epoch))
    html.append(topbar(show_back_to_index=True))

    html.append('<div class="container">')
    # Integrated centered division header
    html.append('<section class="division-header">')
    html.append('  <div class="division-title-block">')
    
    # Format division name more cleanly
    div_display_name = div["name"]
    if "Divisioona" in div_display_name:
        # Convert "2 Divisioona S10" to "Division 2" 
        import re
        match = re.match(r'(\d+)\s+Divisioona\s+S(\d+)', div_display_name)
        if match:
            div_num, season_num = match.groups()
            div_display_name = f"Division {div_num}"
    
    html.append(f'    <h1 class="division-name">{esc_title(div_display_name)} <span class="season">(Season {div["season"]})</span></h1>')
    html.append(f'    <div class="division-meta">Data päivitetty {ts_str}</div>')
    html.append('  </div>')
    html.append('</section>')
    html.append('<div class="page">')

    # Navigation
    html.append('<h2 class="section-title">Joukkueet</h2>')
    html.append('<div class="nav">')
    for t in teams:
        name = t.get("display_name") or t.get("team_name") or t.get("team_id") or "-"
        avatar = t.get("avatar")
        logo = f'<img class="logo nav-logo" src="{avatar}" alt="{escape(name)}" loading="lazy">' if avatar else ''
        css_classes = []
        if t.get("is_banned"):
            css_classes.append("is-banned")
        class_attr = f' class="{" ".join(css_classes)}"' if css_classes else ""
        html.append(
            f'<a href="#team-{t["team_id"]}"{class_attr}>{logo}'
            f'<span class="nav-name">{escape(name)}</span></a>'
        )
    html.append("</div>")

    # Division summary section
    try:
        summary_html = await _render_division_summary_async(pool, div, div_summary, map_summary, teams)
        html.extend(summary_html)
    except Exception as e:
        print(f"ERROR in _render_division_summary_async: {e}")
        import traceback
        traceback.print_exc()

    # Teams grid wrapper
    html.append('<div class="teams-grid">')

    # Render all team cards concurrently - this is where we get the performance boost!
    team_tasks = []
    for i, team in enumerate(teams, start=1):
        task = render_team_card_async(pool, div, team, i, teams, thresholds, div_avgs, banned_ids)
        team_tasks.append(task)
    
    # Wait for all team cards and add to HTML
    if team_tasks:
        team_htmls = await asyncio.gather(*team_tasks)
        for team_html in team_htmls:
            html.extend(team_html)

    # Close containers
    html.append('</div>')  # /teams-grid
    html.append('</div>')  # .page
    html.append('</div>')  # .container
    html.append(page_end())

    # Write output file
    out_path = OUT_DIR / f"{div['slug']}.html"
    html_str = "\n".join(html)
    await write_if_changed_async(out_path, html_str)
    
    return out_path

async def _render_division_summary_async(pool: AsyncConnectionPool, div: dict, div_summary: dict, map_summary: dict, teams: list[dict]) -> list[str]:
    """
    Render the division summary section with stats and leaders.
    """
    from async_db import map_pretty_name_async, get_map_art_async
    
    html = []
    
    TOOLTIP_WMED = ("Painotettu mediaani: pelaajakohtaiset arvot lajitellaan, "
                    "paino = pelatut kierrokset divisioonassa. p50 on pienin arvo, "
                    "jossa kumulatiiviset painot ylittävät 50% (p25/p75 vastaavasti 25%/75%).")

    html.append('<div class="div-summary">')

    # Container for equal height sections
    html.append('<div class="stats-sections-container">')

    # Modern stat cards section (moved into container for alignment)
    html.append('<section class="stats-overview division-stats">')
    html.append('<h2 class="section-title">Divisioona Tilastot</h2>')
    html.append('<div class="stats-grid">')
    
    # Basic stats with icons
    html.append('<div class="stat-card">')
    html.append('<div class="stat-icon">👥</div>')
    html.append(f'<div class="stat-value">{div_summary["teams"]}</div>')
    html.append('<div class="stat-label">Joukkuetta</div>')
    html.append('</div>')

    html.append('<div class="stat-card">')
    html.append('<div class="stat-icon">👤</div>')
    html.append(f'<div class="stat-value">{div_summary["players"]}</div>')
    html.append('<div class="stat-label">Pelaajaa</div>')
    html.append('</div>')

    html.append('<div class="stat-card">')
    html.append('<div class="stat-icon">🗺️</div>')
    html.append(f'<div class="stat-value">{div_summary["maps"]}</div>')
    html.append('<div class="stat-label">Karttaa Pelattu</div>')
    html.append('</div>')

    html.append('<div class="stat-card">')
    html.append('<div class="stat-icon">🎯</div>')
    html.append(f'<div class="stat-value">{div_summary["rounds"]}</div>')
    html.append('<div class="stat-label">Karttaa Pelattu</div>')
    html.append('</div>')

    html.append('<div class="stat-card">')
    html.append('<div class="stat-icon">🎯</div>')
    html.append(f'<div class="stat-value">{div_summary["rounds"]}</div>')
    html.append('<div class="stat-label">Erää Pelattu</div>')
    html.append('</div>')


    html.append(f'<div class="stat-card performance" title="{TOOLTIP_WMED}">')
    html.append('<div class="stat-icon">💥</div>')
    html.append(f'<div class="stat-value">{div_summary["adr_p50"]:.1f}</div>')
    html.append('<div class="stat-label">Median ADR</div>')
    html.append(f'<div class="stat-range">{div_summary["adr_p25"]:.1f}-{div_summary["adr_p75"]:.1f}</div>')
    html.append('</div>')

    # Missing stats that were in the original
    html.append(f'<div class="stat-card performance" title="{TOOLTIP_WMED}">')
    html.append('<div class="stat-icon">⚡</div>')
    html.append(f'<div class="stat-value">{div_summary["kr_p50"]:.2f}</div>')
    html.append('<div class="stat-label">Median K/R</div>')
    html.append(f'<div class="stat-range">{div_summary["kr_p25"]:.2f}-{div_summary["kr_p75"]:.2f}</div>')
    html.append('</div>')

    html.append(f'<div class="stat-card performance" title="{TOOLTIP_WMED}">')
    html.append('<div class="stat-icon">🛡️</div>')
    html.append(f'<div class="stat-value">{div_summary["surv_p50"]:.0f}%</div>')
    html.append('<div class="stat-label">Median Survival</div>')
    html.append(f'<div class="stat-range">{div_summary["surv_p25"]:.0f}%-{div_summary["surv_p75"]:.0f}%</div>')
    html.append('</div>')

    # html.append(f'<div class="stat-card performance" title="{esc_title(TOOLTIP_RATING1)}">')
    # html.append('<div class="stat-icon">⭐</div>')
    # html.append(f'<div class="stat-value">{div_summary["r1_p50"]:.2f}</div>')
    # html.append('<div class="stat-label">Median Rating1</div>')
    # html.append(f'<div class="stat-range">{div_summary["r1_p25"]:.2f}-{div_summary["r1_p75"]:.2f}</div>')
    # html.append('</div>')
    
    html.append('</div>')  # /stats-grid
    html.append('</section>')  # /stats-overview

    # Query the actual map pool for this championship/season
    pool_rows = await query_async(pool, """
        SELECT DISTINCT mp.map_name AS map_id
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ?
            AND mp.map_name IS NOT NULL AND mp.map_name <> ''
            AND m.is_forfeit = 0
    """, (div["championship_id"],))
    season_maps = [r["map_id"] for r in pool_rows]
    if not season_maps:
        # fallback to all known maps if pool is empty (should not happen)
        season_maps = ["de_nuke", "de_inferno", "de_mirage", "de_overpass", "de_dust2", "de_ancient", "de_train", "de_anubis"]

    # Create dictionaries for played and banned counts (by map_id)
    played_counts = {map_name: count for map_name, count in map_summary["top_played"]}
    banned_counts = {map_name: count for map_name, count in map_summary["top_banned"]}

    # Get rounds played per map for this championship
    rounds_rows = await query_async(pool, """
        SELECT mp.map_name, SUM(mp.score_team1 + mp.score_team2) as rounds_played
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ?
            AND mp.map_name IS NOT NULL AND mp.map_name <> ''
            AND m.is_forfeit = 0
        GROUP BY mp.map_name
    """, (div["championship_id"],))
    map_rounds = {r["map_name"]: r["rounds_played"] for r in rounds_rows}

    # Build combined data: only use maps in the season's map pool
    combined_data = []
    for map_id in season_maps:
        played = played_counts.get(map_id, 0)
        banned = banned_counts.get(map_id, 0)
        rounds = map_rounds.get(map_id, 0)
        map_art = await get_map_art_async(pool, map_id)
        pretty = map_id
        img_url = ''
        if map_art:
            pretty = map_art.get('pretty_name', map_id)
            img_url = map_art.get('image_sm')
        img_html = f'<img class="map-img-sm" src="{img_url}" alt="{pretty}" loading="lazy">' if img_url else ''
        combined_data.append((map_id, pretty, img_html, played, banned, rounds))

    # Sort by played count descending, then by name ascending
    combined_data.sort(key=lambda x: (-x[3], x[1]))

    # Render map stats as a sortable table with image, name, played, banned, rounds columns
    html.append('<section class="stats-overview map-stats-section">')
    html.append('<h2 class="section-title">Divisioonan Kartta Tilastot</h2>')
    html.append('<div class="map-stats-grid">')
    html.append('<div class="map-table-container">')
    html.append('<table id="maps-table" class="map-table sortable" data-sort-col="1" data-sort-dir="desc">')
    html.append('<thead><tr>'
                '<th data-sortable onclick="sortTable(\'maps-table\',0,false)">Kartta</th>'
                '<th data-sortable onclick="sortTable(\'maps-table\',1,true)">Pelattu</th>'
                '<th data-sortable onclick="sortTable(\'maps-table\',2,true)">Bannattu</th>'
                '<th data-sortable onclick="sortTable(\'maps-table\',3,true)">Erää pelattu</th>'
                '</tr></thead>')
    html.append('<tbody>')
    for map_id, pretty, img_html, played, banned, rounds in combined_data:
        html.append('<tr>')
        html.append(f'<td style="display:flex;align-items:center;gap:10px;">{img_html}<span>{pretty}</span></td>')
        html.append(f'<td>{played}</td>')
        html.append(f'<td>{banned}</td>')
        html.append(f'<td>{rounds}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')
    html.append('</div>')  # /map-table-container
    html.append('</div>')  # /map-stats-grid
    html.append('</section>')  # /map-stats-section
    
    html.append('</div>')  # /stats-sections-container
    html.append('</div>')  # /div-summary
    
    # Modern Leaders section - Full width with comprehensive data
    html.append('<section class="leaders-section-fullwidth">')
    html.append('<div class="leaders-container">')
    html.append('<h2 class="section-title">Johtajat <span class="section-subtitle">(min 40 erää, paitsi kokonaismäärät)</span></h2>')
    
    leaders = div_summary["leaders"]
    
    # Create comprehensive leader data with all requested statistics
    leader_categories = [
        # Volume & Core Performance
        {
            "title": "Volyymi & Perustilastot",
            "icon": "",
            "stats": [
                ("most_rounds_played", "", "Most Rounds", "kierroksia", int(leaders["most_rounds_played"][2])),
                ("kd", "�", "Best K/D", "K/D suhde", f"{leaders['kd'][2]:.2f}"),
                ("adr", "�", "Best ADR", "keskivahingot", f"{leaders['adr'][2]:.1f}"),
                ("kr", "⚡", "Best K/R", "kills/round", f"{leaders['kr'][2]:.2f}"),
                ("most_total_damage", "💣", "Most Total Damage", "kokonaisvahingot", int(leaders["most_total_damage"][2])),
            ]
        },
        # Frags & Kills
        {
            "title": "Tapot & Kuolemat", 
            "icon": "",
            "stats": [
                ("top_frg_total", "�", "Most Kills", "kokonaiskills", int(leaders["top_frg_total"][2])),
                ("most_deaths_total", "�", "Most Deaths", "kuolemat", int(leaders["most_deaths_total"][2])),
                ("most_assists_total", "🤝", "Most Assists", "assistit", int(leaders["most_assists_total"][2])),
                ("hs_pct", "🎯", "Best HS%", "pääshotit %", f"{leaders['hs_pct'][2]:.1f}%"),
                ("most_mvps", "🏆", "Most MVPs", "MVP:t", int(leaders["most_mvps"][2])),
            ]
        },
        # Clutch & Special Situations
        {
            "title": "Clutch & Tilanteet",
            "icon": "�",
            "stats": [
                ("most_clutch_kills", "�", "Most Clutch Kills", "clutch tapot", int(leaders["most_clutch_kills"][2])),
                ("clutch_wr", "🎯", "Best Clutch WR", "clutch %", f"{leaders['clutch_wr'][2]:.1f}%"),
                ("udpr", "🔧", "Most Utility Damage", "UDPR", f"{leaders['udpr'][2]:.2f}"),
                ("survival_rate", "🛡️", "Best Survival%", "survival %", f"{leaders['survival_rate'][2]:.1f}%"),
                ("rating1", "⭐", "Best Rating1", "rating", f"{leaders['rating1'][2]:.2f}"),
            ]
        },
        # Equipment & Special Kills
        {
            "title": "Erikoisaseet & Flash",
            "icon": "�",
            "stats": [
                ("most_enemies_flashed", "💡", "Most Flashed", "sokaistuja", int(leaders["most_enemies_flashed"][2])),
                ("enemies_per_flash", "⚡", "Best Enemy/Flash", "sokaisu/flash", f"{leaders['enemies_per_flash'][2]:.2f}"),
                ("most_pistol_kills", "🔫", "Most Pistol Kills", "pistolitapot", int(leaders["most_pistol_kills"][2])),
                ("most_sniper_kills", "�", "Most Sniper Kills", "kiikaritapot", int(leaders["most_sniper_kills"][2])),
            ]
        }
    ]
    
    # Render each category
    for category in leader_categories:
        html.append('<div class="leader-category">')
        html.append('<div class="category-header">')
        html.append(f'<h3 class="category-title">{category["title"]}</h3>')
        html.append('</div>')
        html.append('<div class="leaders-row">')
        
        for stat_key, icon, title, subtitle, value in category["stats"]:
            if stat_key in leaders:
                # Always render cards, but show placeholder data if no valid leader
                if leaders[stat_key][0] != "-":
                    player_name = escape(leaders[stat_key][0])
                    team_name = escape(leaders[stat_key][1])
                    display_value = value
                else:
                    player_name = "No data"
                    team_name = "N/A"
                    display_value = "-"
                
                # Find team logo
                team_logo = ""
                if leaders[stat_key][0] != "-":
                    for team in teams:
                        if team.get("team_name") == leaders[stat_key][1] or team.get("team_id") == leaders[stat_key][1]:
                            if team.get("avatar"):
                                team_logo = f'<img class="logo leader-team-logo" src="{team["avatar"]}" alt="{team_name}" loading="lazy">'
                            break
                
                # Create detailed tooltip with breakdown
                tooltip_parts = [f"Player: {player_name}", f"Team: {team_name}", f"Value: {value}"]
                if stat_key == "most_rounds_played":
                    tooltip_parts.append(f"Total rounds played in division {div['name']}")
                elif stat_key == "kd":
                    tooltip_parts.append("Kill/Death ratio (min 40 rounds)")
                elif stat_key == "adr":
                    tooltip_parts.append("Average damage per round")
                elif stat_key == "kr":
                    tooltip_parts.append("Kills per round")
                elif stat_key == "most_total_damage":
                    tooltip_parts.append("Total damage dealt across all matches")
                elif stat_key == "top_frg_total":
                    tooltip_parts.append("Total kills across all matches")
                elif stat_key == "most_deaths_total":
                    tooltip_parts.append("Total deaths across all matches")
                elif stat_key == "most_assists_total":
                    tooltip_parts.append("Total assists across all matches")
                elif stat_key == "hs_pct":
                    tooltip_parts.append("Headshot percentage")
                elif stat_key == "most_mvps":
                    tooltip_parts.append("Most Valuable Player awards")
                elif stat_key == "most_clutch_kills":
                    tooltip_parts.append("Kills in clutch situations (1vX)")
                elif stat_key == "clutch_wr":
                    tooltip_parts.append("Clutch win rate percentage")
                elif stat_key == "udpr":
                    tooltip_parts.append("Utility damage per round")
                elif stat_key == "survival_rate":
                    tooltip_parts.append("Percentage of rounds survived")
                elif stat_key == "rating1":
                    tooltip_parts.append("HLTV 1.0 style rating")
                elif stat_key == "most_enemies_flashed":
                    tooltip_parts.append("Total enemies blinded with flashbangs")
                elif stat_key == "enemies_per_flash":
                    tooltip_parts.append("Average enemies blinded per flashbang thrown")
                elif stat_key == "most_pistol_kills":
                    tooltip_parts.append("Kills with pistol weapons")
                elif stat_key == "most_sniper_kills":
                    tooltip_parts.append("Kills with sniper rifles (AWP, Scout, etc.)")
                
                tooltip_text = " | ".join(tooltip_parts)
                
                html.append(f'<div class="leader-card modern" title="{escape(tooltip_text)}">')
                html.append('<div class="leader-header">')
                html.append('<div class="leader-title-group">')
                html.append(f'<h4 class="leader-title">{title}</h4>')
                if subtitle:
                    html.append(f'<span class="leader-subtitle">{subtitle}</span>')
                html.append('</div>')
                html.append('</div>')
                html.append('<div class="leader-info">')
                html.append('<div class="leader-player-row">')
                html.append(team_logo)
                html.append(f'<div class="leader-player">{player_name}</div>')
                html.append('</div>')
                html.append(f'<div class="leader-team">{team_name}</div>')
                html.append(f'<div class="leader-value">{display_value}</div>')
                html.append('</div>')
                html.append('</div>')
        
        html.append('</div>')  # /leaders-row
        html.append('</div>')  # /leader-category
    
    html.append('</div>')  # /leaders-container
    html.append('</section>')  # /leaders-section-fullwidth
    
    return html

async def should_render_division_async(pool: AsyncConnectionPool, div: dict, out_path: str) -> tuple[bool, str]:
    """Async decision for division regeneration, including GENVER check."""
    p = Path(out_path)
    if not p.exists():
        return True, "html missing"

    try:
        mtime = int(p.stat().st_mtime)
    except OSError:
        return True, "stat error"

    # 1) DB timestamp guard
    db_ts = await get_division_generated_ts_async(pool, div["championship_id"])
    if db_ts is None:
        return True, "no DB timestamp"
    if db_ts > mtime:
        return True, f"db last_seen {int(db_ts)} > html mtime {int(mtime)}"

    # 2) Template version guard
    embedded_ver = _read_embedded_version(out_path)
    if embedded_ver < HTML_TEMPLATE_VERSION:
        return True, f"template version bump {HTML_TEMPLATE_VERSION} (was {embedded_ver})"

    # 3) LAST_MATCH guard from embedded meta (if present)
    embedded_meta = _read_embedded_meta(out_path)
    embedded_last = embedded_meta[2] if embedded_meta else 0
    if embedded_last and embedded_last > mtime:
        return True, f"embedded last_match {int(embedded_last)} > html mtime {int(mtime)}"

    return False, f"(html mtime {int(mtime)} >= last_seen {int(db_ts)} and ver={embedded_ver} last_match={embedded_last})"

async def render_team_summary_async(pool: AsyncConnectionPool, team: dict, div: dict, div_avgs: dict, thresholds: dict) -> list[str]:
    """Async version that renders a team summary section"""
    team_id = team["team_id"]
    
    # Get team data asynchronously
    team_data = await compute_team_summary_data_async(pool, team_id, div["championship_id"])
    players = await compute_player_table_data_async(pool, div["championship_id"], team_id)
    player_deltas = await compute_player_deltas_async(pool, div["championship_id"], team_id)
    
    # For now, render the team using a simplified version of sync logic
    # This is a placeholder - in practice, we'd extract the complex team rendering
    # logic into separate functions that can be called both sync and async
    html = []
    html.append(f'<div class="team-summary" id="team-{team_id}">')
    
    # Team header
    name = team["team_name"] or team["team_id"]
    avatar = team.get("avatar")
    logo = f'<img class="logo team-logo" src="{avatar}" alt="">' if avatar else ''
    html.append(f'<h2 class="team-name">{logo}{escape(name)}</h2>')
    
    # Team stats summary
    html.append('<div class="team-stats">')
    html.append(f'<div>Matches: {team_data.get("matches_played", 0)}</div>')
    html.append(f'<div>W-L: {team_data.get("w", 0)}-{team_data.get("l", 0)}</div>')
    html.append(f'<div>KD: {team_data.get("kd", 0.0):.2f}</div>')
    html.append(f'<div>ADR: {team_data.get("adr", 0.0):.1f}</div>')
    html.append('</div>')
    
    # Player count
    html.append(f'<div>Players: {len(players)}</div>')
    
    html.append("</div>")
    return html

async def generate_all_async(force_regenerate: bool = False, division_filter: Optional[int] = None) -> None:
    """Async version of main HTML generation function (only mode)."""
    import time
    print("Starting async HTML generation...")
    total_start = time.perf_counter()

    # Initialize async database pool
    pool = AsyncConnectionPool(DB_PATH)
    await pool.initialize()

    try:
        # Use the DIVISIONS constant from faceit_config (already imported at top)
        all_divisions = DIVISIONS
        divisions_to_render = DIVISIONS
        
        # Apply division filter if specified
        if division_filter is not None:
            all_divisions = [div for div in all_divisions if div.get("division_num") == division_filter]
            divisions_to_render = all_divisions
            if not all_divisions:
                print(f"No division found with number {division_filter}")
                return
        
        # Default: current season only for rendering (unless --all-seasons is specified)
        # But index page always shows all seasons from all_divisions
        if not getattr(args, 'all_seasons', False):
            current_season = CURRENT_SEASON
            divisions_to_render = [div for div in divisions_to_render if int(div.get("season", 0)) == current_season]
            print(f"[default] Rendering current season only: Season {current_season} ({len(divisions_to_render)} divisions)")
        else:
            print(f"[all-seasons] Rendering all seasons ({len(divisions_to_render)} divisions)")
        
        print(f"Found {len(divisions_to_render)} divisions to render, {len(all_divisions)} total in index")

        # Copy static files
        await copy_static_files_async()

        # Generate division pages concurrently for performance
        async def _safe_render(div: dict) -> tuple[str, Exception | None]:
            try:
                await render_division_async(pool, div)
                return div.get('slug', 'unknown'), None
            except Exception as e:
                return div.get('slug', 'unknown'), e

        tasks = [ _safe_render(div) for div in divisions_to_render ]

        # Wait for all divisions to complete, collect errors instead of failing fast
        results = await asyncio.gather(*tasks, return_exceptions=False)
        failures = [(slug, err) for slug, err in results if err is not None]
        if failures:
            print(f"[errors] {len(failures)} division(s) failed:")
            for slug, err in failures:
                print(f"  - {slug}: {type(err).__name__}: {err}")

        # Generate index page (always with all divisions for complete season/division listings)
        await render_index_async(pool, all_divisions)

    finally:
        total_end = time.perf_counter()
        print(f"[timer] TOTAL generation time: {total_end-total_start:.2f}s")
        print("Async HTML generation completed!")
        await pool.close_all()

async def render_index_async(pool: AsyncConnectionPool, divisions: list[dict]) -> None:
    """Fully async index rendering"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    html = await render_index_pure_async(pool, divisions)
    
    idx_path = OUT_DIR / "index.html"
    did_write = await write_if_changed_async(idx_path, html)
    status = "OK] Wrote" if did_write else "skip ]"
    print(f"[{status} {idx_path}")

# ------------------------------
# Rendering
# ------------------------------

# --- Content-aware write helpers -------------------------------------------

# Capture Finnish and English timestamp phrases for comparison safety.
_TS_PATTERNS = [
    r"Generoitu\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?",   # "Generated 2025-09-06 15:27" (with optional seconds)
    r"\(Generoitu\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?\)", # "(Generated ...)"
    r"Generated\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?",   # future-proof if the English token appears
]

# Build/noise markers to exclude from comparisons,
# e.g. <link href="app.css?b=abcdef1"> or data-build="abcdef1"
_BUILD_PATTERNS = [
    r"\?b=[a-f0-9]{7,}",                   # query-param build hash
    r"data-build=[\"'][a-f0-9]{7,}[\"']",  # data-build attribuutti
]

# Generic ISO timestamp remover as a safeguard (e.g., metadata or comments)
_ISO_TS_ANYWHERE = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?"

def _to_unix_newlines(s: str) -> str:
    # Normalize line endings: CRLF/LF -> LF (preventing repeated writes on Windows)
    return s.replace("\r\n", "\n").replace("\r", "\n")

def _normalize_for_compare_bytes(b: bytes) -> bytes:
    # Decode, normalize newlines, and strip dynamic fragments before comparing
    s = b.decode("utf-8", errors="ignore")
    s = _to_unix_newlines(s)

    # Remove timestamp strings
    for pat in _TS_PATTERNS:
        s = re.sub(pat, "GENERATED_TS", s, flags=re.IGNORECASE)

    # Remove stray ISO timestamps just in case (e.g., in comments)
    s = re.sub(_ISO_TS_ANYWHERE, "GENERATED_TS", s)

    # Remove build/noise markers
    for pat in _BUILD_PATTERNS:
        s = re.sub(pat, "", s, flags=re.IGNORECASE)

    # (Optional) Trim trailing whitespace so editor changes do not affect comparisons
    s = "\n".join(line.rstrip() for line in s.split("\n"))

    return s.encode("utf-8", errors="ignore")

def write_if_changed(path: "Path", content: str) -> bool:
    """
    Write to 'path' only when the normalized content differs from the existing file.
    Returns True if the file was written, False if it was skipped.
    """
    # If forced regeneration is requested, always write regardless of content
    if FORCE_REGEN:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("wb", delete=False, dir=str(path.parent)) as tf:
            tf.write(content.encode("utf-8"))
            tmp_name = tf.name
        os.replace(tmp_name, path)
        return True
    new_bytes = _normalize_for_compare_bytes(_to_unix_newlines(content).encode("utf-8"))

    try:
        old_raw = path.read_bytes()
        old_bytes = _normalize_for_compare_bytes(old_raw)
        if hashlib.sha256(old_bytes).digest() == hashlib.sha256(new_bytes).digest():
            return False  # No changes detected
    except FileNotFoundError:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write with Windows compatibility: write to a temp file and swap it in
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=str(path.parent)) as tf:
        tf.write(content.encode("utf-8"))
        tmp_name = tf.name
    os.replace(tmp_name, path)
    return True

async def write_if_changed_async(path: "Path", content: str) -> bool:
    """
    Async version of write_if_changed using aiofiles.
    Write to 'path' only when the normalized content differs.
    Returns True if the file was written, False if skipped.
    """
    # If forced regeneration is requested, always write regardless of content
    if FORCE_REGEN:
        await aiofiles.os.makedirs(path.parent, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + '.tmp')
        try:
            async with aiofiles.open(tmp_path, 'wb') as f:
                await f.write(content.encode("utf-8"))
            os.replace(tmp_path, path)
            return True
        except Exception:
            try:
                os.unlink(tmp_path)
            except:
                pass
            raise
    new_bytes = _normalize_for_compare_bytes(_to_unix_newlines(content).encode("utf-8"))

    try:
        # Use aiofiles for async file reading
        async with aiofiles.open(path, 'rb') as f:
            old_raw = await f.read()
        old_bytes = _normalize_for_compare_bytes(old_raw)
        if hashlib.sha256(old_bytes).digest() == hashlib.sha256(new_bytes).digest():
            return False  # Ei muutosta
    except FileNotFoundError:
        pass

    # Ensure parent directory exists
    await aiofiles.os.makedirs(path.parent, exist_ok=True)
    
    # Atomic write using aiofiles
    tmp_path = path.with_suffix(path.suffix + '.tmp')
    try:
        async with aiofiles.open(tmp_path, 'wb') as f:
            await f.write(content.encode("utf-8"))
        
        # Atomic replace - this is still sync but very fast
        os.replace(tmp_path, path)
        return True
    except Exception:
        # Clean up temp file if something went wrong
        try:
            os.unlink(tmp_path)
        except:
            pass
        raise

## (legacy helper removed) copy_static_files

async def copy_static_files_async():
    """Async version of copy_static_files using aiofiles."""
    web_static_dir = Path("web_static")
    docs_dir = Path("docs")
    
    # Check if source directory exists
    if not web_static_dir.exists():
        print(f"[static] Warning: {web_static_dir} directory not found")
        return
    
    # Ensure docs directory exists
    await aiofiles.os.makedirs(docs_dir, exist_ok=True)
    
    # Copy files concurrently
    copy_tasks = []
    for file_name in ["styles.css", "app.js"]:
        src = web_static_dir / file_name
        dst = docs_dir / file_name
        if src.exists():
            copy_tasks.append(_copy_file_async(src, dst))
        else:
            print(f"[static] Warning: {src} not found")
    
    if copy_tasks:
        await asyncio.gather(*copy_tasks)

async def _copy_file_async(src: Path, dst: Path):
    """Helper function to copy a single file asynchronously with content comparison"""
    try:
        # Read source content
        async with aiofiles.open(src, 'rb') as src_file:
            src_content = await src_file.read()
        
        # Check if destination exists and compare content
        try:
            async with aiofiles.open(dst, 'rb') as dst_file:
                dst_content = await dst_file.read()
            
            # Compare content - if identical, skip copy
            if src_content == dst_content:
                # Files are identical, no need to copy
                return
        except FileNotFoundError:
            # Destination doesn't exist, proceed with copy
            pass
        
        # Content differs or destination doesn't exist, copy the file
        async with aiofiles.open(dst, 'wb') as dst_file:
            await dst_file.write(src_content)
        
        print(f"[static] Copied {src} -> {dst}")
    except Exception as e:
        print(f"[static] Error copying {src} -> {dst}: {e}")

def main(argv: Optional[list[str]] = None) -> None:
    """Main entry point - parse CLI args and run the async generator"""
    parsed = parse_args(argv)
    _set_runtime_args(parsed)
    print("Running in async mode...")
    asyncio.run(main_async())


async def main_async(parsed: Optional[argparse.Namespace] = None) -> None:
    """Asynchronous main function; optionally accept pre-parsed args"""
    if parsed is not None:
        _set_runtime_args(parsed)
    await generate_all_async(force_regenerate=FORCE_REGEN, division_filter=args.div)


if __name__ == "__main__":
    main()
