# Generate one HTML per division with team summaries, player tables, and map stats.
from pathlib import Path
import sqlite3
from collections import defaultdict
from faceit_config import DIVISIONS
from db import get_conn
from datetime import datetime
from html import escape

DB_PATH = str(Path(__file__).with_name("faceit_reports.sqlite"))
OUT_DIR = Path(__file__).with_name("output")

# ------------------------------
# HTML template (dark theme)
# ------------------------------
HTML_HEAD = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{title}</title>
<style>
:root{
  --fg:#f5f5f5;
  --muted:#aaa;
  --border:#444;
  --bg:#1b1b1b;
  --table-bg:#222;
  --table-alt:#2a2a2a;
  --head:#333;
  --chip-bg:#2a2a2a;
  --nav-bg:#2a2a2a;
  --accent:#4ade80;
}

html,body{height:100%;}
body{
  margin:0;
  font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  font-size: 18px;
  line-height: 1.45;
  color: var(--fg);
  background: var(--bg);
}

.page{
  max-width: 1400px;
  margin: 0 auto;
  padding: 1.25rem 1.25rem 3rem;
}

/* Otsikot */
h1{ font-size: 2rem; margin:.5rem 0 1rem; }
h2{ font-size: 1.5rem; margin:1.25rem 0 .5rem; }
h3{ font-size: 1.15rem; margin:1rem 0 .5rem; }

/* Joukkue-navigaatio */
.nav{ display:flex; flex-wrap:wrap; gap:.75rem; margin:.25rem 0 1rem; }
.nav a{
  color:#cfe1ff;
  text-decoration:none;
  border:1px solid var(--border);
  padding:.25rem .5rem;
  border-radius:999px;
  background: var(--nav-bg);
}
.nav a:hover{ background:#3a3a3a; }

/* Chipit */
.chips { display:flex; gap:.5rem; flex-wrap:wrap; margin:.25rem 0 .75rem 0; }
.chip  { font-size:.95rem; padding:.35rem .65rem; border-radius:999px; background: var(--chip-bg); border:1px solid var(--border); color:var(--fg); }

/* Taulukot */
table{ width:100%; border-collapse: collapse; margin:.5rem 0 1rem; background: var(--table-bg); }
thead th{
  position: sticky; top:0;
  background: var(--head);
  border-bottom:1px solid var(--border);
  font-weight:600;
  color: var(--fg);
}
th, td{ padding:.55rem .7rem; border-bottom:1px solid var(--border); }
tbody tr:nth-child(even){ background: var(--table-alt); }
th, td{ text-align:center; }
th:first-child, td:first-child{ text-align:left; }

/* Sticky 1. sarake ja hover-korostus */
td:first-child, th:first-child{ position:sticky; left:0; z-index:1; background:var(--table-bg); }
tbody tr:hover{ outline:1px solid #555; background:#292929; }

/* progress bar WR-prosenteille */
.bar { position:relative; background:#333; height:20px; border-radius:8px; overflow:hidden; }
.bar > span { position:absolute; left:0; top:0; bottom:0; width:0%; background:var(--accent); }
.bar .val { position:relative; z-index:1; font-size:.9rem; padding-left:.5rem; color:#fff; }

/* asteittainen väriskaala soluille */
.cell-grad.good   { background:linear-gradient(90deg, rgba(34,197,94,.25), transparent); }
.cell-grad.bad    { background:linear-gradient(90deg, rgba(239,68,68,.25), transparent); }
.cell-muted { color:var(--muted); }

/* tooltip otsikoissa */
th[title] { text-decoration: underline dotted #777; text-underline-offset: 3px; cursor: help; }

/* Joukkueosio + ohjeteksti */
.team-section{ padding:.25rem 0 1.25rem; border-top:1px solid var(--border); margin-top:1rem; }
.muted{ color:var(--muted); font-size:.95rem; margin:.25rem 0 1rem; }

/* Toolbar (filter + CSV + columns) */
.toolbar{ display:flex; gap:.75rem; align-items:center; margin:.4rem 0 .5rem; flex-wrap:wrap; }
details.cols summary{ cursor:pointer; }
details.cols div{ display:flex; gap:.75rem; flex-wrap:wrap; padding:.5rem 0; }

/* Tulostus */
@media print{
  body{ background:#fff; color:#000; }
  .page{ max-width:100%; padding:0; }
  .nav, .toolbar{ display:none; }
  .bar{ height:12px; }
}

/* Tabs (Basic / Advanced) */
.tabs { margin:.75rem 0 1rem; }
.tab-nav { display:flex; gap:.5rem; }
.tab-btn {
  background: var(--chip-bg);
  color: var(--fg);
  border:1px solid var(--border);
  padding:.35rem .7rem; border-radius:999px; cursor:pointer;
}
.tab-btn.active { background:#3a3a3a; }
.tab-panel { display:none; }
.tab-panel.active { display:block; }

/* Joukkueen logot */
.logo{
  height:130px; width:130px; object-fit:cover;
  vertical-align:middle; border-radius:4px; margin-right:.5rem;
  background:#111; border:1px solid var(--border);
}
.nav .logo{
  height:35px; width:35px; margin-right:.4rem;
}

</style>

<script>
// --- lajittelu ---
function sortTable(tableId, n, numeric){
  const table = document.getElementById(tableId);
  const dirAttr = table.getAttribute('data-sort-dir') || 'asc';
  const dir = dirAttr === 'asc' ? 1 : -1;
  let rows = Array.from(table.tBodies[0].rows);
  rows.sort((a,b)=>{
    const x = a.cells[n].textContent.trim();
    const y = b.cells[n].textContent.trim();
    if(numeric){
      const nx = parseFloat(x.replace(',', '.')) || 0;
      const ny = parseFloat(y.replace(',', '.')) || 0;
      return (nx - ny) * dir;
    }
    return x.localeCompare(y) * dir;
  });
  table.tBodies[0].append(...rows);
  table.setAttribute('data-sort-dir', dirAttr === 'asc' ? 'desc' : 'asc');
}

function applyDefaultSort(tableId){
  const t = document.getElementById(tableId);
  if(!t) return;
  const col = parseInt(t.getAttribute('data-sort-col') || '0',10);
  const dir = (t.getAttribute('data-sort-dir') || 'asc') === 'asc';
  sortTable(tableId, col, !dir);
  sortTable(tableId, col, dir);
}

// --- Progress bar ---
function renderBar(cell, value){
  cell.innerHTML = '<div class="bar"><span></span><div class="val"></div></div>';
  const span = cell.querySelector('.bar > span');
  const val  = cell.querySelector('.bar .val');
  val.textContent = (isFinite(value) ? value.toFixed(1) : 0) + '%';
  const width = Math.max(0, Math.min(100, value));
  span.style.width = width + '%';
  const g = Math.round(120 * (width/100));
  const r = Math.round(180 * (1 - width/100)) + 60;
  span.style.background = `rgb(${r}, ${g+80}, 100)`;
}

// --- Väriskaalat ---
function colorizeRange(tableId, colIdx, min, max, inverse=false){
  const t = document.getElementById(tableId);
  if(!t || !t.tBodies.length) return;
  const rows = t.tBodies[0].rows;
  for(const tr of rows){
    const td = tr.cells[colIdx];
    let v = parseFloat((td.textContent||'').replace(',', '.'));
    if(!isFinite(v)) { td.classList.add('cell-muted'); continue; }
    const ratio = Math.max(0, Math.min(1, (v - min) / (max - min || 1)));
    const good  = inverse ? (1 - ratio) : ratio;
    if (good >= 0.55) td.classList.add('cell-grad','good');
    else if (good <= 0.45) td.classList.add('cell-grad','bad');
  }
}
function colorizeAuto(tableId, colIdx, inverse=false){
  const t = document.getElementById(tableId); if(!t) return;
  const vals = [...t.tBodies[0].rows].map(tr => parseFloat((tr.cells[colIdx].textContent||'').replace(',','.'))).filter(v=>isFinite(v));
  if(!vals.length) return;
  const min = Math.min(...vals), max = Math.max(...vals);
  colorizeRange(tableId, colIdx, min, max, inverse);
}

// --- Played only ---
function bindPlayedOnly(tableId, chkId){
  const chk = document.getElementById(chkId);
  const t = document.getElementById(tableId);
  if(!chk || !t) return;
  const colPlayed = 1;
  chk.addEventListener('change', ()=> {
    for(const tr of t.tBodies[0].rows){
      const played = parseInt(tr.cells[colPlayed].textContent||'0',10);
      tr.style.display = (chk.checked && !played) ? 'none' : '';
    }
  });
}

// --- Saraketooglet ---
function buildColumnToggles(tableId){
  const t = document.getElementById(tableId);
  const host = document.querySelector(`details.cols div[data-for="${tableId}"]`);
  if(!t || !host) return;
  const ths = [...t.tHead.rows[0].cells];
  ths.forEach((th, idx)=>{
    const id = `${tableId}-col-${idx}`;
    const wrap = document.createElement('label');
    wrap.innerHTML = `<input type="checkbox" id="${id}" checked> ${th.textContent}`;
    host.appendChild(wrap);
    wrap.querySelector('input').addEventListener('change', (e)=>{
      const on = e.target.checked;
      th.style.display = on ? '' : 'none';
      for(const tr of t.tBodies[0].rows){
        tr.cells[idx].style.display = on ? '' : 'none';
      }
    });
  });
}

// Tabien vaihto
function switchTab(containerId, tabName){
  const root = document.getElementById(containerId);
  if(!root) return;
  const panels = root.querySelectorAll('.tab-panel');
  const buttons = root.querySelectorAll('.tab-btn');
  panels.forEach(p => p.classList.remove('active'));
  buttons.forEach(b => b.classList.remove('active'));
  root.querySelector(`[data-tab="${tabName}"]`)?.classList.add('active');
  root.querySelector(`[data-target="${tabName}"]`)?.classList.add('active');
  initTabsAutoSort(containerId);
}


function initTabsAutoSort(rootId){
  const root = document.getElementById(rootId);
  if(!root) return;
  const activePanel = root.querySelector('.tab-panel.active');
  if(!activePanel) return;
  const table = activePanel.querySelector('table');
  if(!table) return;

  // Pakota nimen mukaan (kolumni 0), tekstilajittelu, nouseva
  table.setAttribute('data-sort-col', '0');
  table.setAttribute('data-sort-dir', 'desc'); // tehdään kaksi kutsua, jotta lopputulos on asc

  // 1) sorttaa nimen mukaan (desc), 2) sorttaa uudelleen (asc)
  sortTable(table.id, 0, /*numeric*/ false);
  sortTable(table.id, 0, /*numeric*/ false);
}

// Aja nimen (kolumni 0) mukainen lajittelu kaikille pelaajataabeille heti sivun latauksen jälkeen
document.addEventListener('DOMContentLoaded', () => {
  // Valitse kaikki tab-kontainerit, joilla on id (esim. "tabs-xxxx")
  document.querySelectorAll('.tabs[id]').forEach(root => {
    initTabsAutoSort(root.id);
  });
});

</script>
</head>
<body>
"""

HTML_FOOT = """
</body>
</html>
"""

# ------------------------------
# DB helpers
# ------------------------------

MAP_NAME_DISPLAY = {
    "de_nuke": "Nuke",
    "de_inferno": "Inferno",
    "de_mirage": "Mirage",
    "de_overpass": "Overpass",
    "de_dust2": "Dust II",
    "de_ancient": "Ancient",
    "de_train": "Train",
}

def pretty_map_name(raw: str) -> str:
    return MAP_NAME_DISPLAY.get(raw, raw)

def has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def q(con, sql, params=()):
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    return rows

def get_teams_in_division(con, division_id: int):
    sql = """
    SELECT x.team_id,
           x.team_name,
           t.avatar
    FROM (
      SELECT DISTINCT team1_id AS team_id, team1_name AS team_name
      FROM matches WHERE division_id=? AND team1_id IS NOT NULL
      UNION
      SELECT DISTINCT team2_id AS team_id, team2_name AS team_name
      FROM matches WHERE division_id=? AND team2_id IS NOT NULL
    ) AS x
    LEFT JOIN teams t ON t.team_id = x.team_id
    ORDER BY x.team_name COLLATE NOCASE
    """
    rows = q(con, sql, (division_id, division_id))
    return [r for r in rows if r["team_id"]]

def compute_team_summary(con, division_id: int, team_id: str):
    mp = q(con, "SELECT COUNT(DISTINCT match_id) as c FROM matches WHERE division_id=? AND (team1_id=? OR team2_id=?)",
           (division_id, team_id, team_id))[0]["c"]
    maps_rows = q(con, """
        SELECT m.match_id, m.team1_id, m.team2_id, p.round_index, p.map_name, p.score_team1, p.score_team2, p.winner_team_id
        FROM matches m JOIN maps p ON m.match_id=p.match_id
        WHERE m.division_id=? AND (m.team1_id=? OR m.team2_id=?)
    """, (division_id, team_id, team_id))
    maps_played = len(maps_rows)
    maps_w = sum(1 for r in maps_rows if r.get("winner_team_id") == team_id)
    rd = 0
    for r in maps_rows:
        s1, s2 = r.get("score_team1") or 0, r.get("score_team2") or 0
        if r["team1_id"] == team_id:
            rd += (s1 - s2)
        elif r["team2_id"] == team_id:
            rd += (s2 - s1)
    agg = q(con, "SELECT SUM(kills) kills, SUM(deaths) deaths, AVG(kr) kr, AVG(adr) adr, SUM(utility_damage) util FROM team_stats WHERE team_id=?", (team_id,))[0]
    kills = agg["kills"] or 0
    deaths = agg["deaths"] or 0
    kd = (kills / deaths) if deaths else float(kills)
    return {
        "matches_played": mp,
        "maps_played": maps_played,
        "w": maps_w, "l": maps_played - maps_w,
        "rd": rd,
        "kd": kd, "kr": agg["kr"] or 0.0, "adr": agg["adr"] or 0.0, "util": agg["util"] or 0,
    }

def compute_player_table(con, division_id: int, team_id: str):
    # Selvitään dynaamiset kolumnit kuten ennen (HAS_PISTOL, HAS_FLASH ...)
    HAS_PISTOL = has_column(con, "player_stats", "pistol_kills")
    HAS_FLASH  = has_column(con, "player_stats", "enemies_flashed") and has_column(con, "player_stats", "flash_count")

    rows = q(con, f"""
    SELECT
      ps.player_id AS player_id,
      COALESCE(pl.nickname, MAX(ps.nickname)) AS nickname_display, -- uusin nimi players-taulusta, fallbackina vanhin/viimeisin ps.nickname
      COUNT(*) AS maps_played,

      SUM(ps.kills)  AS k,
      SUM(ps.deaths) AS d,
      SUM(ps.assists) AS a,

      AVG(ps.adr) AS adr,
      AVG(ps.kr)  AS kr,
      AVG(ps.hs_pct) AS hs_pct,

      SUM(ps.sniper_kills) AS awp_kills,
      SUM(ps.mk_3k) AS k3, SUM(ps.mk_4k) AS k4, SUM(ps.mk_5k) AS k5,
      SUM(ps.utility_damage) AS util,

      -- kierrokset per map (maps-taulusta)
      SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) AS rounds,

      -- clutchit (jos sarakkeet ovat kannassa)
      SUM(COALESCE(ps.clutch_kills,0))      AS clutch_kills,
      SUM(COALESCE(ps.cl_1v1_attempts,0))   AS c11_att,
      SUM(COALESCE(ps.cl_1v1_wins,0))       AS c11_win,
      SUM(COALESCE(ps.cl_1v2_attempts,0))   AS c12_att,
      SUM(COALESCE(ps.cl_1v2_wins,0))       AS c12_win,
      SUM(COALESCE(ps.entry_count,0))       AS entry_att,
      SUM(COALESCE(ps.entry_wins,0))        AS entry_win

      {", SUM(ps.pistol_kills) AS pistol_kills" if HAS_PISTOL else ""}
      {", SUM(ps.enemies_flashed) AS flashed, SUM(ps.flash_count) AS flash_count" if HAS_FLASH else ""}

    FROM player_stats ps
    JOIN matches m
      ON m.match_id = ps.match_id
    JOIN maps mp
      ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
    LEFT JOIN players pl
      ON pl.player_id = ps.player_id
    WHERE m.division_id = ? AND ps.team_id = ?
    GROUP BY ps.player_id
    ORDER BY k DESC
    """, (division_id, team_id))


    table = []
    for r in rows:
        k = r["k"] or 0
        d = r["d"] or 0
        a = r["a"] or 0
        kd = (k / d) if d else float(k)
        rounds = r["rounds"] or 0
        maps_played = r["maps_played"] or 0
        rpm = (rounds / maps_played) if maps_played else 0.0

        row = {
            "player_id": r["player_id"],
            "nickname": r["nickname_display"],           # näytetään aina uusin nimi
            "maps_played": maps_played,
            "rounds": rounds,
            "rpm": rpm,                                   # <--- UUSI: rounds per map
            "kd": kd,
            "adr": r["adr"] or 0.0,
            "kr": r["kr"] or 0.0,

            # kokonaismäärät erikseen, jotta HTML voi näyttää ne yksittäisinä sarakkeina
            "kill": k,                                    # <--- UUSI
            "death": d,                                   # <--- UUSI
            "assist": a,                                  # <--- UUSI
            "kda": f"{k}/{d}/{a}",

            "hs_pct": r["hs_pct"] or 0.0,
            "awp_kills": r["awp_kills"] or 0,
            "k3": r["k3"] or 0,
            "k4": r["k4"] or 0,
            "k5": r["k5"] or 0,
            "util": r["util"] or 0,

            # Advanced
            "clutch_kills": r["clutch_kills"] or 0,
            "c11_att": r["c11_att"] or 0,
            "c11_win": r["c11_win"] or 0,
            "c12_att": r["c12_att"] or 0,
            "c12_win": r["c12_win"] or 0,
            "entry_att": r["entry_att"] or 0,
            "entry_win": r["entry_win"] or 0,
        }

        # Valinnaiset sarakkeet jos kannassa on
        if "pistol_kills" in r.keys():
            row["pistol_kills"] = r["pistol_kills"] or 0
        if "flashed" in r.keys() and "flash_count" in r.keys():
            row["flashed"] = r["flashed"] or 0
            row["flash_count"] = r["flash_count"] or 0

        table.append(row)

    return table



def compute_division_map_avgs(con, division_id: int):
    div_avg = {}
    rows = q(con, """
    SELECT p.map_name AS map,
           SUM(ts.kills)*1.0/SUM(NULLIF(ts.deaths,0)) AS kd,
           AVG(ts.adr) AS adr
    FROM team_stats ts
    JOIN maps p   ON p.match_id=ts.match_id AND p.round_index=ts.round_index
    JOIN matches m ON m.match_id=ts.match_id
    WHERE m.division_id = ?
    GROUP BY p.map_name
    """, (division_id,))
    for r in rows:
        div_avg[r["map"]] = ( (r["kd"] or 0.0), (r["adr"] or 0.0) )
    return div_avg

def compute_map_stats_table(con, division_id: int, team_id: str):
    maps_all = q(con, "SELECT DISTINCT map_name FROM maps m JOIN matches t ON t.match_id=m.match_id WHERE t.division_id=? AND map_name IS NOT NULL", (division_id,))
    names = sorted([m["map_name"] for m in maps_all])

    rows = []
    for name in names:
        played_rows = q(con, """
            SELECT m.match_id, m.team1_id, m.team2_id, p.score_team1, p.score_team2, p.winner_team_id, p.round_index
            FROM matches m JOIN maps p ON m.match_id=p.match_id
            WHERE m.division_id=? AND p.map_name=? AND (m.team1_id=? OR m.team2_id=?)
        """, (division_id, name, team_id, team_id))

        played = len(played_rows)
        w = sum(1 for r in played_rows if r.get("winner_team_id") == team_id)
        rd = 0
        for r in played_rows:
            s1, s2 = r.get("score_team1") or 0, r.get("score_team2") or 0
            if r["team1_id"] == team_id: rd += (s1 - s2)
            elif r["team2_id"] == team_id: rd += (s2 - s1)

        ts = q(con, """
            SELECT SUM(ts.kills) kills, SUM(ts.deaths) deaths, AVG(ts.adr) adr
            FROM team_stats ts
            JOIN maps p   ON p.match_id=ts.match_id AND p.round_index=ts.round_index
            JOIN matches m ON m.match_id=ts.match_id
            WHERE ts.team_id=? AND p.map_name=? AND m.division_id=?
        """, (team_id, name, division_id))[0]
        kills = ts["kills"] or 0
        deaths = ts["deaths"] or 0
        kd = (kills / deaths) if deaths else float(kills)
        adr = ts["adr"] or 0.0

        # Votes
        votes_all = q(con, """
            SELECT v.match_id, v.map_name, v.status, v.selected_by_team_id, v.round_num,
                   m.team1_id AS m_team1, m.team2_id AS m_team2
            FROM map_votes v
            JOIN matches m ON m.match_id = v.match_id
            WHERE m.division_id = ?
              AND (m.team1_id = ? OR m.team2_id = ?)
        """, (division_id, team_id, team_id))

        dedup = {}
        for v in votes_all:
            key = (v["match_id"], v["map_name"], v["status"], v["selected_by_team_id"])
            if key not in dedup or (v.get("round_num") or 0) < (dedup[key].get("round_num") or 1<<30):
                dedup[key] = v

        by_match = {}
        for v in dedup.values():
            by_match.setdefault(v["match_id"], []).append(v)

        first_ban = second_ban = opp_ban = picks = opp_picks = 0
        own_picks_by_match = {}
        opp_picks_by_match = {}

        def _rn(v):
            try: return int(v.get("round_num") or 0)
            except: return 0

        for mid, lst in by_match.items():
            m_team1 = lst[0]["m_team1"]; m_team2 = lst[0]["m_team2"]
            opp_id = m_team2 if m_team1 == team_id else (m_team1 if m_team2 == team_id else None)
            if not opp_id: continue

            pick_rounds = [_rn(v) for v in lst if v["status"] == "pick"]
            cutoff = min(pick_rounds) if pick_rounds else 10**9

            ours_all = [v for v in lst if v["selected_by_team_id"] == team_id and v["status"] in ("drop","pick")]
            opps_all = [v for v in lst if v["selected_by_team_id"] == opp_id  and v["status"] in ("drop","pick")]

            own_drops = sorted([v for v in ours_all if v["status"] == "drop" and _rn(v) < cutoff], key=_rn)
            opp_drops = [v for v in opps_all if v["status"] == "drop" and _rn(v) < cutoff]

            if len(own_drops) >= 1 and own_drops[0]["map_name"] == name:
                first_ban += 1
            if len(own_drops) >= 2 and own_drops[1]["map_name"] == name:
                second_ban += 1
            if any(v["map_name"] == name for v in opp_drops):
                opp_ban += 1

            own_pick_maps = {v["map_name"] for v in ours_all if v["status"] == "pick"}
            opp_pick_maps = {v["map_name"] for v in opps_all if v["status"] == "pick"}
            if name in own_pick_maps: picks += 1
            if name in opp_pick_maps: opp_picks += 1
            own_picks_by_match[mid] = own_pick_maps
            opp_picks_by_match[mid] = opp_pick_maps

        total_own_ban = first_ban + second_ban

        # WR:t
        wr = (w / played * 100.0) if played else 0.0
        own_pick_wins = own_pick_games = 0
        opp_pick_wins = opp_pick_games = 0
        for r in played_rows:
            mid = r["match_id"]
            if mid in own_picks_by_match and name in own_picks_by_match[mid]:
                own_pick_games += 1
                if r.get("winner_team_id") == team_id: own_pick_wins += 1
            elif mid in opp_picks_by_match and name in opp_picks_by_match[mid]:
                opp_pick_games += 1
                if r.get("winner_team_id") == team_id: opp_pick_wins += 1
        wr_own = (own_pick_wins / own_pick_games * 100.0) if own_pick_games else 0.0
        wr_opp = (opp_pick_wins / opp_pick_games * 100.0) if opp_pick_games else 0.0

        rows.append({
            "map": name,
            "played": played,
            "picks": picks,
            "opp_picks": opp_picks,
            "wr": wr,
            "wr_own": wr_own,
            "wr_opp": wr_opp,
            "kd": kd,
            "adr": adr,
            "rd": rd,
            "ban1": first_ban,
            "ban2": second_ban,
            "opp_ban": opp_ban,
            "total_own_ban": total_own_ban,
        })
    return rows

# ------------------------------
# Rendering
# ------------------------------
def render_division(con, div):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    teams = get_teams_in_division(con, div["division_id"])
    div_avgs = compute_division_map_avgs(con, div["division_id"])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = [HTML_HEAD.replace("{title}", f"{div['name']} – generoitu {ts}")]
    html.append('<div class="page">')

    html.append(f"<h1>{div['name']} <span class='muted'>(generoitu {ts})</span></h1>")
    html.append('<div class="nav">')
    for t in teams:
        name = t["team_name"] or t["team_id"]
        avatar = t.get("avatar")
        logo = f'<img class="logo" src="{avatar}" alt="">' if avatar else ''
        html.append(f'<a href="#team-{t["team_id"]}">{logo}{escape(name)}</a>')
    html.append("</div>")


    html.append('<div class="muted">Vinkki: klikkaa joukkueen otsikkoriviä avataksesi tai sulkeaksesi sen.</div>')

    for ti, t in enumerate(teams, start=1):
        team_id = t["team_id"]; team_name = t["team_name"] or t["team_id"]
        html.append(f'<details class="team-section" id="team-{team_id}" open>')
        # hae avatar muistista (teams-listasta)
        team_avatar = next((t.get("avatar") for t in teams if t["team_id"] == team_id), None)
        logo = f'<img class="logo" src="{team_avatar}" alt="">' if team_avatar else ''
        html.append(f"<summary><h2>{logo}{escape(team_name)}</h2></summary>")


        s = compute_team_summary(con, div["division_id"], team_id)
        chips = [
            f'<span class="chip">Matches {s["matches_played"]}</span>',
            f'<span class="chip">Maps {s["maps_played"]}</span>',
            f'<span class="chip">W-L {s["w"]}-{s["l"]}</span>',
            f'<span class="chip">±RD {s["rd"]}</span>',
            f'<span class="chip">KD {s["kd"]:.2f}</span>',
            f'<span class="chip">KR {s["kr"]:.2f}</span>',
            f'<span class="chip">ADR {s["adr"]:.1f}</span>',
            f'<span class="chip">Util {int(s["util"])}</span>',
        ]
        html.append("<div>" + " ".join(chips) + "</div>")

        # Players (TABS: Basic / Advanced)
        players = compute_player_table(con, div["division_id"], team_id)
        # Johdetut mittarit + optiosarakkeiden tunnisteet
        has_flash  = any(("flashed" in p and "flash_count" in p) for p in players)
        has_pistol = any(("pistol_kills" in p) for p in players)

        for p in players:
            # Winratet (%)
            c11_att = p.get("c11_att", 0) or 0
            c11_win = p.get("c11_win", 0) or 0
            p["c11_wr"] = (c11_win / c11_att * 100.0) if c11_att else 0.0

            c12_att = p.get("c12_att", 0) or 0
            c12_win = p.get("c12_win", 0) or 0
            p["c12_wr"] = (c12_win / c12_att * 100.0) if c12_att else 0.0

            entry_att = p.get("entry_att", 0) or 0
            entry_win = p.get("entry_win", 0) or 0
            p["entry_wr"] = (entry_win / entry_att * 100.0) if entry_att else 0.0

            # Utility damage per round
            rounds = p.get("rounds", 0) or 0
            util   = p.get("util", 0) or 0
            p["udpr"] = (util / rounds) if rounds else 0.0

            # Impact-proxy: 2*KR + 0.42*AR - 0.41*DR
            kr = p.get("kr", 0.0) or 0.0
            ar = (p.get("assist", 0) or 0) / rounds if rounds else 0.0
            dr = (p.get("death", 0)  or 0) / rounds if rounds else 0.0
            p["impact"] = 2.0*kr + 0.42*ar - 0.41*dr

            # Enemies per flash (jos dataa on)
            if has_flash:
                fc = p.get("flash_count", 0) or 0
                p["enemies_per_flash"] = (p.get("flashed", 0) or 0) / fc if fc else 0.0
            else:
                p["enemies_per_flash"] = None

        tab_root_id = f"tabs-{team_id[:8]}"

        html.append('<h3>Players</h3>')
        html.append(f"""
          <div id="{tab_root_id}" class="tabs">
            <div class="tab-nav">
              <button class="tab-btn active" data-target="basic"
                      onclick="switchTab('{tab_root_id}','basic')">Basic</button>
              <button class="tab-btn" data-target="advanced"
                      onclick="switchTab('{tab_root_id}','advanced')">Advanced</button>
            </div>
        """)


        # ---------- BASIC ----------
        tid_basic = f"players-basic-{ti}"
        html.append(f'<div class="tab-panel active" data-tab="basic">')
        html.append(f'<table id="{tid_basic}" data-sort-col="3" data-sort-dir="desc">')
        html.append(f"""<thead><tr>
          <th onclick="sortTable('{tid_basic}',0,false)">Nickname</th>
          <th onclick="sortTable('{tid_basic}',1,true)">Maps</th>
          <th onclick="sortTable('{tid_basic}',2,true)" title="Total rounds">Rounds</th>
          <th onclick="sortTable('{tid_basic}',3,true)" title="Kills/Deaths">KD</th>
          <th onclick="sortTable('{tid_basic}',4,true)">ADR</th>
          <th onclick="sortTable('{tid_basic}',5,true)">KR</th>
          <th onclick="sortTable('{tid_basic}',6,true)">K</th>
          <th onclick="sortTable('{tid_basic}',7,true)">D</th>
          <th onclick="sortTable('{tid_basic}',8,true)">A</th>
          <th onclick="sortTable('{tid_basic}',9,true)">HS%</th>
          <th onclick="sortTable('{tid_basic}',10,true)">AWP</th>
          <th onclick="sortTable('{tid_basic}',11,true)">3K</th>
          <th onclick="sortTable('{tid_basic}',12,true)">4K</th>
          <th onclick="sortTable('{tid_basic}',13,true)">5K</th>
          <th onclick="sortTable('{tid_basic}',14,true)">Util dmg</th>
        </tr></thead><tbody>""")
        for p in players:
            html.append(f"""<tr>
              <td>{p["nickname"]}</td>
              <td>{p["maps_played"]}</td>
              <td title="Rounds/Map: {p['rpm']:.1f}">{p["rounds"]}</td>
              <td>{p["kd"]:.2f}</td>
              <td>{p["adr"]:.1f}</td>
              <td>{p["kr"]:.2f}</td>
              <td>{p["kill"]}</td>
              <td>{p["death"]}</td>
              <td>{p["assist"]}</td>
              <td>{p["hs_pct"]:.1f}</td>
              <td>{p["awp_kills"]}</td>
              <td>{p["k3"]}</td>
              <td>{p["k4"]}</td>
              <td>{p["k5"]}</td>
              <td>{int(p["util"])}</td>
            </tr>""")
        html.append("</tbody></table>")
        html.append(f"<script>colorizeRange('{tid_basic}', 3, 0.3, 1.5, false);</script>")
        html.append(f"<script>colorizeRange('{tid_basic}', 4, 50, 120, false);</script>")
        html.append(f"<script>colorizeRange('{tid_basic}', 5, 0.2, 1.2, false);</script>")
        html.append(f"<script>applyDefaultSort('{tid_basic}');</script>")
        html.append("</div>")  # /tab-panel basic

        # ---------- ADVANCED ----------
        tid_adv = f"players-adv-{ti}"
        html.append(f'<div class="tab-panel" data-tab="advanced">')
        html.append(f'<table id="{tid_adv}" data-sort-col="7" data-sort-dir="desc">')  # oletus: UDPR
        html.append("<thead><tr>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',0,false)\">Nickname</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',1,true)\" title='Clutch-fragit 1vX-tilanteissa'>Clutch K</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',2,true)\" title='1v1 WR%, suluissa yritykset'>1v1 WR%</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',3,true)\" title='1v2 WR%, suluissa yritykset'>1v2 WR%</th>")

        # Entry stats
        html.append(f"<th onclick=\"sortTable('{tid_adv}',4,true)\" title='Entry attempts (ensimmäiset kontaktit)'>Entry Att</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',5,true)\" title='Entry wins (voitetut ensimmäiset kontaktit)'>Entry Win</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',6,true)\" title='Entry win rate'>Entry WR%</th>")

        # Utility-derivaatit
        html.append(f"<th onclick=\"sortTable('{tid_adv}',7,true)\" title='Utility damage per round'>UDPR</th>")
        html.append(f"<th onclick=\"sortTable('{tid_adv}',8,true)\" title='Impact-proxy: 2*KR + 0.42*AR - 0.41*DR'>Impact</th>")

        col_idx = 9
        if has_flash:
            html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Enemies Flashed / Flash Count'>Enem/Flash</th>")
            col_idx += 1
        if has_pistol:
            html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\">Pistol K</th>")
        html.append("</tr></thead><tbody>")

        for p in players:
          html.append("<tr>")
          html.append(f"<td>{p['nickname']}</td>")
          html.append(f"<td>{p['clutch_kills']}</td>")
          html.append(f"<td data-sort=\"{p['c11_wr']:.1f}\" title=\"Attempts: {p['c11_att']}, Wins: {p['c11_win']}\">{p['c11_wr']:.0f}% ({p['c11_att']})</td>")
          html.append(f"<td data-sort=\"{p['c12_wr']:.1f}\" title=\"Attempts: {p['c12_att']}, Wins: {p['c12_win']}\">{p['c12_wr']:.0f}% ({p['c12_att']})</td>")

          # Entryt
          html.append(f"<td>{p['entry_att']}</td>")
          html.append(f"<td>{p['entry_win']}</td>")
          html.append(f"<td>{p['entry_wr']:.0f}</td>")

          # Utility-derivaatit
          html.append(f"<td>{p['udpr']:.2f}</td>")
          html.append(f"<td>{p['impact']:.2f}</td>")

          if has_flash:
              val = p['enemies_per_flash'] if p['enemies_per_flash'] is not None else 0.0
              html.append(f"<td>{val:.2f}</td>")
          if has_pistol:
              val = p.get('pistol_kills', 0) or 0
              html.append(f"<td>{val}</td>")
          html.append("</tr>")

        html.append("</tbody></table>")

        # Väritykset: WR:t vihreäksi kun korkea
        html.append(f"<script>colorizeRange('{tid_adv}', 2, 0, 100, false);</script>")
        html.append(f"<script>colorizeRange('{tid_adv}', 3, 0, 100, false);</script>")
        html.append(f"<script>colorizeRange('{tid_adv}', 6, 0, 100, false);</script>")
        html.append(f"<script>colorizeRange('{tid_adv}', 7, 0.5, 15, false);</script>")
        html.append(f"<script>colorizeRange('{tid_adv}', 8, 0.6, 2.2, false);</script>")
        html.append(f"<script>applyDefaultSort('{tid_adv}');</script>")
        html.append("</div>")  # /tab-panel advanced

        # Map stats
        maps = compute_map_stats_table(con, div["division_id"], team_id)

        # Chipit
        best_wr = max((r for r in maps if r["played"]>0), key=lambda r: r["wr"], default=None)
        most_pick = max(maps, key=lambda r: r["picks"], default=None)
        most_ban  = max(maps, key=lambda r: r["total_own_ban"], default=None)
        played_rows = [r for r in maps if r["played"]>=2]
        avoid = min(played_rows, key=lambda r: r["wr"], default=None)

        html.append('<div class="chips">')
        if most_ban and most_ban["total_own_ban"]>0:
            html.append(f'<span class="chip">Most banned: {pretty_map_name(most_ban["map"])} ({most_ban["total_own_ban"]}×)</span>')
        if most_pick and most_pick["picks"]>0:
            html.append(f'<span class="chip">Most picked: {pretty_map_name(most_pick["map"])} ({most_pick["picks"]}×)</span>')
        if best_wr and best_wr["wr"]>0:
            html.append(f'<span class="chip">Best WR: {pretty_map_name(best_wr["map"])} ({best_wr["wr"]:.0f}%)</span>')
        if avoid:
            html.append(f'<span class="chip">Map to avoid: {pretty_map_name(avoid["map"])} ({avoid["wr"]:.0f}%)</span>')
        html.append('</div>')

        # Toolbar (filter + CSV + column toggles)
        tid2 = f"maps-{ti}"
        html.append(f"""
        <div class="toolbar">
          <label><input type="checkbox" id="{tid2}-played-only"> Show played only</label>
          <details class="cols">
            <summary>Columns</summary>
            <div data-for="{tid2}"></div>
          </details>
        </div>
        """)


        html.append(f'<h3>Map Stats</h3>')
        html.append(f'<table id="{tid2}" data-sort-col="1" data-sort-dir="desc">')
        html.append(f"""
        <thead><tr>
        <th title="Map name" onclick="sortTable('{tid2}',0,false)">Map</th>
        <th title="Maps played" onclick="sortTable('{tid2}',1,true)">Played</th>
        <th title="Matches this map was your pick" onclick="sortTable('{tid2}',2,true)">Picks</th>
        <th title="Matches this map was opponent pick" onclick="sortTable('{tid2}',3,true)">Opp picks</th>
        <th title="Winrate on this map" onclick="sortTable('{tid2}',4,true)">WR %</th>
        <th title="Winrate when you picked" onclick="sortTable('{tid2}',5,true)">WR own pick %</th>
        <th title="Winrate when opponent picked" onclick="sortTable('{tid2}',6,true)">WR opp pick %</th>
        <th title="Team K/D on this map" onclick="sortTable('{tid2}',7,true)">KD</th>
        <th title="Average Damage / Round" onclick="sortTable('{tid2}',8,true)">ADR</th>
        <th title="Round diff (won - lost)" onclick="sortTable('{tid2}',9,true)">±RD</th>
        <th title="Times this map was your first ban" onclick="sortTable('{tid2}',10,true)">1st ban</th>
        <th title="Times this map was your second ban" onclick="sortTable('{tid2}',11,true)">2nd ban</th>
        <th title="Matches where opponent banned this map" onclick="sortTable('{tid2}',12,true)">Opp ban</th>
        <th title="Your total bans (1st+2nd)" onclick="sortTable('{tid2}',13,true)">Total own ban</th>
        </tr></thead><tbody>
        """)

        # rivit
        for r in maps:
            # Δ vs division avg tooltippeihin
            dkd = 0.0; dadr = 0.0
            if r["map"] in div_avgs:
                dkd = (r["kd"] or 0.0) - div_avgs[r["map"]][0]
                dadr= (r["adr"] or 0.0) - div_avgs[r["map"]][1]
            html.append(f"""<tr>
            <td>{pretty_map_name(r["map"])}</td>
            <td>{r["played"]}</td>
            <td>{r["picks"]}</td>
            <td>{r["opp_picks"]}</td>
            <td>{r["wr"]:.1f}</td>
            <td>{r["wr_own"]:.1f}</td>
            <td>{r["wr_opp"]:.1f}</td>
            <td title="Δ vs div avg: {dkd:+.2f}">{r["kd"]:.2f}</td>
            <td title="Δ vs div avg: {dadr:+.1f}">{r["adr"]:.1f}</td>
            <td>{r["rd"]}</td>
            <td>{r["ban1"]}</td>
            <td>{r["ban2"]}</td>
            <td>{r["opp_ban"]}</td>
            <td>{r["total_own_ban"]}</td>
            </tr>""")
        html.append("</tbody></table>")

        # WR-bars + väritys + oletussorttaus + työkalut
        html.append("""
        <script>
        (function(){
          const t = document.getElementById('{TID}');
          if (!t || !t.tBodies.length) return;
          const rows = t.tBodies[0].rows;
          for (const tr of rows) {
            const played = parseInt(tr.cells[1].textContent||'0',10);
            [4,5,6].forEach(function(i){
              const num = parseFloat((tr.cells[i].textContent || '').replace(',', '.'));
              renderBar(tr.cells[i], isFinite(num) ? num : 0);
              const span = tr.cells[i].querySelector('.bar > span');
              span.style.opacity = Math.max(.35, Math.min(1, Math.sqrt(played)/2));
              tr.cells[i].title = 'Played: ' + played;
            });
          }
          colorizeAuto('{TID}', 7, false);  // KD
          colorizeAuto('{TID}', 8, false);  // ADR
          colorizeRange('{TID}', 9, -15, 15, false); // RD
          applyDefaultSort('{TID}');
          bindPlayedOnly('{TID}', '{TID}-played-only');
          buildColumnToggles('{TID}');
        })();
        </script>
        """.replace("{TID}", tid2))

        html.append("</details>")  # team section

    html.append('</div>')  # .page
    html.append(HTML_FOOT)
    out_path = OUT_DIR / f"{div['slug']}.html"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(html), encoding="utf-8")
    return out_path


def write_index():
    """Generoi GitHub Pages -ystävällisen index.html:n output-hakemistoon."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Aikaleima
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Rakennetaan yksinkertainen etusivu uudelleenkäyttäen samaa HTML-pohjaa
    html = [HTML_HEAD.replace("{title}", f"CS2 Faceit Reports – generoitu {ts}")]
    html.append('<div class="page">')
    html.append(f"<h1>CS2 Faceit Reports <span class='muted'>(generoitu {ts})</span></h1>")

    # Pieni ohjeistus
    html.append("<p class='muted'>Valitse divisioona alta. Sivu linkittää valmiiksi generoituun raporttiin.</p>")

    # Lista divisioonista
    html.append('<div class="nav">')
    for div in DIVISIONS:
        # Huom: linkki suhteessa index.html → output/<slug>.html
        html.append(f'<a href="output/{div["slug"]}.html">{div["name"]}</a>')
    html.append('</div>')

    html.append("</div>")  # .page
    html.append(HTML_FOOT)

    # Kirjoita projektin juureen index.html (GitHub Pages yleensä näyttää tämän)
    idx_path = Path(__file__).with_name("index.html")
    idx_path.write_text("\n".join(html), encoding="utf-8")
    print(f"[OK] Wrote {idx_path}")




def main():
    con = get_conn(DB_PATH)
    for div in DIVISIONS:
        path = render_division(con, div)
        print(f"[OK] Wrote {path}")
    con.close()

    # GENEROI GITHUB-INDEX
    write_index()

if __name__ == "__main__":
    main()
