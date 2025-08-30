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
body{ margin:0; font-family: system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; font-size:18px; line-height:1.45; color:var(--fg); background:var(--bg); }
.page{ max-width:1400px; margin:0 auto; padding:1.25rem 1.25rem 3rem; }
h1{ font-size:2rem; margin:.5rem 0 1rem; }
h2{ font-size:1.5rem; margin:1.25rem 0 .5rem; }
h3{ font-size:1.15rem; margin:1rem 0 .5rem; }
.nav{ display:flex; flex-wrap:wrap; gap:.75rem; margin:.25rem 0 1rem; }
.nav a{ color:#cfe1ff; text-decoration:none; border:1px solid var(--border); padding:.25rem .5rem; border-radius:999px; background:var(--nav-bg); }
.nav a:hover{ background:#3a3a3a; }
.chips{ display:flex; gap:.5rem; flex-wrap:wrap; margin:.25rem 0 .75rem 0; }
.chip{ font-size:.95rem; padding:.35rem .65rem; border-radius:999px; background:var(--chip-bg); border:1px solid var(--border); color:var(--fg); }

table{ width:100%; border-collapse:collapse; margin:.5rem 0 1rem; background:var(--table-bg); }
thead th{ position:sticky; top:0; background:var(--head); border-bottom:1px solid var(--border); font-weight:600; color:var(--fg); }
th,td{ padding:.55rem .7rem; border-bottom:1px solid var(--border); }
tbody tr:nth-child(even){ background:var(--table-alt); }
th,td{ text-align:center; }
th:first-child,td:first-child{ text-align:left; }
td:first-child,th:first-child{ position:sticky; left:0; z-index:1; background:var(--table-bg); }
tbody tr:hover{ outline:1px solid #555; background:#292929; }

.bar{ position:relative; background:#333; height:20px; border-radius:8px; overflow:hidden; }
.bar>span{ position:absolute; left:0; top:0; bottom:0; width:0%; background:var(--accent); }
.bar .val{ position:relative; z-index:1; font-size:.9rem; padding-left:.5rem; color:#fff; }

/* Split WR bar (win + loss) */
.bar-split{position:relative;height:20px;border-radius:6px;overflow:hidden;background:#333}
.bar-split .win{position:absolute;left:0;top:0;bottom:0;background:#22c55e}
.bar-split .loss{position:absolute;top:0;bottom:0;background:#ef4444}
.bar-split .val{position:relative;z-index:1;text-align:center;line-height:20px;font-size:.85rem;color:#fff}

.cell-grad.good{ background:linear-gradient(90deg, rgba(34,197,94,.25), transparent); }
.cell-grad.bad{  background:linear-gradient(90deg, rgba(239,68,68,.25), transparent); }
.cell-muted{ color:var(--muted); }
th[title]{ text-decoration: underline dotted #777; text-underline-offset:3px; cursor:help; }

.team-section{ padding:.25rem 0 1.25rem; border-top:1px solid var(--border); margin-top:1rem; }
.muted{ color:var(--muted); font-size:.95rem; margin:.25rem 0 1rem; }

.toolbar{ display:flex; gap:.75rem; align-items:center; margin:.4rem 0 .5rem; flex-wrap:wrap; }
details.cols summary{ cursor:pointer; }
details.cols div{ display:flex; gap:.75rem; flex-wrap:wrap; padding:.5rem 0; }

.tabs{ margin:.75rem 0 1rem; }
.tab-nav{ display:flex; gap:.5rem; }
.tab-btn{ background:var(--chip-bg); color:var(--fg); border:1px solid var(--border); padding:.35rem .7rem; border-radius:999px; cursor:pointer; }
.tab-btn.active{ background:#3a3a3a; }
.tab-panel{ display:none; }
.tab-panel.active{ display:block; }

.logo{ height:130px; width:130px; object-fit:cover; vertical-align:middle; border-radius:4px; margin-right:.5rem; background:#111; border:1px solid var(--border); }
.nav .logo{ height:35px; width:35px; margin-right:.4rem; }

.div-summary{ display:grid; grid-template-columns:1.2fr 1fr; gap:1rem; margin:.75rem 0 1rem; }
.div-summary .card{ background:var(--table-bg); border:1px solid var(--border); border-radius:8px; padding:.75rem .9rem; }
.summary-grid{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.6rem; }
.summary-item{ background:var(--table-alt); border:1px solid var(--border); border-radius:6px; padding:.5rem .6rem; text-align:center; }
.summary-item .label{ color:var(--muted); font-size:.9rem; }
.summary-item .val{ font-size:1.15rem; font-weight:600; }
.leaders table{ width:100%; border-collapse:collapse; }
.leaders th,.leaders td{ padding:.35rem .5rem; border-bottom:1px solid var(--border); text-align:left; }
.leaders th{ color:var(--muted); font-weight:600; }

@media print{
  body{ background:#fff; color:#000; }
  .page{ max-width:100%; padding:0; }
  .nav,.toolbar{ display:none; }
  .bar{ height:12px; }
}
</style>

<script>
function sortTable(tableId,n,numeric){
  const table=document.getElementById(tableId);
  const dirAttr=table.getAttribute('data-sort-dir')||'asc';
  const dir=dirAttr==='asc'?1:-1;
  let rows=Array.from(table.tBodies[0].rows);
  rows.sort((a,b)=>{
    const x=a.cells[n].textContent.trim(); const y=b.cells[n].textContent.trim();
    if(numeric){
      const nx=parseFloat(x.replace(',','.'))||0; const ny=parseFloat(y.replace(',','.'))||0;
      return (nx-ny)*dir;
    }
    return x.localeCompare(y)*dir;
  });
  table.tBodies[0].append(...rows);
  table.setAttribute('data-sort-dir', dirAttr==='asc'?'desc':'asc');
}
function applyDefaultSort(tableId){
  const t=document.getElementById(tableId); if(!t) return;
  const col=parseInt(t.getAttribute('data-sort-col')||'0',10);
  const dir=(t.getAttribute('data-sort-dir')||'asc')==='asc';
  sortTable(tableId,col,!dir); sortTable(tableId,col,dir);
}
function renderBar(cell,value){
  cell.innerHTML='<div class="bar"><span></span><div class="val"></div></div>';
  const span=cell.querySelector('.bar > span'); const val=cell.querySelector('.bar .val');
  val.textContent=(isFinite(value)?value.toFixed(1):0)+'%';
  const width=Math.max(0,Math.min(100,value)); span.style.width=width+'%';
  const g=Math.round(120*(width/100)); const r=Math.round(180*(1-width/100))+60;
  span.style.background=`rgb(${r},${g+80},100)`;
}
function bindPlayedOnly(tableId,chkId){
  const chk=document.getElementById(chkId); const t=document.getElementById(tableId);
  if(!chk||!t) return; const colPlayed=1;
  chk.addEventListener('change',()=>{ for(const tr of t.tBodies[0].rows){ const played=parseInt(tr.cells[colPlayed].textContent||'0',10); tr.style.display=(chk.checked&&!played)?'none':''; }});
}
/* p25–p50–p75 liuku: punainen→vihreä */
function colorizeContinuous(tableId,colIdx,p25,p50,p75,inverse=false){
  const t=document.getElementById(tableId); if(!t||!t.tBodies.length) return;
  const rows=t.tBodies[0].rows;
  for(const tr of rows){
    const td=tr.cells[colIdx]; let v=parseFloat((td.textContent||'').replace(',','.'));
    if(!isFinite(v)){ td.classList.add('cell-muted'); continue; }
    let ratio; if(v<=p25) ratio=0; else if(v>=p75) ratio=1; else ratio=(v-p25)/(p75-p25||1);
    if(inverse) ratio=1-ratio;
    const r=Math.round(240*(1-ratio)); const g=Math.round(220*ratio); td.style.background=`rgba(${r},${g},0,0.28)`;
  }
}
function postProcessTable(tableId,opts){
  const t=document.getElementById(tableId); if(!t||!t.tBodies.length) return;
  const rows=t.tBodies[0].rows;
  if(opts.bars){
    for(const tr of rows){
      const played=parseInt(tr.cells[1].textContent||'0',10);
      opts.bars.forEach(i=>{
        const num=parseFloat((tr.cells[i].textContent||'').replace(',','.'));
        renderBar(tr.cells[i], isFinite(num)?num:0);
        const span=tr.cells[i].querySelector('.bar > span'); if(span) span.style.opacity=Math.max(.35,Math.min(1,Math.sqrt(played)/2));
      });
    }
  }
  if (opts.wrbars){
    for (const tr of rows){
      opts.wrbars.forEach(i => {
        const td = tr.cells[i];
        if (!td || !td.classList.contains('wr')) return;

        const g   = parseInt(td.dataset.g || '0', 10);        // games played
        const w   = parseInt(td.dataset.w || '0', 10);        // wins
        const pctAttr = parseFloat((td.dataset.pct || '').replace(',','.'));
        const pct = isFinite(pctAttr) ? pctAttr : (g ? (100*w/g) : 0);

        if (!g) {
          // Pelaajataulukot: näytä 0–0 (0%) harmaana, jos data-zero="show"
          if (td.dataset.zero === 'show') {
            td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
            const val = td.querySelector('.val');
            val.textContent = '0–0 (0%)';
            td.querySelector('.win').style.width  = '0%';
            td.querySelector('.loss').style.left  = '0%';
            td.querySelector('.loss').style.width = '100%';
            td.querySelector('.win').style.background  = '#555';  // neutraali harmaa
            td.querySelector('.loss').style.background = '#555';  // neutraali harmaa
            td.classList.add('cell-muted');
            td.title = 'No attempts';
          } else {
            // Karttataulukot: pidä "not played"
            td.textContent = 'not played';
            td.classList.add('cell-muted');
            td.title = 'No games';
          }
          return;
        }

        renderSplitWR(td, g, pct);  // piirtää vihreä/punainen -splitin
      });
    }
  }

  if(opts.color){ opts.color.forEach(c=>colorizeContinuous(tableId,c.col,c.p[0],c.p[1],c.p[2],c.inverse||false)); }
  if(opts.defaultSort){ sortTable(tableId,opts.defaultSort.col,opts.defaultSort.dir==='asc'); }
  if(opts.toggles) buildColumnToggles(tableId);
}
function buildColumnToggles(tableId){
  const t=document.getElementById(tableId);
  const host=document.querySelector(`details.cols div[data-for="${tableId}"]`);
  if(!t||!host) return; const ths=[...t.tHead.rows[0].cells];
  ths.forEach((th,idx)=>{
    const id=`${tableId}-col-${idx}`; const wrap=document.createElement('label');
    wrap.innerHTML=`<input type="checkbox" id="${id}" checked> ${th.textContent}`;
    host.appendChild(wrap);
    wrap.querySelector('input').addEventListener('change',(e)=>{
      const on=e.target.checked; th.style.display=on?'':'none';
      for(const tr of t.tBodies[0].rows){ tr.cells[idx].style.display=on?'':'none'; }
    });
  });
}
function switchTab(containerId,tabName){
  const root=document.getElementById(containerId); if(!root) return;
  const panels=root.querySelectorAll('.tab-panel'); const buttons=root.querySelectorAll('.tab-btn');
  panels.forEach(p=>p.classList.remove('active')); buttons.forEach(b=>b.classList.remove('active'));
  root.querySelector(`[data-tab="${tabName}"]`)?.classList.add('active');
  root.querySelector(`[data-target="${tabName}"]`)?.classList.add('active');
  initTabsAutoSort(containerId);
}
function initTabsAutoSort(rootId){
  const root=document.getElementById(rootId); if(!root) return;
  const activePanel=root.querySelector('.tab-panel.active'); if(!activePanel) return;
  const table=activePanel.querySelector('table'); if(!table) return;
  table.setAttribute('data-sort-col','0'); table.setAttribute('data-sort-dir','desc');
  sortTable(table.id,0,false); sortTable(table.id,0,false);
}
function renderWRCell(td){
  const w = parseInt(td.dataset.w || '0', 10);    // wins
  const g = parseInt(td.dataset.g || '0', 10);    // games
  const l = Math.max(0, g - w);                   // losses
  const pctAttr = parseFloat((td.dataset.pct || '').replace(',','.'));
  const pct = isFinite(pctAttr) ? pctAttr : (g ? (100*w/g) : 0);

  td.innerHTML = '<div class="bar"><span></span><div class="val"></div></div>';
  const span = td.querySelector('.bar > span');
  const val  = td.querySelector('.bar .val');

  // palkin leveys
  const wPct = Math.max(0, Math.min(100, pct));
  span.style.width = wPct + '%';

  // teksti "W–L (P%)"
  val.textContent = `${w}–${l} (${Math.round(pct)}%)`;

  // sävy: vihreä kasvaa, punainen vähenee
  const gcol = Math.round(180 * (wPct/100));
  const rcol = Math.round(200 * (1 - wPct/100));
  span.style.background = `rgb(${rcol},${gcol},100)`;

  td.title = g ? `Wins: ${w}, Losses: ${l}, WR: ${pct.toFixed(1)}%` : 'No games';
}
function renderSplitWR(td, played, wrPct){
  const g = Math.max(0, parseInt(played || 0, 10));
  const pct = Math.max(0, Math.min(100, parseFloat((wrPct||0))));
  const wins = Math.round(g * pct / 100);
  const losses = Math.max(0, g - wins);

  td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
  const win  = td.querySelector('.win');
  const loss = td.querySelector('.loss');
  const val  = td.querySelector('.val');

  win.style.width  = pct + '%';
  loss.style.left  = pct + '%';
  loss.style.width = (100 - pct) + '%';
  val.textContent  = (g ? `${wins}–${losses} (${Math.round(pct)}%)` : '0–0 (0%)');

  td.title = g ? `Wins: ${wins}, Losses: ${losses}, WR: ${pct.toFixed(1)}%` : 'No games';
}
document.addEventListener('DOMContentLoaded',()=>{ document.querySelectorAll('.tabs[id]').forEach(root=>initTabsAutoSort(root.id)); });
</script>
</head>
<body>
"""

HTML_FOOT = """
</body>
</html>
"""

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
# DB helpers
# ------------------------------

def weighted_percentile(values, weights, p):
    """
    Painotettu prosenttipiste p (0..100) ilman numpyä.
    values: lista arvoja
    weights: vastaavat painot (>=0)
    """
    if not values:
        return 0.0
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total = sum(w for _, w in pairs)
    if total <= 0:
        # fallback: tavallinen mediaani
        k = len(pairs) // 2
        return pairs[k][0]
    threshold = total * (p / 100.0)
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= threshold:
            return v
    return pairs[-1][0]

def weighted_median(values, weights):
    return weighted_percentile(values, weights, 50)

def esc_title(s: str) -> str:
    # Poistaa yksittäiset heittomerkit ja korvaa rivinvaihdot HTML:lle sopiviksi
    return (s or "").replace("'", "").replace("\n", "&#10;")

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

def _safe_div(a, b):
    return (a / b) if b else 0.0

def compute_division_player_summary(con, division_id: int, min_rounds: int = 40, min_flashes: int = 10):
    """
    Division summary + Leaders (korjattu):
      - ADR/KR lasketaan kierros-painotettuna: sum(adr_i * rounds_i) / sum(rounds_i), KR = sum(kills)/sum(rounds)
      - Per-round -leaderit vaativat min_rounds (oletus 40)
      - "Most Enemies Flashed / round" vaatii lisäksi min_flashes (oletus 10 heittoa)
      - Clutcher = 1v1 + 1v2 WR, vaatii min 10 yritystä
      - Entry WR vaatii min 10 duelia
      - Top Fragger / Most Deaths ovat absoluuttisia summia (ei min_rounds)
    """
    rows = q(con, """
      SELECT
        ps.player_id,
        COALESCE(pl.nickname, MAX(ps.nickname)) AS nick,
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

        -- kierrokset per kartta rivillä -> käytä painotukseen
        SUM(mp.score_team1 + mp.score_team2)                             AS rounds,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.adr,0) )    AS adr_weighted,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.kr,0) )     AS kr_weighted

      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      LEFT JOIN players pl ON pl.player_id = ps.player_id
      LEFT JOIN teams   t  ON t.team_id   = ps.team_id
      WHERE m.division_id = ?
      GROUP BY ps.player_id
    """, (division_id,))

    # Joukkue-, kartta- ja kierrosmäärät summaryyn
    teams = q(con, """
      SELECT COUNT(*) AS c FROM (
        SELECT DISTINCT team1_id AS tid FROM matches WHERE division_id=? AND team1_id IS NOT NULL
        UNION
        SELECT DISTINCT team2_id AS tid FROM matches WHERE division_id=? AND team2_id IS NOT NULL
      )
    """, (division_id, division_id,))[0]["c"] or 0

    maps_cnt = q(con, """
      SELECT COUNT(*) AS c
      FROM maps mp JOIN matches m ON m.match_id=mp.match_id
      WHERE m.division_id=?
    """, (division_id,))[0]["c"] or 0

    total_rounds = q(con, """
      SELECT SUM(mp.score_team1 + mp.score_team2) AS r
      FROM maps mp JOIN matches m ON m.match_id=mp.match_id
      WHERE m.division_id=?
    """, (division_id,))[0]["r"] or 0

    # Jakaumat (painotettu p25/p50/p75)
    kd_vals, kd_w = [], []
    adr_vals, adr_w = [], []
    kr_vals,  kr_w  = [], []
    surv_vals, surv_w = [], []
    r1_vals, r1_w   = [], []

    # Leaders-poolit
    leaders_pool = []         # vain rounds >= min_rounds
    totals_kills = []
    totals_deaths = []

    for r in rows:
        nick = r["nick"] or r["player_id"]
        team = r.get("team_name") or "-"
        rounds = r["rounds"] or 0

        kills   = r["kills"] or 0
        deaths  = r["deaths"] or 0
        assists = r["assists"] or 0

        # Kierros-painotetut mittarit
        adr = (r["adr_weighted"] / rounds) if rounds else 0.0
        kr  = (kills / rounds) if rounds else 0.0
        kd  = (kills / deaths) if deaths else float(kills)   # jos 0 kuolemaa, aseta KD = kills

        # Survival% ja Rating1 (HLTV1.0-approks.)
        deaths_pr = (deaths / rounds) if rounds else 0.0
        survival_pct = max(0.0, 1.0 - deaths_pr) * 100.0
        surv_ratio = survival_pct / 100.0
        rating1 = ((kr / 0.679) + (surv_ratio / 0.317) + (adr / 79.9)) / 3.0 if rounds else 0.0

        # jakaumiin painotus = pelatut kierrokset
        if rounds > 0:
            kd_vals.append(kd);          kd_w.append(rounds)
            adr_vals.append(adr);        adr_w.append(rounds)
            kr_vals.append(kr);          kr_w.append(rounds)
            surv_vals.append(survival_pct); surv_w.append(rounds)
            r1_vals.append(rating1);     r1_w.append(rounds)

        # absoluuttiset leaderit
        totals_kills.append( (nick, team, kills) )
        totals_deaths.append((nick, team, deaths))

        # per-round leaderien rajaus
        if rounds >= min_rounds:
            udpr = (r["util_total"] or 0) / rounds
            flashed_pr = (r["flashed_total"] or 0) / rounds
            assist_pr  = assists / rounds

            # entry/clutch – rajat
            ewin = r["entry_wins"] or 0
            eatt = r["entry_count"] or 0
            entry_wr = (100.0 * ewin / eatt) if eatt >= 10 else -1.0

            c11w = r["c11_wins"] or 0; c11a = r["c11_atts"] or 0
            c12w = r["c12_wins"] or 0; c12a = r["c12_atts"] or 0
            c_wins = c11w + c12w
            c_atts = c11a + c12a
            clutch_wr = (100.0 * c_wins / c_atts) if c_atts >= 10 else -1.0

            # Laske enemies-per-flash (EB/F) ja käytä sitä leaderiin
            flashed_total = r["flashed_total"] or 0
            flash_cnt_total = r["flash_cnt_total"] or 0

            if flash_cnt_total >= min_flashes and rounds >= min_rounds:
                enemies_per_flash = flashed_total / flash_cnt_total
            else:
                enemies_per_flash = -1.0  # suodata pois leader-vertailusta

            leaders_pool.append({
                "nick": nick, "team": team, "rounds": rounds,
                "kd": kd, "adr": adr, "kr": kr,
                "udpr": udpr,
                "enemies_per_flash": enemies_per_flash,   # <-- käytä tätä
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
        # suodata ulos negatiiviset "ei kelpaa" -arvot
        valid = [x for x in leaders_pool if x[metric] is not None and x[metric] >= 0]
        if not valid:
            return ("-", "-", 0.0)
        b = max(valid, key=lambda x: x[metric])
        return (b["nick"], b["team"], b[metric])

    top_frg_total     = max(totals_kills,  key=lambda x: x[2]) if totals_kills  else ("-", "-", 0)
    most_deaths_total = max(totals_deaths, key=lambda x: x[2]) if totals_deaths else ("-", "-", 0)

    leaders = {
        "top_frg_total":     top_frg_total,        # (nick, team, kills)
        "most_deaths_total": most_deaths_total,    # (nick, team, deaths)
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

def _percentile(vals, p):
    """Pieni prosenttipiste-funktio ilman numpyä (p 0..100)."""
    if not vals:
        return None
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    k = (len(vals)-1) * (p/100.0)
    f = int(k)
    c = min(f+1, len(vals)-1)
    if f == c:
        return vals[f]
    d0 = vals[f] * (c - k)
    d1 = vals[c] * (k - f)
    return d0 + d1

def compute_division_thresholds(con, division_id: int):
    rows = q(con, """
      SELECT
        ps.player_id,
        SUM(ps.kills)                     AS kills,
        SUM(ps.deaths)                    AS deaths,
        AVG(ps.adr)                       AS adr,
        AVG(ps.kr)                        AS kr,
        AVG(ps.hs_pct)                    AS hs_pct,
        SUM(ps.utility_damage)            AS util,
        SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) AS rounds,
        SUM(COALESCE(ps.entry_wins,0))    AS entry_wins,
        SUM(COALESCE(ps.entry_count,0))   AS entry_count,
        SUM(COALESCE(ps.cl_1v1_Wins,0))    AS cl_1v1_Wins,
        SUM(COALESCE(ps.cl_1v1_Attempts,0))   AS cl_1v1_Attempts,
        SUM(COALESCE(ps.cl_1v2_Wins,0))    AS cl_1v2_Wins,
        SUM(COALESCE(ps.cl_1v2_Attempts,0))   AS cl_1v2_Attempts,
        SUM(COALESCE(ps.enemies_flashed,0)) AS enemies_flashed,
        SUM(COALESCE(ps.flash_count,0))     AS flash_count
      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      JOIN maps mp   ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      WHERE m.division_id = ?
      GROUP BY ps.player_id
    """, (division_id,))


    kd_vals, adr_vals, kr_vals, hs_pct_vals, udpr_vals, impact_vals = [], [], [], [], [], []
    entrywr_vals, cl_1v1_vals, cl_1v2_vals, enem_per_flash_vals  = [], [], [], []
    survival_vals, rating1_vals = [], []

    for r in rows:
        kills  = r["kills"] or 0
        deaths  = r["deaths"] or 0
        kd = (kills / deaths) if deaths else float(kills)

        adr = r["adr"] or 0.0
        kr  = r["kr"] or 0.0
        hs_pct  = r["hs_pct"] or 0.0

        rounds = r["rounds"] or 0
        util   = r["util"] or 0
        udpr   = (util / rounds) if rounds else 0.0

        # Impact (yksinkertainen placeholder: ar/dr ei ole erikseen)
        ar = 0.0
        dr = 0.0
        impact = 2.0*kr + 0.42*ar - 0.41*dr

        # --- UUSI: Survival% ja Rating1 ---
        deaths_per_round = (deaths / rounds) if rounds else 0.0
        survival = max(0.0, 1.0 - deaths_per_round) * 100.0  # prosentteina 0..100

        # HLTV Rating 1.0 -kaava (yleisesti käytetty approksimaatio)
        # KPR = kr, Survival osuutena 0..1, ADR sellaisenaan
        survival_ratio = survival / 100.0
        rating1 = ((kr / 0.679) + (survival_ratio / 0.317) + (adr / 79.9)) / 3.0

        # Entry WR (%)
        ewin = r["entry_wins"]  or 0
        eatt = r["entry_count"] or 0
        entry_wr = (100.0 * ewin / eatt) if eatt else None

        # Winratet (%)
        c11_att = r.get("c11_att", 0) or 0
        c11_win = r.get("c11_win", 0) or 0
        c11_wr = (c11_win / c11_att * 100.0) if c11_att else 0.0

        c12_att = r.get("c12_att", 0) or 0
        c12_win = r.get("c12_win", 0) or 0
        c12_wr = (c12_win / c12_att * 100.0) if c12_att else 0.0

        efl = r["enemies_flashed"] or 0
        fct = r["flash_count"]     or 0
        enem_per_flash = (efl / fct) if fct else None  # None jos ei heittoja
        if enem_per_flash is not None:
            enem_per_flash_vals.append(enem_per_flash)


        kd_vals.append(kd)
        adr_vals.append(adr)
        kr_vals.append(kr)
        hs_pct_vals.append(hs_pct)
        udpr_vals.append(udpr)
        impact_vals.append(impact)
        entrywr_vals.append(entry_wr)
        cl_1v1_vals.append(c11_wr)
        cl_1v2_vals.append(c12_wr)
        survival_vals.append(survival)
        rating1_vals.append(rating1)

    def _percentile(lst, q):
        lst = sorted(lst)
        if not lst: return 0.0
        pos = (len(lst)-1) * q
        i = int(pos)
        frac = pos - i
        if i+1 < len(lst):
            return lst[i] + frac * (lst[i+1] - lst[i])
        return lst[i]

    def pack(lst, fallback=(0.0, 0.5, 1.0)):
        lst = [v for v in lst if v is not None]
        if not lst:
            return fallback
        p25 = _percentile(lst, 0.25)
        p50 = _percentile(lst, 0.50)
        p75 = _percentile(lst, 0.75)
        # varmistetaan että ala- ja ylärajat eivät ole identtiset
        if p25 == p75:
            p25 = min(p25, p25*0.9)
            p75 = max(p75, p75*1.1 if p75 != 0 else 0.1)
        return (p25, p50, p75)

    return {
        "kd":       pack(kd_vals),
        "adr":      pack(adr_vals),
        "kr":       pack(kr_vals),
        "hs_pct":       pack(hs_pct_vals),
        "udpr":     pack(udpr_vals),
        "impact":   pack(impact_vals),
        "entry_wr": pack(entrywr_vals, fallback=(30.0, 50.0, 70.0)),
        "c11_wr": pack(cl_1v1_vals, fallback=(30.0, 50.0, 70.0)),
        "c12_wr": pack(cl_1v2_vals, fallback=(30.0, 50.0, 70.0)),
        "enem_flash": pack(enem_per_flash_vals, fallback=(0.3, 0.6, 0.9)),
        "survival": pack(survival_vals, fallback=(30.0, 50.0, 70.0)),   # prosenttiasteikko
        "rating1":  pack(rating1_vals,  fallback=(0.85, 1.00, 1.15)),  # tyypilliset haarukat
    }

def compute_division_map_summary(con, division_id: int):
    """
    Division-tason karttayhteenveto:
      - top_played:  top-3 pelatuimmat kartat (maps-taulusta)
      - top_banned:  top-3 bannatuimmat kartat (map_votes.status='drop')
    Palauttaa: { 'top_played': [(map, cnt),...], 'top_banned': [(map, cnt),...] }
    """
    played_rows = q(con, """
        SELECT mp.map_name AS map_name, COUNT(*) AS c
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.division_id = ? AND mp.map_name IS NOT NULL
        GROUP BY mp.map_name
        ORDER BY c DESC, mp.map_name ASC
        LIMIT 3
    """, (division_id,))
    top_played = [(r["map_name"], r["c"]) for r in played_rows]

    ban_rows = q(con, """
        SELECT v.map_name AS map_name, COUNT(*) AS c
        FROM map_votes v
        JOIN matches m ON m.match_id = v.match_id
        WHERE m.division_id = ?
          AND v.status = 'drop'
          AND v.map_name IS NOT NULL
        GROUP BY v.map_name
        ORDER BY c DESC, v.map_name ASC
        LIMIT 3
    """, (division_id,))
    top_banned = [(r["map_name"], r["c"]) for r in ban_rows]

    return {"top_played": top_played, "top_banned": top_banned}

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
    HAS_PISTOL = has_column(con, "player_stats", "pistol_kills")
    HAS_FLASH  = (has_column(con, "player_stats", "enemies_flashed")
                  and has_column(con, "player_stats", "flash_count"))
    HAS_FLASH_SUCC = has_column(con, "player_stats", "flash_successes")
    HAS_MVPS  = has_column(con, "player_stats", "mvps")

    select_cols = [
        "ps.player_id AS player_id",
        "COALESCE(pl.nickname, MAX(ps.nickname)) AS nickname_display",
        "COUNT(*) AS maps_played",
        "SUM(COALESCE(ps.kills,0)) AS kills",
        "SUM(COALESCE(ps.deaths,0)) AS deaths",
        "SUM(COALESCE(ps.assists,0)) AS assists",
        "AVG(COALESCE(ps.adr,0)) AS adr",
        "AVG(COALESCE(ps.kr,0)) AS kr",
        "AVG(COALESCE(ps.hs_pct,0)) AS hs_pct",
        "SUM(COALESCE(ps.sniper_kills,0)) AS awp_kills",
        "SUM(COALESCE(ps.mk_3k,0)) AS k3",
        "SUM(COALESCE(ps.mk_4k,0)) AS k4",
        "SUM(COALESCE(ps.mk_5k,0)) AS k5",
        "SUM(COALESCE(ps.utility_damage,0)) AS util",
    ]
    if HAS_MVPS:
        select_cols.append("SUM(COALESCE(ps.mvps,0)) AS mvps")
    if HAS_FLASH:
        select_cols += [
            "SUM(COALESCE(ps.enemies_flashed,0)) AS flashed",
            "SUM(COALESCE(ps.flash_count,0)) AS flash_count",
        ]
    if HAS_FLASH_SUCC:
        select_cols.append("SUM(COALESCE(ps.flash_successes,0)) AS flash_successes")

    # rounds + clutch/entry
    select_cols += [
        "SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) AS rounds",
        "SUM(COALESCE(ps.clutch_kills,0))    AS clutch_kills",
        "SUM(COALESCE(ps.cl_1v1_attempts,0)) AS c11_att",
        "SUM(COALESCE(ps.cl_1v1_wins,0))     AS c11_win",
        "SUM(COALESCE(ps.cl_1v2_attempts,0)) AS c12_att",
        "SUM(COALESCE(ps.cl_1v2_wins,0))     AS c12_win",
        "SUM(COALESCE(ps.entry_count,0))     AS entry_att",
        "SUM(COALESCE(ps.entry_wins,0))      AS entry_win",
    ]
    if HAS_PISTOL:
        select_cols.append("SUM(COALESCE(ps.pistol_kills,0)) AS pistol_kills")

    sql = f"""
      SELECT
        {", ".join(select_cols)}
      FROM player_stats ps
      JOIN matches m
        ON m.match_id = ps.match_id
      JOIN maps mp
        ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      LEFT JOIN players pl
        ON pl.player_id = ps.player_id
      WHERE m.division_id = ? AND ps.team_id = ?
      GROUP BY ps.player_id
      ORDER BY kills DESC
    """
    rows = q(con, sql, (division_id, team_id))


    table = []
    for r in rows:
        kills = r["kills"] or 0
        deaths = r["deaths"] or 0
        assists = r["assists"] or 0
        kd = (kills / deaths) if deaths else float(kills)
        rounds = r["rounds"] or 0
        maps_played = r["maps_played"] or 0
        rpm = (rounds / maps_played) if maps_played else 0.0

        row = {
            "player_id": r["player_id"],
            "nickname": r["nickname_display"],           # näytetään aina uusin nimi
            "maps_played": maps_played,
            "rounds": rounds,
            "rpm": rpm,
            "kd": kd,
            "adr": r["adr"] or 0.0,
            "kr": r["kr"] or 0.0,

            # kokonaismäärät erikseen, jotta HTML voi näyttää ne yksittäisinä sarakkeina
            "kill": kills,
            "death": deaths,
            "assist": assists,
            "mvps": r.get("mvps", 0) or 0,

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

        deaths_per_round = (deaths / rounds) if rounds else 0.0
        survival_pct = max(0.0, 1.0 - deaths_per_round) * 100.0  # 0..100 %

        survival_ratio = survival_pct / 100.0
        rating1 = ((row["kr"] / 0.679) + (survival_ratio / 0.317) + (row["adr"] / 79.9)) / 3.0
        row["survival_pct"] = survival_pct
        row["rating1"]      = rating1

        # Valinnaiset sarakkeet jos kannassa on
        if "pistol_kills" in r.keys():
            row["pistol_kills"] = r["pistol_kills"] or 0
        if "flashed" in r.keys():        row["flashed"] = r["flashed"] or 0
        if "flash_count" in r.keys():    row["flash_count"] = r["flash_count"] or 0
        if "flash_successes" in r.keys():row["flash_successes"] = r["flash_successes"] or 0


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

            # WR:t prosentteina kuten ennen
            "wr": wr,
            "wr_own": wr_own,
            "wr_opp": wr_opp,

            "wins": w,
            "games": played,
            "wins_own": own_pick_wins,
            "games_own": own_pick_games,
            "wins_opp": opp_pick_wins,
            "games_opp": opp_pick_games,

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
    thresholds = compute_division_thresholds(con, div["division_id"])
    
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = [HTML_HEAD.replace("{title}", f"{div['name']} - generoitu {ts}")]
    html.append('<div class="page">')

    html.append(f"<h1>{div['name']} <span class='muted'>(generoitu {ts})</span></h1>")
    html.append('<div class="nav">')
    for t in teams:
        name = t["team_name"] or t["team_id"]
        avatar = t.get("avatar")
        logo = f'<img class="logo" src="{avatar}" alt="">' if avatar else ''
        html.append(f'<a href="#team-{t["team_id"]}">{logo}{escape(name)}</a>')
    html.append("</div>")

    # --- Divisioonan lyhyt yhteenveto pelaajista ---
    divsum = compute_division_player_summary(con, div["division_id"], min_rounds=20)
    mp_sum = compute_division_map_summary(con, div["division_id"])

    html.append('<div class="div-summary">')

    # Vasemman puolen "perusluvut" kortti
    html.append('<div class="card">')
    html.append('<h3>Division summary</h3>')
    TOOLTIP_WMED = ("Painotettu mediaani: pelaajakohtaiset arvot lajitellaan, "
                    "paino = pelatut kierrokset divisioonassa. p50 on pienin arvo, "
                    "jossa kumulatiiviset painot ylittävät 50% (p25/p75 vastaavasti 25%/75%).")


    html.append('<div class="summary-grid">')
    html.append(f'<div class="summary-item"><div class="label">Teams</div><div class="val">{divsum["teams"]}</div></div>')
    html.append(f'<div class="summary-item"><div class="label">Players</div><div class="val">{divsum["players"]}</div></div>')
    html.append(f'<div class="summary-item"><div class="label">Maps</div><div class="val">{divsum["maps"]}</div></div>')
    html.append(f'<div class="summary-item"><div class="label">Rounds</div><div class="val">{divsum["rounds"]}</div></div>')

    # KD: mediaani + IQR
    html.append(
        f'<div class="summary-item" title="{TOOLTIP_WMED}">'
        f'  <div class="label">Median KD (p50)</div>'
        f'  <div class="val">{divsum["kd_p50"]:.2f}</div>'
        f'  <div class="label">p25-p75</div>'
        f'  <span class="cell-muted" style="font-weight:400;">{divsum["kd_p25"]:.2f}-{divsum["kd_p75"]:.2f}</span>'
        f'</div>'
    )

    # ADR: mediaani + IQR
    html.append(
        f'<div class="summary-item" title="{TOOLTIP_WMED}">'
        f'<div class="label">Median ADR (p50)</div>'
        f'<div class="val">{divsum["adr_p50"]:.1f} '
        f'<div class="label">p25-p75</div>'
        f'<span class="cell-muted" style="font-weight:400;">{divsum["adr_p25"]:.1f}-{divsum["adr_p75"]:.1f}</span>'
        f'</div></div>'
    )

    # KR: mediaani + IQR
    html.append(
        f'<div class="summary-item" title="{TOOLTIP_WMED}">'
        f'<div class="label">Median KR (p50)</div>'
        f'<div class="val">{divsum["kr_p50"]:.2f} '
        f'<div class="label">p25-p75</div>'
        f'<span class="cell-muted" style="font-weight:400;">{divsum["kr_p25"]:.2f}-{divsum["kr_p75"]:.2f}</span>'
        f'</div></div>'
    )

    # Survival%: mediaani + IQR
    html.append(
        f'<div class="summary-item" title="{TOOLTIP_WMED}">'
        f'<div class="label">Median Survival% (p50)</div>'
        f'<div class="val">{divsum["surv_p50"]:.0f}% '
        f'<div class="label">p25-p75</div>'
        f'<span class="cell-muted" style="font-weight:400;">{divsum["surv_p25"]:.0f}%–{divsum["surv_p75"]:.0f}%</span>'
        f'</div></div>'
    )

    # Rating1: mediaani + IQR
    html.append(
        f'<div class="summary-item" title="{esc_title(TOOLTIP_RATING1)}">'
        f'<div class="label">Median Rating1 (p50)</div>'
        f'<div class="val">{divsum["r1_p50"]:.2f} '
        f'<div class="label">p25-p75</div>'
        f'<span class="cell-muted" style="font-weight:400;">{divsum["r1_p25"]:.2f}-{divsum["r1_p75"]:.2f}</span>'
        f'</div></div>'
    )
    # Top 3 pelatuimmat kartat (ruutuna)
    lines = "<br>".join([f"{pretty_map_name(n)} <span class='cell-muted'>({c}×)</span>" for n, c in mp_sum["top_played"]])
    html.append(
        f"<div class='summary-item'>"
        f"  <div class='label'>Most played (top3)</div>"
        f"  <div class='val' style='line-height:1.25'>{lines}</div>"
        f"</div>"
    )

    # Top 3 bannatuimmat kartat (ruutuna)
    lines = "<br>".join([f"{pretty_map_name(n)} <span class='cell-muted'>({c}×)</span>" for n, c in mp_sum["top_banned"]])
    html.append(
        f"<div class='summary-item'>"
        f"  <div class='label'>Most banned (top3)</div>"
        f"  <div class='val' style='line-height:1.25'>{lines}</div>"
        f"</div>"
    )


    html.append('</div>')  # /summary-grid

    html.append('</div>')  # /card

    # Oikean puolen "Leaders" kortti
    html.append('<div class="card leaders">')
    html.append('<h3>Leaders (min 40 rounds, except totals)</h3>')
    html.append('<table><thead><tr><th>Metric</th><th>Player</th><th>Value</th></tr></thead><tbody>')

    # Absoluuttiset
    html.append(f'<tr><td>Top Fragger (total kills)</td>'
                f'<td>{escape(divsum["leaders"]["top_frg_total"][0])} <span class="cell-muted">({escape(divsum["leaders"]["top_frg_total"][1])})</span></td>'
                f'<td>{int(divsum["leaders"]["top_frg_total"][2])}</td></tr>')

    html.append(f'<tr><td>Most Deaths (total)</td>'
                f'<td>{escape(divsum["leaders"]["most_deaths_total"][0])} <span class="cell-muted">({escape(divsum["leaders"]["most_deaths_total"][1])})</span></td>'
                f'<td>{int(divsum["leaders"]["most_deaths_total"][2])}</td></tr>')

    # Per round -mittarit (min 40 rounds)
    html.append(f'<tr><td>Top ADR</td>'
                f'<td>{escape(divsum["leaders"]["adr"][0])} <span class="cell-muted">({escape(divsum["leaders"]["adr"][1])})</span></td>'
                f'<td>{divsum["leaders"]["adr"][2]:.1f}</td></tr>')

    html.append(f'<tr><td>Top KD</td>'
                f'<td>{escape(divsum["leaders"]["kd"][0])} <span class="cell-muted">({escape(divsum["leaders"]["kd"][1])})</span></td>'
                f'<td>{divsum["leaders"]["kd"][2]:.2f}</td></tr>')

    html.append(f'<tr><td>Top Utility (UDPR)</td>'
                f'<td>{escape(divsum["leaders"]["udpr"][0])} <span class="cell-muted">({escape(divsum["leaders"]["udpr"][1])})</span></td>'
                f'<td>{divsum["leaders"]["udpr"][2]:.2f}</td></tr>')

    html.append(
      f'<tr><td>Most Enemies Blinded / flash</td>'
      f'<td>{escape(divsum["leaders"]["enemies_per_flash"][0])} '
      f'<span class="cell-muted">({escape(divsum["leaders"]["enemies_per_flash"][1])})</span></td>'
      f'<td>{divsum["leaders"]["enemies_per_flash"][2]:.2f}</td></tr>')

    html.append(f'<tr><td>Top Support (assists / round)</td>'
                f'<td>{escape(divsum["leaders"]["assist_pr"][0])} <span class="cell-muted">({escape(divsum["leaders"]["assist_pr"][1])})</span></td>'
                f'<td>{divsum["leaders"]["assist_pr"][2]:.2f}</td></tr>')

    html.append(f'<tr><td>Top Entry (winrate)</td>'
                f'<td>{escape(divsum["leaders"]["entry_wr"][0])} <span class="cell-muted">({escape(divsum["leaders"]["entry_wr"][1])})</span></td>'
                f'<td>{divsum["leaders"]["entry_wr"][2]:.1f}%</td></tr>')

    html.append(f'<tr><td>Top Clutcher (winrate)</td>'
                f'<td>{escape(divsum["leaders"]["clutch_wr"][0])} <span class="cell-muted">({escape(divsum["leaders"]["clutch_wr"][1])})</span></td>'
                f'<td>{divsum["leaders"]["clutch_wr"][2]:.1f}%</td></tr>')

    html.append('</tbody></table>')
    html.append('</div>')


    html.append('</div>')  # /div-summary


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
        # Basic headers (esim. id = tid_basic)
        html.append(f"""<thead><tr>
          <th onclick="sortTable('{tid_basic}',0,false)">Nickname</th>
          <th onclick="sortTable('{tid_basic}',1,true)">Maps</th>
          <th onclick="sortTable('{tid_basic}',2,true)" title="Total rounds">Rounds</th>
          <th onclick="sortTable('{tid_basic}',3,true)" title="Kills/Deaths">KD</th>
          <th onclick="sortTable('{tid_basic}',4,true)">ADR</th>
          <th onclick="sortTable('{tid_basic}',5,true)">KR</th>
          <th onclick="sortTable('{tid_basic}',6,true)">Kills</th>
          <th onclick="sortTable('{tid_basic}',7,true)">Deaths</th>
          <th onclick="sortTable('{tid_basic}',8,true)">Assists</th>
          <th onclick="sortTable('{tid_basic}',9,true)">HS%</th>
          <th onclick="sortTable('{tid_basic}',11,true)">3K</th>
          <th onclick="sortTable('{tid_basic}',12,true)">4K</th>
          <th onclick="sortTable('{tid_basic}',13,true)">ACE</th>
          <th onclick="sortTable('{tid_basic}',14,true)">MVPs</th>
          </tr></thead>""")
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
            <td>{p["k3"]}</td>
            <td>{p["k4"]}</td>
            <td>{p["k5"]}</td>
            <td>{p["mvps"]}</td>
          </tr>""")
        html.append("</tbody></table>")

        html.append(f"""
        <script>
        postProcessTable('{tid_basic}', {{
          color: [
            {{col:3, p:[{thresholds['kd'][0]:.4f}, {thresholds['kd'][1]:.4f}, {thresholds['kd'][2]:.4f}] }},
            {{col:4, p:[{thresholds['adr'][0]:.4f}, {thresholds['adr'][1]:.4f}, {thresholds['adr'][2]:.4f}] }},
            {{col:5, p:[{thresholds['kr'][0]:.4f}, {thresholds['kr'][1]:.4f}, {thresholds['kr'][2]:.4f}]  }},
            {{col:9, p:[{thresholds['hs_pct'][0]:.4f}, {thresholds['hs_pct'][1]:.4f}, {thresholds['hs_pct'][2]:.4f}]  }}
          ],
          defaultSort: {{col:0, dir:'asc'}},
          toggles: true
        }});
        </script>
        """)

        #html.append(f"<script>applyDefaultSort('{tid_basic}');</script>")
        html.append("</div>")  # /tab-panel basic

        # ---------- ADVANCED ----------
        tid_adv = f"players-adv-{ti}"
        html.append(f'<div class="tab-panel" data-tab="advanced">')
        html.append(f'<table id="{tid_adv}" data-sort-col="7" data-sort-dir="desc">')

        # Otsikot
        html.append("<thead><tr>")
        col_idx = 0
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},false)\">Nickname</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Clutch-fragit 1vX-tilanteissa'>Clutch Kills</th>"); col_idx += 1
        # WR-palkit: 1v1, 1v2 ja yhdistetty Entry
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='1v1 clutch winrate (W–L, %)'>1v1 WR</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='1v2 clutch winrate (W–L, %)'>1v2 WR</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Entry duels winrate (W–L, %)'>Entry WR</th>"); col_idx += 1


        # Util, UDPR, Impact
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Total utility damage'>Util dmg</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Utility damage per round'>UDPR</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Percentage of rounds survived'>Survival %</th>"); col_idx += 1
        html.append(
            f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" "
            f"title='{esc_title(TOOLTIP_RATING1)}'>Rating1</th>"
        ); col_idx += 1



        # Flash-sarakkeet vain jos dataa
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of flashbang grenades thrown by the player'>Flash Cnt</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of enemies blinded by flashes'>Flashed</th>");   col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Count of successful flashes that actually blinded enemies'>Flash Succ</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Ratio: enemies blinded per flash thrown'>Enem/Flash</th>"); col_idx += 1

        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of pistol kills'>Pistol Kills</th>"); col_idx += 1
        html.append(f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of sniper kills'>Sniper Kills</th>"); col_idx += 1

        html.append("</tr></thead><tbody>")

        for p in players:
          html.append("<tr>")
          html.append(f"<td>{p['nickname']}</td>")
          html.append(f"<td>{p['clutch_kills']}</td>")
          # 1v1 WR palkki
          html.append(
            f"<td class='wr' data-zero='show' data-g='{p['c11_att']}' data-w='{p['c11_win']}' "
            f"data-pct='{p['c11_wr']:.1f}' title='Attempts: {p['c11_att']}, Wins: {p['c11_win']}'>"
            f"</td>"
          )

          # 1v2 WR palkki
          html.append(
            f"<td class='wr' data-zero='show' data-g='{p['c12_att']}' data-w='{p['c12_win']}' "
            f"data-pct='{p['c12_wr']:.1f}' title='Attempts: {p['c12_att']}, Wins: {p['c12_win']}'>"
            f"</td>"
          )

          # Entry WR yhdistettynä (W–L näkyy palkissa)
          html.append(
            f"<td class='wr' data-zero='show' data-g='{p['entry_att']}' data-w='{p['entry_win']}' "
            f"data-pct='{p['entry_wr']:.1f}' title='Attempts: {p['entry_att']}, Wins: {p['entry_win']}'>"
            f"</td>"
          )


          # Utility: total + per round + impact
          html.append(f"<td>{int(p['util'])}</td>")
          html.append(f"<td>{p['udpr']:.2f}</td>")
          html.append(f"<td>{p['survival_pct']:.0f}</td>")
          html.append( f"<td title='{esc_title(TOOLTIP_RATING1)}'>{p['rating1']:.2f}</td>")

          # Flash-sarakkeet (jos dataa)
          
          html.append(f"<td>{p.get('flash_count',0)}</td>")
          html.append(f"<td>{p.get('flashed',0)}</td>")
          html.append(f"<td>{p.get('flash_successes',0)}</td>")
          val = p['enemies_per_flash'] if p['enemies_per_flash'] is not None else 0.0
          html.append(f"<td>{val:.2f}</td>")

          html.append(f"<td>{p.get('pistol_kills',0)}</td>")
          html.append(f"<td>{p.get('awp_kills',0)}</td>")
          html.append("</tr>")

        html.append("</tbody></table>")

        html.append(f"""
        <script>
        postProcessTable('{tid_adv}', {{
          wrbars: [2, 3, 4],
          color: [
            {{col:6,  p:[{thresholds['udpr'][0]:.4f}, {thresholds['udpr'][1]:.4f}, {thresholds['udpr'][2]:.4f}]}},
            {{col:7,  p:[{thresholds['survival'][0]:.4f}, {thresholds['survival'][1]:.4f}, {thresholds['survival'][2]:.4f}]}},
            {{col:8,  p:[{thresholds['rating1'][0]:.4f},  {thresholds['rating1'][1]:.4f},  {thresholds['rating1'][2]:.4f}]}},
            {{col:10, p:[{thresholds['enem_flash'][0]:.4f}, {thresholds['enem_flash'][1]:.4f}, {thresholds['enem_flash'][2]:.4f}]}}
          ],
          defaultSort: {{col:0, dir:'asc'}},
          toggles: true
        }});
        </script>
        """)
        #html.append(f"<script>applyDefaultSort('{tid_adv}');</script>")
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
            <!-- WR %: kokonaisuus -->
            <td class="wr" data-w="{r['wins']}" data-g="{r['games']}" data-pct="{r['wr']:.1f}"></td>

            <!-- WR own pick % -->
            <td class="wr" data-w="{r['wins_own']}" data-g="{r['games_own']}" data-pct="{r['wr_own']:.1f}"></td>

            <!-- WR opp pick % -->
            <td class="wr" data-w="{r['wins_opp']}" data-g="{r['games_opp']}" data-pct="{r['wr_opp']:.1f}"></td>


            <td title="Δ vs div avg: {dkd:+.2f}">{r["kd"]:.2f}</td>
            <td title="Δ vs div avg: {dadr:+.1f}">{r["adr"]:.1f}</td>
            <td>{r["rd"]}</td>
            <td>{r["ban1"]}</td>
            <td>{r["ban2"]}</td>
            <td>{r["opp_ban"]}</td>
            <td>{r["total_own_ban"]}</td>
            </tr>""")
        html.append("</tbody></table>")
        html.append(f"""
        <script>
        postProcessTable('{tid2}', {{
          // WR-sarakkeiden split-palkit datasta
          wrbars: [4,5,6],

          // (valinnaisesti) dynaaminen väritys KD/ADR/RD tms. – pidä oma nykyinen listasi:
          color: [
            {{col:7, p:[{thresholds['kd'][0]:.4f}, {thresholds['kd'][1]:.4f}, {thresholds['kd'][2]:.4f}] }},
            {{col:8, p:[{thresholds['adr'][0]:.4f}, {thresholds['adr'][1]:.4f}, {thresholds['adr'][2]:.4f}] }},
            // RD esimerkkinä kiinteällä alueella:
            // colorizeRangea jos käytät, tai pidä oma toteutuksesi
          ],
          defaultSort: {{col:0, dir:'asc'}},
          toggles: true
        }});
        bindPlayedOnly('{tid2}', '{tid2}-played-only');
        </script>
        """)

        html.append("</details>")  # team section

    html.append('</div>')  # .page
    html.append(HTML_FOOT)
    out_path = OUT_DIR / f"{div['slug']}.html"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(html), encoding="utf-8")
    return out_path


def write_index():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = [HTML_HEAD.replace("{title}", f"CS2 Faceit Reports - generoitu {ts}")]
    html.append('<div class="page">')
    html.append(f"<h1>CS2 Faceit Reports <span class='muted'>(generoitu {ts})</span></h1>")
    html.append("<p class='muted'>Valitse divisioona alta. Sivu linkittää valmiiksi generoituun raporttiin.</p>")
    html.append('<div class="nav">')
    for div in DIVISIONS:
        html.append(f'<a href="output/{div["slug"]}.html">{div["name"]}</a>')
    html.append('</div></div>')
    html.append("</body></html>")
    Path("index.html").write_text("\n".join(html), encoding="utf-8")

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = get_conn(DB_PATH)
    con.row_factory = sqlite3.Row

    for div in DIVISIONS:
        path = render_division(con, div)
        print(f"[OK] Wrote {path}")

    # Lopuksi etusivu
    write_index()

if __name__ == "__main__":
    main()
