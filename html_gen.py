# Generate one HTML per division with team summaries, player tables, and map stats.
from pathlib import Path
import sqlite3
import os
from collections import defaultdict
from faceit_config import DIVISIONS
from datetime import datetime
from html import escape
import hashlib, tempfile, re

_TS_RX = re.compile(r"Generoitu\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}")

from db import (
    get_conn,
    get_teams_in_championship,
    compute_team_summary_data,
    compute_player_table_data,
    compute_map_stats_table_data,
    compute_champ_map_avgs_data,
    compute_champ_map_summary_data,
    compute_champ_thresholds_data,
    get_map_art, normalize_map_id,
)


DB_PATH = str(Path(__file__).with_name("pappaliiga.db"))
OUT_DIR = Path(__file__).with_name("docs")

# --- YHTEINEN POHJA KAIKILLE SIVUILLE (CSS + JS) ---
UNIFIED_HEAD = """<!doctype html>
<html lang="fi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{title}</title>
<style>
/* ---- Colors & base ---- */
:root{
  --bg: #0b1020;
  --card: #121932;
  --muted: #9db1d1;
  --fg:#e7eefc;
  --border:#27324d;
  --table-bg:#0e1730;
  --table-alt:#0f1a38;
  --head:#0f213f;
  --chip-bg:#121b36;
  --nav-bg:#121b36;
  --accent:#3aa3ff;
  --accent-2:#7dd3fc;
  --ok:#39d98a;
  --warn:#f4bf4f;
  --err:#f97066;
  --radius:14px;
  --shadow:0 10px 30px rgba(0,0,0,0.25);
}

/* Team logos: different size for index vs division */
img.logo.nav-logo {
  max-height: 40px;
  max-width: 40px;
  vertical-align: middle;
  margin-right: 6px;
  border-radius: 3px;
}

/* Bigger logos in team section headings */
img.logo.team-logo {
  max-height: 100px;
  max-width: 100px;
  vertical-align: middle;
  margin-right: 10px;
  border-radius: 6px;
}
img.logo.promo-logo {
  max-height: 200px;
  max-width: 200px;
  vertical-align: middle;
  margin-right: 6px;
  border-radius: 3px;
}

*{box-sizing:border-box;}
html,body{height:100%;}
body{
  margin:0;
  background: radial-gradient(1800px 600px at 20% -10%, #152046 0%, #0b1020 55%) fixed;
  color:var(--fg);
  font:18px/1.45 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
}
a{ color:var(--accent); text-decoration:none; }
.muted{ color:var(--muted); }

/* ---- Containers ---- */
.container{ max-width:1800px; margin:0 auto; padding:24px; }
.page{ max-width:1800px; margin:0 auto; padding:1.25rem 1.25rem 3rem; }

/* ---- Navigation / brand ---- */
.nav{ display:flex; flex-wrap:wrap; gap:.75rem; margin:0.25rem 0 1rem; }
.topbar{ display:flex; align-items:center; justify-content:space-between; gap:16px; margin: 0 0 20px 0; }
.brand{ display:flex; align-items:center; gap:12px; font-weight:700; letter-spacing:.3px; color:var(--fg); }
.brand .dot{ width:10px; height:10px; border-radius:50%; background:linear-gradient(135deg, var(--accent), var(--accent-2)); }

/* ---- Buttons ---- */
.btn{
  display:inline-flex; align-items:center; gap:8px;
  border:1px solid rgba(255,255,255,0.12);
  background:linear-gradient(180deg, rgba(58,163,255,0.12), rgba(125,211,252,0.08));
  color:var(--fg); padding:10px 14px; border-radius:10px;
  transition:.15s transform, .15s background, .15s border-color;
}
.btn:hover{ transform: translateY(-1px); border-color: rgba(255,255,255,0.25); }
.btn-primary{ background:linear-gradient(180deg, #3aa3ff, #2a7cd6); border:none; color:#fff; }
.btn-ghost{ background:linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02)); }
.btn-floating{
  position:fixed; right:18px; bottom:18px; z-index:50;
  box-shadow: var(--shadow);
}

/* ---- Hero ---- */
.hero{
  display:grid;
  grid-template-columns: 1.2fr 0.8fr;
  gap:24px;
  align-items:stretch;
  margin-bottom:28px;
}
.hero-card{
  background: linear-gradient(180deg, rgba(18,25,50,0.9), rgba(12,18,36,0.9));
  border:1px solid rgba(255,255,255,0.08);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding:24px;
  text-align:center; /* keskitetään otsikko, tekstit ja napit */
}
.hero h1{ margin:0 0 8px 0; font-size:36px; line-height:1.1; letter-spacing:.2px; }
.hero p{ margin: 0 0 16px 0; color: var(--muted); }
/* nappirivi aina keskelle */
.hero-cta{
  display:flex;
  justify-content:center;
  align-items:center;
  flex-wrap:wrap;
  gap:10px;
  margin-top:10px;
}

.badge{
  display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px;
  background: rgba(58,163,255,0.12); color:var(--accent-2); border: 1px solid rgba(125,211,252,0.25);
  font-size:13px;
}

/* ---- Grid & cards ---- */
.grid{ display:grid; gap:16px; grid-template-columns: repeat(12,1fr); }
.card {
  grid-column: span 2; /* 6 cards per row on 12-col grid */
  min-height: 100px;
  background: linear-gradient(180deg, rgba(18,25,50,0.85), rgba(12,18,36,0.85));
  border:1px solid rgba(255,255,255,0.08);
  border-radius: var(--radius);
  padding:14px;
  box-shadow: var(--shadow);
  display:flex;
  flex-direction:column;
  justify-content:space-between;
  transition: transform .15s;
  text-align:center; 
}
.card h3{ margin:0 0 4px 0; font-size:16px; }
.card small{ color: var(--muted); font-size:13px; }

.card:hover{ transform: translateY(-2px); }
.footer{ margin-top:24px; padding:16px; color:var(--muted); font-size:13px; text-align:center; opacity:.8; }

/* ---- Tables & utilities ---- */
table{ width:100%; border-collapse:collapse; margin:.5rem 0 1rem; background:var(--table-bg); }
thead th{ position:sticky; top:0; background:var(--head); border-bottom:1px solid var(--border); font-weight:600; color:var(--fg); }
th,td{ padding:.55rem .7rem; border-bottom:1px solid var(--border); text-align:center; }
tbody tr:nth-child(even){ background:var(--table-alt); }
th:first-child,td:first-child{ text-align:left; position:sticky; left:0; z-index:1; background:var(--table-bg); }
tbody tr:hover{ outline:1px solid #1e2b4a; background:#152247; }
.chips{ display:flex; gap:.5rem; flex-wrap:wrap; margin:.4rem 0 .8rem; }
/* Global chip (generic) */
.chip{ font-size:.95rem; padding:.35rem .65rem; border-radius:999px; background:var(--chip-bg); border:1px solid var(--border); color:var(--fg); }

.bar{ position:relative; background:#1b2a4a; height:20px; border-radius:8px; overflow:hidden; }
.bar>span{ position:absolute; left:0; top:0; bottom:0; width:0%; background:var(--accent); }
.bar .val{ position:relative; z-index:1; font-size:.9rem; padding-left:.5rem; color:#fff; }
.bar-split{position:relative;height:20px;border-radius:6px;overflow:hidden;background:#1b2a4a}
.bar-split .win{position:absolute;left:0;top:0;bottom:0;background:#22c55e}
.bar-split .loss{position:absolute;top:0;bottom:0;background:#ef4444}
.bar-split .val{position:relative;z-index:1;text-align:center;line-height:20px;font-size:.85rem;color:#fff}

.cell-grad.good{ background:linear-gradient(90deg, rgba(34,197,94,0.25), transparent); }
.cell-grad.bad{  background:linear-gradient(90deg, rgba(239,68,68,0.25), transparent); }
.cell-muted{ color:var(--muted); }
th[title]{ text-decoration: underline dotted #777; text-underline-offset:3px; cursor:help; }

/* ---- Division summary ---- */
.div-summary{
  display:grid;
  grid-template-columns: 1fr 1fr; /* left/right columns */
  gap:1rem;
  margin:.75rem 0 1rem;
  align-items: start;
}
.div-summary .card{ grid-column: auto !important; }
.summary-grid{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.6rem; }
.summary-item{ background:var(--table-alt); border:1px solid var(--border); border-radius:6px; padding:.5rem .6rem; text-align:center; }
.summary-item .label{ color:var(--muted); font-size:.9rem; }
.summary-item .val{ font-size:1.15rem; font-weight:600; }

/* ---- Responsiveness ---- */
@media (max-width: 960px){
  .hero{ grid-template-columns: 1fr; }
  .card{ grid-column: span 12; }
  .div-summary{ grid-template-columns: 1fr; }
  .div-summary .card{ grid-column: auto !important; }
}

/* ---- Tabs ---- */
.tabs { margin-top: 1rem; }
.tab-nav { display: flex; gap: .5rem; margin-bottom: .5rem; }
.tab-btn {
  padding: 6px 12px; border-radius: 8px;
  background: var(--chip-bg); border:1px solid var(--border);
  color: var(--fg); cursor:pointer;
}
.tab-btn.active {
  background: var(--accent); color:#fff; border-color: var(--accent);
}
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* ---- Matches mirror layout (scoped, cleaned) ---- */
.matches-mirror .matches-head{display:flex;align-items:center;justify-content:space-between;gap:.75rem;margin-bottom:.5rem;}
.matches-mirror .title{font-weight:700;}
.matches-mirror .matches-list{display:flex;flex-direction:column;gap:.6rem;}

/* Card + row */
.match-row{border:1px solid var(--border);border-radius:14px;overflow:hidden;background:var(--card);}
.match-row[open] .match-summary{border-bottom:1px solid var(--border);}
.card.matches-mirror:hover{transform:none;}
.match-row:hover{border-color:rgba(255,255,255,.12);box-shadow:none;}

.match-summary{
  list-style:none;
  display:grid;
  grid-template-columns: minmax(0,1fr) auto minmax(0,1fr); /* vasen | keskiblokki | oikea */
  align-items:center;
  gap:.65rem;
  padding:.6rem 1rem;
  cursor:pointer;
}

/* Vasen ja oikea asettuvat reunoihin, keskiblokki on aina keskellä */
.match-summary .side-left{
  text-align:left;
  justify-self:start;
}
/* oikea tiimipalsta: logo ihan kortin reunaan */
.match-summary .side-right{
  margin-left:auto;
  text-align:right;
  justify-content:flex-end;
}
/* järjestys: nimi ennen logoa, jotta logo päätyy ihan oikeaan reunaan */
.match-summary .side-right .logo{
  order: 2;
  margin-left:.55rem;
}

.match-summary .center{
  justify-self:center;
  text-align:center;
  display:flex; flex-direction:column; align-items:center; gap:.25rem;
  min-width:160px;
}

.match-summary::-webkit-details-marker{display:none;}
.match-summary .team{display:flex;align-items:center;gap:.55rem;min-width:0;}
.match-summary .team .logo{width:100px;height:100px;border-radius:8px;object-fit:contain;box-shadow:0 0 0 1px rgba(255,255,255,.06) inset;}
.match-summary .team .name{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}

.match-summary .meta{font-size:.9rem;color:var(--muted);display:flex;align-items:center;gap:.35rem;}
.match-summary .scoreline{display:flex;align-items:center;gap:.5rem;font-size:1.1rem;}
.match-summary .maps-score{font-weight:800;letter-spacing:.5px;min-width:2.6ch;text-align:center;}
.result-row{display:flex;gap:.5rem;justify-content:center;margin:2px 0;}

/* Summary-row chips ONLY (kept small) */
.match-summary .chip{
  padding:.12rem .5rem;
  border:1px solid transparent;
  border-radius:999px;
  font-size:.82rem;
  line-height:1.2;
  letter-spacing:.2px;
  white-space:nowrap;
}

.result-win{background:rgba(40,160,90,.16);border-color:rgba(40,160,90,.42);color:#64e09f;}
.result-loss{background:rgba(210,60,60,.14);border-color:rgba(210,60,60,.38);color:#ff8b8b;}
.result-draw{background:rgba(160,160,160,.14);border-color:rgba(160,160,160,.35);color:#dadada;}
.result-upcoming{background:rgba(130,130,130,.12);border-color:rgba(130,130,130,.32);color:#cfcfcf;}
.stage-chip{padding:.05rem .45rem;border:1px solid rgba(255,255,255,.12);border-radius:999px;font-size:.78rem;color:var(--muted);}

/* Details area */
.match-details{padding:.65rem .85rem;display:flex;flex-direction:column;gap:.35rem;}

/* Map-side chip stacks (yksi yhtenäinen määrittely) */
.matches-mirror .map-side{
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:flex-start;
  gap:.35rem;
  min-width:0;
}
/* Kaikki karttarivin chipit samalla koolla */
.matches-mirror .map-side .chip{
  padding:.44rem 1.0rem !important;
  font-size:1.06rem !important;
  line-height:1.16 !important;
  border:1px solid var(--border);
  border-radius:999px;
  background:var(--chip-bg);
  color:var(--fg);
}
/* Kierroschippi suuremmaksi + värit */
.matches-mirror .map-side .chip.round{
  padding:.56rem 1.05rem !important;
  font-size:1.18rem !important;
  font-weight:800;
  line-height:1.05;
  border:1px solid transparent;
  border-radius:999px;
}
.matches-mirror .map-side .chip.round.win  { background:rgba(34,197,94,.18);  border-color:rgba(34,197,94,.45);  color:#66e0a3; }
.matches-mirror .map-side .chip.round.loss { background:rgba(239,68,68,.18);  border-color:rgba(239,68,68,.45);  color:#ff8a8a; }
.matches-mirror .map-side .chip.round.draw { background:rgba(160,160,160,.14); border-color:rgba(160,160,160,.35); color:#dadada; }
/* Pick aina pinon alas */
.matches-mirror .map-side .chip.pick{
  margin-top:auto;
  color:var(--accent-2);
  border-color:rgba(125,211,252,.45);
  background:rgba(58,163,255,.12);
}

/* Map row: left stack | image | right stack */
.matches-mirror .map-row{
  display:grid;
  grid-template-columns:minmax(10rem,1fr) auto minmax(10rem,1fr);
  align-items:stretch; text-align:center; gap:.8rem; padding:.5rem 0;
}
/* Viiva karttarivien väliin (ei ensimmäisen yläpuolelle) */
.matches-mirror .match-details .map-row + .map-row{
  border-top:1px dashed var(--border);
  margin-top:.5rem;
  padding-top:.75rem;
}

.matches-mirror .map-name{text-align:center;font-weight:700;}
.matches-mirror .map-img{
  display:block;margin:.35rem auto 0;width:100%;max-width:420px;height:auto;
  border-radius:8px;border:1px solid var(--border);opacity:.95;
}

/* Totals centered */
.aggregate{border-top:1px dashed var(--border);padding-top:.45rem;margin-top:.25rem;}
.aggregate .totals{display:flex;flex-wrap:wrap;gap:.8rem;align-items:center;justify-content:center;}
.aggregate .label{color:var(--muted);}

/* Fixed inner width: summary + details same width */
.matches-mirror{--mirror-max:1080px;}
.matches-mirror .match-summary,
.matches-mirror .match-details{max-width:var(--mirror-max);margin:0 auto;width:100%;}

/* Mobile */
@media (max-width:720px){
  .matches-mirror .map-row{grid-template-columns:1fr;row-gap:.45rem;}
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
        const g   = parseInt(td.dataset.g || '0', 10);
        const w   = parseInt(td.dataset.w || '0', 10);
        const pctAttr = parseFloat((td.dataset.pct || '').replace(',','.'));
        const pct = isFinite(pctAttr) ? pctAttr : (g ? (100*w/g) : 0);
        if (!g) {
          if (td.dataset.zero === 'show') {
            td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
            const val = td.querySelector('.val');
            val.textContent = '0–0 (0%)';
            td.querySelector('.win').style.width  = '0%';
            td.querySelector('.loss').style.left  = '0%';
            td.querySelector('.loss').style.width = '100%';
            td.querySelector('.win').style.background  = '#555';
            td.querySelector('.loss').style.background = '#555';
            td.classList.add('cell-muted');
            td.title = 'No attempts';
          } else {
            td.textContent = 'not played';
            td.classList.add('cell-muted');
            td.title = 'No games';
          }
          return;
        }
        renderSplitWR(td, g, pct);
      });
    }
  }
  if(opts.color){ opts.color.forEach(c=>colorizeContinuous(tableId,c.col,c.p[0],c.p[1],c.p[2],c.inverse||false)); }
  if(opts.defaultSort){ sortTable(tableId,opts.defaultSort.col,opts.defaultSort.dir==='asc'); }
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
  const w = parseInt(td.dataset.w || '0', 10);
  const g = parseInt(td.dataset.g || '0', 10);
  const l = Math.max(0, g - w);
  const pctAttr = parseFloat((td.dataset.pct || '').replace(',','.'));
  const pct = isFinite(pctAttr) ? pctAttr : (g ? (100*w/g) : 0);
  td.innerHTML = '<div class="bar"><span></span><div class="val"></div></div>';
  const span = td.querySelector('.bar > span');
  const val  = td.querySelector('.bar .val');
  const wPct = Math.max(0, Math.min(100, pct));
  span.style.width = wPct + '%';
  val.textContent = `${w}–${l} (${Math.round(pct)}%)`;
  const gcol = Math.round(180 * (wPct/100));
  const rcol = Math.round(200 * (1 - wPct/100));
  span.style.background = `rgb(${rcol},${gcol},100)`;
  td.title = g ? `Wins: ${w}, Losses: ${l}, WR: ${pct.toFixed(1)}%` : 'No games';
}
function renderSplitWR(td, played, wrPct){
  const g   = Math.max(0, parseInt(played || 0, 10));
  const pct = Math.max(0, Math.min(100, parseFloat((wrPct || 0))));
  const wins   = Math.round(g * pct / 100);
  const losses = Math.max(0, g - wins);

  td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
  const win  = td.querySelector('.win');
  const loss = td.querySelector('.loss');
  const val  = td.querySelector('.val');

  win.style.width  = pct + '%';
  loss.style.left  = pct + '%';
  loss.style.width = (100 - pct) + '%';

  if (td.dataset.mode === 'ratio') {
    // Example: Flash Succ = successes / throws (pct)
    val.textContent = g ? `${wins}/${g} (${Math.round(pct)}%)` : '0/0 (0%)';
    td.title = g ? `Successes: ${wins}, Throws: ${g}, Rate: ${pct.toFixed(1)}%` : 'No attempts';
  } else {
    // Default: WR (wins–losses)
    val.textContent = g ? `${wins}–${losses} (${Math.round(pct)}%)` : '0–0 (0%)';
    td.title = g ? `Wins: ${wins}, Losses: ${losses}, WR: ${pct.toFixed(1)}%` : 'No games';
  }
}
document.addEventListener('DOMContentLoaded',()=>{ document.querySelectorAll('.tabs[id]').forEach(root=>initTabsAutoSort(root.id)); });
</script>
</head>
<body class="{page_class}">
"""


HTML_FOOT = """
</body>
</html>
"""

def page_start(title: str, page_class: str = "") -> str:
    return UNIFIED_HEAD.replace("{title}", title).replace("{page_class}", page_class)

def topbar(show_back_to_index: bool):
    back = '<a class="btn btn-ghost" href="index.html">← Takaisin indexiin</a>' if show_back_to_index else ""
    return f"""
    <div class="container">
      <div class="topbar">
        <div class="brand">
          <img src="https://armafinland.fi/css/gfx/armafin-logo-200px.png" alt="AFI logo" class="logo promo-logo"/>
          <span>AFI - Pappaliiga Stats v1</span>
        </div>
        <div class="nav">
          {back}
        </div>  
      </div>
    </div>
    """


def floating_back():
    # Kelluva paluunappi (näkyy myös indexissä, halutessa voi piilottaa CSS:llä)
    return '<a class="btn btn-primary btn-floating back-index" href="index.html" title="Palaa indexiin">← Index</a>'

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

def map_image_from_db(con: sqlite3.Connection, map_raw: str) -> tuple[str, str]:
    """
    Palauttaa (kuva_url, pretty_name). Fallbackina FACEITin staattinen kuva + raw-nimi.
    """
    art = get_map_art(con, map_raw)
    if art:
        url = art.get("image_lg") or art.get("image_sm") or ""
        pretty = art.get("pretty_name") or map_raw
        if url:
            return url, pretty
        return "", pretty
    slug = normalize_map_id(map_raw).replace("de_", "")
    return f"https://static.faceit.com/images/games/cs2/maps/{slug}.webp", map_raw

def map_pretty_name(con: sqlite3.Connection, raw: str) -> str:
    """
    Palauttaa kaunistetun nimen maps_catalogista tai hyvän fallbackin.
    """
    art = get_map_art(con, raw)
    if art and (art.get("pretty_name")):
        return art["pretty_name"]
    if not raw:
        return "—"
    slug = normalize_map_id(raw).replace("de_", "").replace("_", " ")
    return slug.title()


def has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def q(con, sql, params=()):
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    return rows

def _safe_div(a, b):
    return (a / b) if b else 0.0

from datetime import datetime

def _fmt_ts(ts: int) -> str:
    try:
        if not ts:
            return "—"
        # Show weekday too, e.g. 2025-08-31 Sun 21:27
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %a %H:%M")
    except Exception:
        return "—"

def _fmt_opt_stat(label: str, value: float | int, fmt: str) -> str:
    """Return 'Label X' only if value is non-zero; otherwise empty string."""
    try:
        v = float(value)
    except Exception:
        v = 0.0
    if abs(v) < 1e-9:
        return ""
    return f"{label} {fmt % v}"

def render_team_matches_mirror(con: sqlite3.Connection, division_id: int, team_id: str, team_name: str, teams: list[dict]) -> str:
    """
    Mirror-näkymä joukkueelle. KORJAUKSET:
      1) rf/ra haetaan jokaiselle karttariville erikseen (aiemmin käytettiin
         edellisen silmukan viimeisiä arvoja, mikä sekoitti "R xx" -chipit).
      2) Statit haetaan aina mapin sisäisistä left/right-dikteistä (back-compat
         opp_*-avaimiin säilytetty).
      3) Totals-rivin K/D lasketaan summasta (left.kills/deaths), ei vanhoista top-level-avaimista.
    """
    from db import get_team_matches_mirror
    from html import escape

    rows = get_team_matches_mirror(con, division_id, team_id)

    # avatar-helper
    def _avatar_of(tid: str | None) -> str | None:
        if not tid: return None
        x = next((t for t in teams if str(t.get("team_id")) == str(tid)), None)
        return x.get("avatar") if x else None

    left_avatar = _avatar_of(team_id)

    def _ts(row) -> int:
        return int(row.get("ts") or row.get("started_at") or 0)

    def _status(row) -> str:
        return str(row.get("status") or "—")

    def _faceit_url(row) -> str:
        if row.get("faceit_url"):
            return row["faceit_url"]
        fid = row.get("faceit_match_id")
        return f"https://www.faceit.com/cs2/room/{fid}" if fid else "#"

    def _team_name(tid: str | None, fallback: str = "—") -> str:
        if tid and str(tid) == str(team_id):
            return team_name or fallback
        x = next((t for t in teams if str(t.get("team_id")) == str(tid)), None)
        # huom! team_name eikä name
        return (x.get("team_name") if x else None) or fallback

    def _map_key(m: dict, *candidates: str, default=None):
        for k in candidates:
            if k in m and m[k] is not None:
                return m[k]
        return default

    def _fmt_kd(kills: int, deaths: int) -> float:
        if deaths > 0:
            return kills / deaths
        return float(kills) if kills > 0 else 0.0

    html: list[str] = []
    html.append(f'<div class="card matches-mirror" data-team-id="{team_id}">')
    html.append('  <div class="matches-head">')
    html.append('    <div class="title">Matches</div>')
    html.append(f'    <label class="toggle-played"><input type="checkbox" id="only-played-{team_id}"><span> Näytä vain pelatut</span></label>')
    html.append('  </div>')
    html.append(f'  <div class="matches-list" id="matches-{team_id}">')

    for r in rows:
        maps = r.get("maps") or []

        # määrittele vasen/oikea joukkue
        if "left" in r and "right" in r:
            left_tid  = r["left"].get("team_id")  or team_id
            right_tid = r["right"].get("team_id") or (r.get("team2_id") if r.get("team1_id")==team_id else r.get("team1_id"))
            left_name  = (r["left"].get("team_name")  or _team_name(left_tid))
            right_name = (r["right"].get("team_name") or _team_name(right_tid))
        else:
            t1, t2 = r.get("team1_id"), r.get("team2_id")
            left_tid  = str(team_id)
            right_tid = str(t2 if str(t1) == str(team_id) else t1)
            left_name  = _team_name(left_tid)
            right_name = _team_name(right_tid)

        right_avatar = _avatar_of(right_tid)

        # Totals turvallisesti rf/ra:sta
        mw = ml = rd = 0
        any_rounds = False
        for m in maps:
            rf = int(_map_key(m, "rf", default=0))
            ra = int(_map_key(m, "ra", default=0))
            if rf or ra:
                any_rounds = True
                rd += (rf - ra)
                mw += (1 if rf > ra else 0)
                ml += (1 if rf < ra else 0)

        played = bool(r.get("played", any_rounds))

        # header-badge
        if played:
            if mw > ml:   badge_cls, badge_txt = "result-win", "W"
            elif ml > mw: badge_cls, badge_txt = "result-loss", "L"
            else:         badge_cls, badge_txt = "result-draw", "D"
        else:
            badge_cls, badge_txt = "result-upcoming", "upcoming"

        date_s = _fmt_ts(_ts(r))
        stage  = _status(r).capitalize()
        maps_score = f"{mw}–{ml}" if played else "—"
        faceit_url = _faceit_url(r)

        # --- SUMMARY ---
        html.append(f'  <details class="match-row" data-played={"1" if played else "0"}>')
        html.append('      <summary class="match-summary" role="button">')
        l_logo = f'<img class="logo" src="{left_avatar}" alt="">' if left_avatar else ''
        html.append(f'        <div class="team side-left">{l_logo}<div class="name">{escape(left_name)}</div></div>')
        html.append('        <div class="center">')
        html.append(f'          <div class="meta"><span class="date">{date_s}</span></div>')
        html.append(f'          <div class="result-row"><span class="stage-chip">{escape(stage)}</span></div>')
        if badge_txt != "upcoming":
          html.append(f'          <div class="result-row"><span class="chip {badge_cls}">{badge_txt}</span></div>')
        html.append(f'          <div class="scoreline"><span class="maps-score">{maps_score}</span></div>')
        html.append(f'          <a class="faceit-link" href="{faceit_url}" target="_blank" rel="noopener">Open on FACEIT</a>')
        html.append('        </div>')
        r_logo = f'<img class="logo" src="{right_avatar}" alt="">' if right_avatar else ''
        html.append(f'        <div class="team side-right"><div class="name">{escape(right_name)}</div>{r_logo}</div>')
        html.append('      </summary>')

        # --- DETAILS: kartat ---
        html.append('      <div class="match-details">')
        for m in maps:
            # rf/ra tälle kartalle (tämä on varsinainen fix)
            rf = int(_map_key(m, "rf", default=0))
            ra = int(_map_key(m, "ra", default=0))

            # lue arvot ensisijaisesti left/right-dikteistä, muuten back-compat
            def _m_side_val(side: str | None, key: str, *alts: str, default=0):
                if side and isinstance(m.get(side), dict):
                    v = m[side].get(key, None)
                    if v is None:
                        for a in alts:
                            if a in m[side] and m[side][a] is not None:
                                v = m[side][a]; break
                    if v is not None:
                        return v
                return _map_key(m, key, *alts, default=default)

            # oma puoli
            kills  = int(_m_side_val("left",  "kills",  default=0))
            deaths = int(_m_side_val("left",  "deaths", default=0))
            adr    = float(_m_side_val("left",  "adr",   default=0.0))
            dmg    = int(_m_side_val("left",  "damage", "dmg", default=0))
            kd_val = _fmt_kd(kills, deaths)

            # vastustaja
            okills  = int(_m_side_val("right", "kills",  "opp_kills",  default=0))
            odeaths = int(_m_side_val("right", "deaths", "opp_deaths", default=0))
            oadr    = float(_m_side_val("right", "adr",  "opp_adr",    default=0.0))
            odmg    = int(_m_side_val("right", "damage","dmg","opp_damage","opp_dmg", default=0))
            okd_val = _fmt_kd(okills, odeaths)

            # Pick-merkintä
            pick_tid = str(_map_key(m, "pick_team_id", "selected_by_team_id", "picked_by_team_id", default="0"))
            picked_left  = (pick_tid == str(left_tid))
            picked_right = (pick_tid == str(right_tid))

            # karttanimi + kuva
            map_raw = str(_map_key(m, "map", "map_name", default="—"))
            img_url, pretty = map_image_from_db(con, map_raw)

            # chipit: R, ADR, K/D, DMG, Pick
            left_chips = [
                f'<span class="chip round {"win" if rf > ra else ("loss" if rf < ra else "draw")}">R {rf}</span>',
                f'<span class="chip stat"><span class="stat-label">ADR</span> {adr:.1f}</span>',
                f'<span class="chip stat"><span class="stat-label">K/D</span> {kd_val:.2f}</span>',
                f'<span class="chip stat"><span class="stat-label">DMG</span> {dmg}</span>',
            ]
            if picked_left:
                left_chips.append('<span class="chip stat pick">Pick</span>')

            right_chips = [
                f'<span class="chip round {"win" if ra > rf else ("loss" if ra < rf else "draw")}">R {ra}</span>',
                f'<span class="chip stat"><span class="stat-label">ADR</span> {oadr:.1f}</span>',
                f'<span class="chip stat"><span class="stat-label">K/D</span> {okd_val:.2f}</span>',
                f'<span class="chip stat"><span class="stat-label">DMG</span> {odmg}</span>',
            ]
            if picked_right:
                right_chips.append('<span class="chip stat pick">Pick</span>')

            # rivi
            html.append('        <div class="map-row">')
            html.append('          <div class="map-side side-left">'  + " ".join(left_chips)  + '</div>')
            html.append(f'          <div class="map-name">{escape(pretty)}'
                        f'            <img class="map-img" src="{img_url}" alt="{escape(pretty)}" onerror="this.style.display=\'none\'">'
                        f'          </div>')
            html.append('          <div class="map-side side-right">' + " ".join(right_chips) + '</div>')
            html.append('        </div>')

        # Totals (K/D summista vasemmalta)
        if maps:
            tot_kills = sum(int((m.get("left") or {}).get("kills")  or 0) for m in maps)
            tot_death = sum(int((m.get("left") or {}).get("deaths") or 0) for m in maps)
            tot_kd = _fmt_kd(tot_kills, tot_death)
            bits = [f'Maps {mw+ml} ({mw}-{ml})', f'RD {rd:+d}']
            if tot_kd > 0:
                bits.append(f'K/D {tot_kd:.2f}')
            html.append('        <div class="aggregate">')
            html.append('          <div class="totals"><span class="label">Totals:</span> ' + " ".join(f"<span>{escape(x)}</span>" for x in bits) + '</div>')
            html.append('        </div>')

        html.append('      </div>')
        html.append('  </details>')

    html.append('  </div>')
    html.append(f"""
    <script>
    (function(){{
      var root=document.getElementById('matches-{team_id}');
      if(!root) return;
      var box=root.parentElement.querySelector('#only-played-{team_id}');
      function apply(){{
        var only=box && box.checked;
        root.querySelectorAll('.match-row').forEach(function(row){{
          var played=row.getAttribute('data-played')==='1';
          row.style.display=(only && !played)?'none':'';
        }});
      }}
      if(box) box.addEventListener('change',apply);
      apply();
    }})();
    </script>
    """)
    html.append('</div>')
    return "\n".join(html)

def compute_champ_player_summary(con, division_id: int, min_rounds: int = 40, min_flashes: int = 10):
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
      WHERE m.championship_id = ?
      GROUP BY ps.player_id
    """, (division_id,))

    # Teams
    teams = q(con, """
      SELECT COUNT(*) AS c FROM (
        SELECT DISTINCT team1_id AS tid FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
        UNION
        SELECT DISTINCT team2_id AS tid FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
      )
    """, (division_id, division_id))[0]["c"] or 0

    # Maps count
    maps_cnt = q(con, """
      SELECT COUNT(*) AS c
      FROM maps mp JOIN matches m ON m.match_id=mp.match_id
      WHERE m.championship_id=?
    """, (division_id,))[0]["c"] or 0

    # Total rounds
    total_rounds = q(con, """
      SELECT SUM(mp.score_team1 + mp.score_team2) AS r
      FROM maps mp JOIN matches m ON m.match_id=mp.match_id
      WHERE m.championship_id=?
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

def _index_card_stats(con: sqlite3.Connection, championship_id: str) -> tuple[int, int, int]:
    """
    Palauttaa (teams, played, total) index-kortille.
    - teams: uniikkien joukkueiden määrä (team1_id ∪ team2_id)
    - played: pelatut matsit (finished_at IS NOT NULL TAI status='finished')
    - total: kaikki matsit kannassa
    """
    r = q(con, """
      SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN finished_at IS NOT NULL OR LOWER(COALESCE(status,''))='finished'
                 THEN 1 ELSE 0 END) AS played
      FROM matches
      WHERE championship_id=?
    """, (championship_id,))
    total = int((r[0]["total"] or 0)) if r else 0
    played = int((r[0]["played"] or 0)) if r else 0

    teams = q(con, """
      SELECT COUNT(*) AS c FROM (
        SELECT team1_id AS tid FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
        UNION
        SELECT team2_id AS tid FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
      )
    """, (championship_id, championship_id))
    team_cnt = int((teams[0]["c"] or 0)) if teams else 0

    return (team_cnt, played, total)

def render_index(con: sqlite3.Connection, divisions: list[dict]) -> str:
    # Group divisions by season
    by_season: dict[int, list[dict]] = {}
    for div in divisions:
        s = int(div.get("season") or 0)
        by_season.setdefault(s, []).append(div)

    html = []
    html.append(page_start("Pappaliiga — Index", "is-index"))
    html.append(topbar(show_back_to_index=False))

    # Hero + container start (kept as in your original)
    html.append("""
      <div class="container">
        <section class="hero">
          <div class="hero-card">
            <h1>Armafinland</h1>
            <p>
              Arma Finland on suomenkielisille pelaajille ja peliporukoille tarkoitettu avoin peliyhteisö. Yhteisö tarjoaa Arman pelaamista taktisessa ympäristössä mahdollisimman monen suomalaisen pelaajan kanssa. Pelitapahtumissa keskitytään realismiin, toimintaan joukkueissa ja yhteistyöhön. Tapahtumissa on usein erilaisia ajoneuvoja ja aseita. Teemat vaihtelevat toisen maailmansodan, kylmän sodan ja nykyajan konfliktien välillä.
            </p>
            <div class="hero-cta">
              <a class="btn btn-primary" href="https://armafinland.fi/discord" title="Liity Armafinland Discordiin">Liity AFI Discord</a>
              <a class="btn" href="https://armafinland.fi/" title="Lue lisää yhteisöstä">Lue lisää</a>
            </div>
          </div>

          <div class="hero-card">
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
    """) 

    # Render sections per season (new)
    for season in sorted(by_season.keys(), reverse=True):
        # lajittelu numerojärjestykseen division_id:n mukaan
        divs = sorted(by_season[season], key=lambda d: int(d.get("division_num") or 0))
        html.append(f'<h2 style="margin:0 0 10px 0;">Season {season}</h2>')
        html.append('<div class="grid">')
        for div in divs:
            title = esc_title(div.get("name", "Division"))
            slug = (div.get("slug") or "").strip()
            season_num = int(div.get("season") or 0)
            href = f"{slug}.html" if slug else "index.html"

            teams, played, total = _index_card_stats(con, div["championship_id"])

            html.append(f"""
              <a class="card" href="{href}" title="{title}">
                <div>
                  <h3>{title}</h3>
                  <small>{teams} joukkuetta<br>{played}/{total} ottelua pelattu</small>
                </div>
              </a>
            """)
        html.append("</div>")  # /grid

    html.append("""
      <div class="footer">
        By Tuntematon from Armafinland
      </div>
    </div>
    """)  # /container

    # Floating back button
    html.append(floating_back())
    html.append(page_end())
    return "\n".join(html)




# ------------------------------
# Rendering
# ------------------------------
def render_division(con, div):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    teams = get_teams_in_championship(con, div["championship_id"])
    div_avgs = compute_champ_map_avgs_data(con, div["championship_id"])
    thresholds = compute_champ_thresholds_data(con, div["championship_id"])

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = []
    title = f"{esc_title(div['name'])} (Season {div['season']}) — Pappaliiga Stats"
    html.append(page_start(title, "is-division"))
    html.append(topbar(show_back_to_index=True))

    # --- sivun sisältö alkaa ---
    html.append('<div class="container">')
    html.append(f"<h1 style='text-align:center'>{div['name']} (Season {div['season']})</h1>")
    html.append(
        f"<div class='muted' style='text-align:center; margin-top:-6px; font-size:0.9em;'>"
        f"Generoitu {ts}"
        f"</div>"
    )
    html.append('<div class="page">')
    html.append('<div class="page">')


    html.append('<div class="nav">')
    for t in teams:
        name = t["team_name"] or t["team_id"]
        avatar = t.get("avatar")
        logo = f'<img class="logo nav-logo" src="{avatar}" alt="">' if avatar else ''
        html.append(f'<a href="#team-{t["team_id"]}">{logo}{escape(name)}</a>')
    html.append("</div>")

    # --- Divisioonan lyhyt yhteenveto pelaajista ---
    divsum = compute_champ_player_summary(con, div["championship_id"], min_rounds=20)
    mp_sum = compute_champ_map_summary_data(con, div["championship_id"])

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
    lines = "<br>".join([f"{map_pretty_name(con, n)} <span class='cell-muted'>({c}×)</span>" for n, c in mp_sum["top_played"]])
    html.append(
        f"<div class='summary-item'>"
        f"  <div class='label'>Most played (top4)</div>"
        f"  <div class='val' style='line-height:1.25'>{lines}</div>"
        f"</div>"
    )

    # Top 3 bannatuimmat kartat (ruutuna)
    lines = "<br>".join([f"{map_pretty_name(con, n)} <span class='cell-muted'>({c}×)</span>" for n, c in mp_sum["top_banned"]])
    html.append(
        f"<div class='summary-item'>"
        f"  <div class='label'>Most banned (top4)</div>"
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

    html.append('<div class="muted">Joillakin arvoilla on tooltip missä lisää tietoa.</div>')

    for ti, t in enumerate(teams, start=1):
        team_id = t["team_id"]; team_name = t["team_name"] or t["team_id"]
        html.append(f'<details class="team-section" id="team-{team_id}" open>')
        # hae avatar muistista (teams-listasta)
        team_avatar = next((t.get("avatar") for t in teams if t["team_id"] == team_id), None)
        logo = f'<img class="logo team-logo" src="{team_avatar}" alt="">' if team_avatar else ''
        html.append(f"<summary><h2>{logo}{escape(team_name)}</h2></summary>")


        # --- Lataa pelaajadata ensin, jotta voidaan laskea varaluotettavat tiimikompaktit ---
        players = compute_player_table_data(con, div["championship_id"], team_id)

        # Johdetut mittarit + optiosarakkeiden tunnisteet (pidä entiset)
        has_flash  = any(("flashed" in p and "flash_count" in p) for p in players)
        has_pistol = any(("pistol_kills" in p) for p in players)

        # Laske fallback-arvot pelaajista (jos tiivistelmä ei palauta niitä)
        tot_k   = sum(p.get("kill",   0) for p in players)
        tot_d   = sum(p.get("death",  0) for p in players)
        tot_r   = sum(p.get("rounds", 0) for p in players)
        tot_util = sum(p.get("util",  0) for p in players)

        # ADR kierros-painotettuna
        adr_weighted_sum = sum((p.get("adr", 0.0) * p.get("rounds", 0)) for p in players)
        fallback_stats = {
            "kd":  (tot_k / tot_d) if tot_d else float(tot_k),
            "kr":  (tot_k / tot_r) if tot_r else 0.0,
            "adr": (adr_weighted_sum / tot_r) if tot_r else 0.0,
            "util": float(tot_util),
        }

        # Alkuperäinen tiivistelmä (W-L, RD, ym. tulevat täältä edelleen)
        s = compute_team_summary_data(con, div["championship_id"], team_id)

        # Paikkaa puuttuvat/nollatiedot pelaajista lasketuilla arvoilla
        for k in ("kd", "kr", "adr", "util"):
            if (k not in s) or (s[k] in (None, 0) and tot_r > 0):
                s[k] = fallback_stats[k]

        # Sirut
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

            # --- : Survival% ja Rating1 (HLTV 1.0 -approksimaatio) ---
            death = p.get("death", 0) or 0
            adr   = p.get("adr", 0.0) or 0.0
            surv_ratio = 1.0 - ((death / rounds) if rounds else 0.0)
            # clamp 0..1 varmuuden vuoksi
            surv_ratio = max(0.0, min(1.0, surv_ratio))
            p["survival_pct"] = surv_ratio * 100.0
            # Sama kaava, jota käytät sivun tooltipissa
            p["rating1"] = ((kr / 0.679) + (surv_ratio / 0.317) + (adr / 79.9)) / 3.0 if rounds else 0.0
            

            # Enemies per flash (jos dataa on)
            fc = p.get("flash_count", 0) or 0
            if has_flash:
                p["enemies_per_flash"] = (p.get("flashed", 0) or 0) / fc if fc else 0.0
            else:
                p["enemies_per_flash"] = None

            # Flash success for display
            fsu = p.get("flash_successes", p.get("flash_succ", 0)) or 0
            p["flash_succ_pct"]        = (100.0 * fsu / fc) if fc else 0.0

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
          <th onclick="sortTable('{tid_basic}',6,true)">Damage</th>
          <th onclick="sortTable('{tid_basic}',7,true)">Kills</th>
          <th onclick="sortTable('{tid_basic}',8,true)">Deaths</th>
          <th onclick="sortTable('{tid_basic}',9,true)">Assists</th>
          <th onclick="sortTable('{tid_basic}',10,true)">HS%</th>
          <th onclick="sortTable('{tid_basic}',11,true)">2K</th>
          <th onclick="sortTable('{tid_basic}',12,true)">3K</th>
          <th onclick="sortTable('{tid_basic}',13,true)">4K</th>
          <th onclick="sortTable('{tid_basic}',14,true)">ACE</th>
          <th onclick="sortTable('{tid_basic}',15,true)">MVPs</th>
          </tr></thead>""")
        for p in players:
          html.append(f"""<tr>
            <td>{p["nickname"]}</td>
            <td>{p["maps_played"]}</td>
            <td title="Rounds/Map: {p['rpm']:.1f}">{p["rounds"]}</td>
            <td>{p["kd"]:.2f}</td>
            <td>{p["adr"]:.1f}</td>
            <td>{p["kr"]:.2f}</td>
            <td>{p["damage"]}</td>
            <td>{p["kill"]}</td>
            <td>{p["death"]}</td>
            <td>{p["assist"]}</td>
            <td>{p["hs_pct"]:.1f}</td>
            <td>{p["k2"]}</td>
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
            {{col:10, p:[{thresholds['hs_pct'][0]:.4f}, {thresholds['hs_pct'][1]:.4f}, {thresholds['hs_pct'][2]:.4f}]  }}
          ],
          defaultSort: {{col:0, dir:'asc'}},
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


        # Flash ratio bar (succ / throws), then totals and efficiency
        col_flash_ratio = col_idx
        html.append(
            f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" "
            "title='Successful flashes out of all thrown (successes / throws). Cell shows S/T and % as a bar.'>"
            "Flash Succ (S/T)</th>"
        ); col_idx += 1

        html.append(
            f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" "
            "title='Number of enemies blinded by the player''s flashes'>Flashed</th>"
        ); col_idx += 1

        html.append(
            f"<th onclick=\"sortTable('{tid_adv}',{col_idx},true)\" "
            "title='Enemies blinded per flash thrown'>Enem/Flash</th>"
        ); col_idx += 1

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

            # Flash Succ ratio bar (succ/throws) – WR-tyylinen palkki
            _s = int(p.get("flash_successes", p.get("flash_succ", 0)) or 0)
            _c = int(p.get("flash_count", 0) or 0)
            _pct = (100.0 * _s / _c) if _c else 0.0
            html.append(
                f"<td class='wr' data-mode='ratio' data-zero='show' "
                f"data-g='{_c}' data-w='{_s}' data-pct='{_pct:.1f}' "
                f"title='Successes: {_s}, Throws: {_c}'>"
                f"</td>"
            )

            # Total enemies blinded
            html.append(f"<td>{p.get('flashed', 0)}</td>")

            # Efficiency: enemies blinded per flash
            val = p['enemies_per_flash'] if p['enemies_per_flash'] is not None else 0.0
            html.append(f"<td>{val:.2f}</td>")

            html.append(f"<td>{p.get('pistol_kills',0)}</td>")
            html.append(f"<td>{p.get('awp_kills',0)}</td>")
            html.append("</tr>")
            
        html.append("</tbody></table>")

        html.append(f"""
        <script>
        postProcessTable('{tid_adv}', {{
          wrbars: [2, 3, 4, 9],
          color: [
            {{col:6,  p:[{thresholds['udpr'][0]:.4f}, {thresholds['udpr'][1]:.4f}, {thresholds['udpr'][2]:.4f}]}},
            {{col:7,  p:[{thresholds['survival'][0]:.4f}, {thresholds['survival'][1]:.4f}, {thresholds['survival'][2]:.4f}]}},
            {{col:8,  p:[{thresholds['rating1'][0]:.4f},  {thresholds['rating1'][1]:.4f},  {thresholds['rating1'][2]:.4f}]}},
            {{col:11, p:[{thresholds['enemies_per_flash'][0]:.4f}, {thresholds['enemies_per_flash'][1]:.4f}, {thresholds['enemies_per_flash'][2]:.4f}]}}
          ],
          defaultSort: {{col:0, dir:'asc'}},
        }});
        </script>
        """)

        #html.append(f"<script>applyDefaultSort('{tid_adv}');</script>")
        html.append("</div>")  # /tab-panel advanced

        # Map stats
        maps = compute_map_stats_table_data(con, div["championship_id"], team_id)

        # Chipit
        best_wr = max((r for r in maps if r["played"]>0), key=lambda r: r["wr"], default=None)
        most_pick = max(maps, key=lambda r: r["picks"], default=None)
        most_ban  = max(maps, key=lambda r: r["total_own_ban"], default=None)
        played_rows = [r for r in maps if r["played"]>=2]
        avoid = min(played_rows, key=lambda r: r["wr"], default=None)

        html.append('<div class="chips">')
        if most_ban and most_ban["total_own_ban"]>0:
            html.append(f'<span class="chip">Most banned: {map_pretty_name(con, most_ban["map"])} ({most_ban["total_own_ban"]}×)</span>')
        if most_pick and most_pick["picks"]>0:
            html.append(f'<span class="chip">Most picked: {map_pretty_name(con, most_pick["map"])} ({most_pick["picks"]}×)</span>')
        if best_wr and best_wr["wr"]>0:
            html.append(f'<span class="chip">Best WR: {map_pretty_name(con, best_wr["map"])} ({best_wr["wr"]:.0f}%)</span>')
        if avoid:
            html.append(f'<span class="chip">Map to avoid: {map_pretty_name(con, avoid["map"])} ({avoid["wr"]:.0f}%)</span>')
        html.append('</div>')

        # Toolbar (filter + CSV + column toggles)
        tid2 = f"maps-{ti}"
        html.append(f"""
        <div class="toolbar">
          <label><input type="checkbox" id="{tid2}-played-only"> Show played only</label>
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
            <td>{map_pretty_name(con, r["map"])}</td>
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
        }});
        bindPlayedOnly('{tid2}', '{tid2}-played-only');
        </script>
        """)
        html.append(render_team_matches_mirror(con, div["championship_id"], team_id, team_name, teams))
        html.append("</details>")  # team section

    html.append('</div>')      # .page
    html.append('</div>')      # .container
    html.append(floating_back())
    html.append(page_end())

    out_path = OUT_DIR / f"{div['slug']}.html"
    html_str = "\n".join(html)
    did_write = write_if_changed(out_path, html_str)
    print(f"[{'write' if did_write else 'skip '}]", out_path)
    if did_write:
        print(f"[OK] Wrote {out_path}")
    return out_path

def write_index(con: sqlite3.Connection):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    html = render_index(con, DIVISIONS)
    idx_path = OUT_DIR / "index.html"
    did_write = write_if_changed(idx_path, html)
    print(f"[{'write' if did_write else 'skip '}]", idx_path)
    if did_write:
        print(f"[OK] Wrote {idx_path}")


# --- Content-aware write helpers -------------------------------------------

# Nappaa sekä suomen- että englanninkielisiä aikaleimatekstejä (varmuuden vuoksi).
_TS_PATTERNS = [
    r"Generoitu\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?",   # "Generoitu 2025-09-06 15:27" (tai sekunneilla)
    r"\(Generoitu\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?\)", # "(Generoitu ...)"
    r"Generated\s+\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?",   # jos joskus käytössä
]

# Mahdollisia build/nonssi-merkintöjä, joita ei haluta vaikuttamaan vertailuun:
# esim. <link href="app.css?b=abcdef1"> tai data-build="abcdef1"
_BUILD_PATTERNS = [
    r"\?b=[a-f0-9]{7,}",                   # query-param build hash
    r"data-build=[\"'][a-f0-9]{7,}[\"']",  # data-build attribuutti
]

# Yleinen ISO-ajan poistaja varmistukseksi (jos viet jonkin ajan meta- tai kommenttikenttään)
_ISO_TS_ANYWHERE = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?"

def _to_unix_newlines(s: str) -> str:
    # Normalisoi rivinvaihdot: CRLF/LF -> LF (tämä oli syypää jatkuviin kirjoituksiin Windowsissa)
    return s.replace("\r\n", "\n").replace("\r", "\n")

def _normalize_for_compare_bytes(b: bytes) -> bytes:
    # Dekoodaa, normalisoi rivinvaihdot ja poista dynaamiset osat vertailusta
    s = b.decode("utf-8", errors="ignore")
    s = _to_unix_newlines(s)

    # Poista aikaleimatekstit
    for pat in _TS_PATTERNS:
        s = re.sub(pat, "GENERATED_TS", s, flags=re.IGNORECASE)

    # Poista yksittäiset ISO-ajat varmuuden vuoksi (jos esiintyvät esim. kommenteissa)
    s = re.sub(_ISO_TS_ANYWHERE, "GENERATED_TS", s)

    # Poista build/nonssi-merkkaukset
    for pat in _BUILD_PATTERNS:
        s = re.sub(pat, "", s, flags=re.IGNORECASE)

    # (Valinnainen) Siivoa trailing whitespace rivuilta, jotta editori-muutokset eivät vaikuta
    s = "\n".join(line.rstrip() for line in s.split("\n"))

    return s.encode("utf-8", errors="ignore")

def write_if_changed(path: "Path", content: str) -> bool:
    """
    Kirjoita 'path' vain jos normalisoitu sisältö poikkeaa vanhasta.
    Palauttaa True jos kirjoitettiin, False jos ohitettiin.
    """
    new_bytes = _normalize_for_compare_bytes(_to_unix_newlines(content).encode("utf-8"))

    try:
        old_raw = path.read_bytes()
        old_bytes = _normalize_for_compare_bytes(old_raw)
        if hashlib.sha256(old_bytes).digest() == hashlib.sha256(new_bytes).digest():
            return False  # Ei muutosta
    except FileNotFoundError:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write Windows-yhteensopivasti: kirjoita temp-tiedostoon ja vaihda paikalleen
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=str(path.parent)) as tf:
        tf.write(content.encode("utf-8"))
        tmp_name = tf.name
    os.replace(tmp_name, path)
    return True

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = get_conn(DB_PATH)
    con.row_factory = sqlite3.Row

    for div in DIVISIONS:
        path = render_division(con, div)

    write_index(con)

if __name__ == "__main__":
    main()
