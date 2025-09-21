# db.py
# SQLite helpers for Pappaliiga CS (championship-centric).
# - Safe UPSERTs for championships, matches, maps, votes, team/player per-map stats
# - Back-compat insert_* wrappers that call the new upsert_* functions
# - Query functions for fetching stats (used by html_gen.py)
# All comments in English by design.

from __future__ import annotations
import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
DEFAULT_TEAM_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"

# -------------------------
# Connection & init
# -------------------------

def get_conn(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")

    # Performance pragmas (safe defaults for this workload)
    try:
        con.execute("PRAGMA journal_mode=WAL;")      # persistent in DB file
    except Exception:
        pass
    try:
        con.execute("PRAGMA synchronous=NORMAL;")    # per-connection
    except Exception:
        pass
    try:
        con.execute("PRAGMA temp_store=MEMORY;")     # per-connection
    except Exception:
        pass
    try:
        con.execute("PRAGMA mmap_size=1073741824;")  # 1 GiB
    except Exception:
        pass

    return con


def init_db(con: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    con.executescript(sql)
    con.executescript("""
    CREATE INDEX IF NOT EXISTS idx_matches_champ      ON matches(championship_id);
    CREATE INDEX IF NOT EXISTS idx_matches_status     ON matches(status);
    CREATE INDEX IF NOT EXISTS idx_matches_ts         ON matches(championship_id, finished_at, started_at, scheduled_at, configured_at, last_seen_at);

    CREATE INDEX IF NOT EXISTS idx_maps_match         ON maps(match_id);
    CREATE INDEX IF NOT EXISTS idx_maps_match_round   ON maps(match_id, round_index);

    CREATE INDEX IF NOT EXISTS idx_map_votes_match    ON map_votes(match_id);

    CREATE INDEX IF NOT EXISTS idx_ps_match_round     ON player_stats(match_id, round_index);
    CREATE INDEX IF NOT EXISTS idx_ps_team_match      ON player_stats(team_id, match_id);
    """)
    con.commit()

# -------------------------
# Championships
# -------------------------

def upsert_championship(con: sqlite3.Connection, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge by (season, division_num, is_playoffs) OR by championship_id.
    Returns the canonical row dict with the championship_id you must use downstream.
    """
    cur = con.execute(
        """
        SELECT championship_id, season, division_num, name, is_playoffs, slug
        FROM championships
        WHERE season=? AND division_num=? AND is_playoffs=?
        LIMIT 1
        """,
        (row["season"], row["division_num"], row["is_playoffs"])
    ).fetchone()

    if cur:
        existing_id = cur[0]
        con.execute(
            """
            UPDATE championships SET
              name = CASE WHEN name IS NULL OR name='' THEN :name ELSE name END,
              slug = CASE WHEN slug IS NULL OR slug='' THEN :slug ELSE slug END
            WHERE season=:season AND division_num=:division_num AND is_playoffs=:is_playoffs
            """,
            row
        )
        con.commit()
        out = dict(row)
        out["championship_id"] = existing_id
        return out

    sql = """
    INSERT INTO championships (championship_id, season, division_num, name, is_playoffs, slug)
    VALUES (:championship_id, :season, :division_num, :name, :is_playoffs, :slug)
    ON CONFLICT(championship_id) DO UPDATE SET
      season       = COALESCE(championships.season, excluded.season),
      division_num = COALESCE(championships.division_num, excluded.division_num),
      name         = CASE WHEN championships.name IS NULL OR championships.name='' THEN excluded.name ELSE championships.name END,
      is_playoffs  = COALESCE(championships.is_playoffs, excluded.is_playoffs),
      slug         = CASE WHEN championships.slug IS NULL OR championships.slug='' THEN excluded.slug ELSE championships.slug END
    """
    con.execute(sql, row)
    con.commit()
    return dict(row)

# -------------------------
# Teams & Players
# -------------------------

def upsert_team(con: sqlite3.Connection, team: Dict[str, Any]) -> None:
    """
    team = { team_id, name, avatar, updated_at? }
    Takuut:
      - tallennetaan aina jokin avatar (oletus jos puuttuu)
      - ei ylikirjoiteta olemassa olevaa ei-tyhjää arvoa
    """
    if "updated_at" not in team:
        team["updated_at"] = None
    avatar_in = team.get("avatar")
    team["avatar"] = avatar_in if (avatar_in is not None and str(avatar_in).strip() != "") else DEFAULT_TEAM_AVATAR

    sql = """
    INSERT INTO teams (team_id, name, avatar, updated_at)
    VALUES (:team_id, :name, :avatar, COALESCE(:updated_at, strftime('%s','now')))
    ON CONFLICT(team_id) DO UPDATE SET
      name       = CASE WHEN teams.name IS NULL OR teams.name='' THEN excluded.name ELSE teams.name END,
      avatar     = COALESCE(NULLIF(teams.avatar, ''), NULLIF(excluded.avatar, ''), :default_avatar),
      updated_at = COALESCE(excluded.updated_at, teams.updated_at)
    """
    con.execute(sql, {**team, "default_avatar": DEFAULT_TEAM_AVATAR})

def upsert_player(con: sqlite3.Connection, player: Dict[str, Any]) -> None:
    """
    player = { player_id, nickname, updated_at? }
    """
    if "updated_at" not in player:
        player["updated_at"] = None
    sql = """
    INSERT INTO players (player_id, nickname, updated_at)
    VALUES (:player_id, :nickname, COALESCE(:updated_at, strftime('%s','now')))
    ON CONFLICT(player_id) DO UPDATE SET
      nickname   = CASE WHEN players.nickname IS NULL OR players.nickname='' THEN excluded.nickname ELSE players.nickname END,
      updated_at = COALESCE(excluded.updated_at, players.updated_at)
    """
    con.execute(sql, player)

# -------------------------
# Query functions for stats (used by html_gen.py)
# -------------------------

def query(con: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    return rows

_COLS_CACHE: dict[tuple[int, str], set[str]] = {}

def has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    key = (id(con), table)
    cols = _COLS_CACHE.get(key)
    if cols is None:
        cur = con.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cur.fetchall()}
        _COLS_CACHE[key] = cols
    return col in cols

def get_teams_in_championship(con: sqlite3.Connection, division_id: int) -> list[dict]:
    sql = """
    WITH team_ids AS (
      SELECT DISTINCT team1_id AS team_id FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
      UNION
      SELECT DISTINCT team2_id AS team_id FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
    )
    SELECT t.team_id,
           COALESCE(t.name, '') AS team_name,
           t.avatar
    FROM team_ids x
    LEFT JOIN teams t ON t.team_id = x.team_id
    ORDER BY team_name COLLATE NOCASE
    """
    rows = query(con, sql, (division_id, division_id))
    return [r for r in rows if r["team_id"]]

def compute_team_summary_data(con: sqlite3.Connection, division_id: int, team_id: str) -> dict:
    # Haetaan VAIN pelatut kartat (join maps) → näistä johdetaan kaikki
    rows = query(con, """
        SELECT m.match_id, m.team1_id, m.team2_id,
               p.round_index, p.map_name, p.score_team1, p.score_team2, p.winner_team_id
        FROM matches m
        JOIN maps p ON p.match_id = m.match_id
        WHERE m.championship_id=? AND (m.team1_id=? OR m.team2_id=?)
        AND p.map_name <> 'forfeit'
    """, (division_id, team_id, team_id))

    # Pelatut ottelut = distinct match_id karttariveistä
    matches_played = len({r["match_id"] for r in rows})
    maps_played = len(rows)
    maps_w = sum(1 for r in rows if r.get("winner_team_id") == team_id)

    # Round-difference joukkueen näkökulmasta
    rd = 0
    for r in rows:
        s1 = r.get("score_team1") or 0
        s2 = r.get("score_team2") or 0
        if r["team1_id"] == team_id:
            rd += (s1 - s2)
        elif r["team2_id"] == team_id:
            rd += (s2 - s1)

    # Aggregaatit suoraan player_statsista (ei team_stats-taulua)
    agg = query(con, """
        SELECT
          SUM(ps.kills)           AS kills,
          SUM(ps.deaths)          AS deaths,
          AVG(COALESCE(ps.kr,0))  AS kr,
          AVG(COALESCE(ps.adr,0)) AS adr,
          SUM(COALESCE(ps.utility_damage,0)) AS util
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        WHERE ps.team_id=? AND m.championship_id=?
    """, (team_id, division_id))[0]

    kills = agg["kills"] or 0
    deaths = agg["deaths"] or 0
    kd = (kills / deaths) if deaths else float(kills)

    return {
        "matches_played": matches_played,
        "maps_played": maps_played,
        "w": maps_w,
        "l": maps_played - maps_w,
        "rd": rd,
        "kd": kd,
        "kr": agg["kr"] or 0.0,
        "adr": agg["adr"] or 0.0,
        "util": agg["util"] or 0,
    }


def compute_player_table_data(con: sqlite3.Connection, division_id: int, team_id: str) -> list[dict[str, Any]]:
    HAS_PISTOL = has_column(con, "player_stats", "pistol_kills")
    HAS_FLASH  = (has_column(con, "player_stats", "enemies_flashed")
                  and has_column(con, "player_stats", "flash_count"))
    HAS_FLASH_SUCC = has_column(con, "player_stats", "flash_successes")
    HAS_MVPS  = has_column(con, "player_stats", "mvps")

    select_cols = [
        "ps.player_id AS player_id",
        "COALESCE(MAX(pl.nickname),'') AS nickname_display",
        "COUNT(*) AS maps_played",
        "SUM(COALESCE(ps.kills,0)) AS kills",
        "SUM(COALESCE(ps.deaths,0)) AS deaths",
        "SUM(COALESCE(ps.assists,0)) AS assists",
        "AVG(COALESCE(ps.adr,0)) AS adr",
        "AVG(COALESCE(ps.kr,0)) AS kr",
        "AVG(COALESCE(ps.hs_pct,0)) AS hs_pct",
        "SUM(COALESCE(ps.sniper_kills,0)) AS awp_kills",
        "SUM(COALESCE(ps.mk_2k,0)) AS k2",
        "SUM(COALESCE(ps.mk_3k,0)) AS k3",
        "SUM(COALESCE(ps.mk_4k,0)) AS k4",
        "SUM(COALESCE(ps.mk_5k,0)) AS k5",
        "SUM(COALESCE(ps.utility_damage,0)) AS util",
        "SUM(COALESCE(ps.damage,0)) AS damage",
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

    select_cols += [
        "SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) AS rounds",
        "SUM(COALESCE(ps.clutch_kills,0))    AS clutch_kills",
        "SUM(COALESCE(ps.cl_1v1_attempts,0)) AS c11_att",
        "SUM(COALESCE(ps.cl_1v1_wins,0))     AS c11_win",
        "SUM(COALESCE(ps.cl_1v2_attempts,0)) AS c12_att",
        "SUM(COALESCE(ps.cl_1v2_wins,0))     AS c12_win",
        "SUM(COALESCE(ps.entry_count,0))     AS entry_count",
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
      WHERE m.championship_id = ? AND ps.team_id = ?
      GROUP BY ps.player_id
      ORDER BY kills DESC
    """
    rows = query(con, sql, (division_id, team_id))

    out = []
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
            "nickname": r["nickname_display"],
            "maps_played": maps_played,
            "rounds": rounds,
            "rpm": rpm,
            "kd": kd,
            "adr": r["adr"] or 0.0,
            "kr": r["kr"] or 0.0,
            "kill": kills,
            "death": deaths,
            "assist": assists,
            "mvps": r.get("mvps", 0) or 0,
            "hs_pct": r["hs_pct"] or 0.0,
            "awp_kills": r["awp_kills"] or 0,
            "k2": r["k2"] or 0,
            "k3": r["k3"] or 0,
            "k4": r["k4"] or 0,
            "k5": r["k5"] or 0,
            "util": r["util"] or 0,
            "clutch_kills": r["clutch_kills"] or 0,
            "c11_att": r["c11_att"] or 0,
            "c11_win": r["c11_win"] or 0,
            "c12_att": r["c12_att"] or 0,
            "c12_win": r["c12_win"] or 0,
            "entry_count": r["entry_count"] or 0,
            "entry_win": r["entry_win"] or 0,
            "damage": r["damage"] or 0,
        }
        if "pistol_kills" in r.keys():
            row["pistol_kills"] = r["pistol_kills"] or 0
        if "flashed" in r.keys():        row["flashed"] = r["flashed"] or 0
        if "flash_count" in r.keys():    row["flash_count"] = r["flash_count"] or 0
        if "flash_successes" in r.keys():row["flash_successes"] = r["flash_successes"] or 0

        out.append(row)

    return out

def compute_champ_map_avgs_data(con: sqlite3.Connection, division_id: int) -> dict[str, tuple[float, float]]:
    """
    Palauttaa {map_name: (kd, adr)} koko divisioonalle.
    Lasketaan player_statsista:
      - kd = SUM(kills) / SUM(deaths) kartalla
      - adr = kierros-painotettu ADR kartalla (paino = kartan pelatut kierrokset = score1+score2)
    """
    rows = query(con, """
        SELECT
          mp.map_name                                     AS map,
          SUM(ps.kills)                                   AS kills,
          SUM(ps.deaths)                                  AS deaths,
          SUM( (COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) * COALESCE(ps.adr,0) ) AS adr_w,
          SUM(  COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0) )                        AS rw
        FROM player_stats ps
        JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        JOIN matches m  ON m.match_id  = ps.match_id
        WHERE m.championship_id = ?
        AND mp.map_name <> 'forfeit'
        GROUP BY mp.map_name
    """, (division_id,))

    out: dict[str, tuple[float, float]] = {}
    for r in rows:
        kills = r["kills"] or 0
        deaths = r["deaths"] or 0
        kd = (kills / deaths) if deaths else float(kills)
        adr = (r["adr_w"] / r["rw"]) if (r["rw"] or 0) > 0 else 0.0
        out[r["map"]] = (kd, adr)
    return out

def compute_map_stats_table_data(con, championship_id: int, team_id: str):
    """
    Palauttaa listan rivejä [{map, played, picks, opp_picks, wins, games, wr,
                               wins_own, games_own, wr_own,
                               wins_opp, games_opp, wr_opp,
                               kd, adr, rd, ban1, ban2, opp_ban, total_own_ban,
                               decov}]
    """
    # Map pool for the season; fallbacks if not present
    pool = get_season_map_pool(con, championship_id)
    if pool:
        all_maps = [r["map_id"] for r in pool]
    else:
        rows = query(con, """
            SELECT DISTINCT mp.map_name AS map_id
            FROM maps mp
            JOIN matches m ON m.match_id = mp.match_id
            WHERE m.championship_id = ?
                AND mp.map_name IS NOT NULL AND mp.map_name <> ''
                AND mp.map_name <> 'forfeit'
        """, (championship_id,))
        if rows:
            all_maps = [r["map_id"] for r in rows]
        else:
            all_maps = ["de_nuke","de_inferno","de_mirage","de_overpass","de_dust2","de_ancient","de_train","de_anubis"]

    values_sql = ", ".join([f"('{m}')" for m in all_maps])

    sql = f"""
        WITH allmaps(map) AS (
            VALUES {values_sql}
        ),
        my_matches AS (
            SELECT m.*
            FROM matches m
            WHERE m.championship_id = :champ
              AND (:team = m.team1_id OR :team = m.team2_id)
        ),
        team_maps AS (
            -- Pelatut kartat + W/L sekä pick-alkuperä
            SELECT
                mp.map_name AS map,
                CASE WHEN m.team1_id = :team THEN mp.score_team1 ELSE mp.score_team2 END AS rounds_for,
                CASE WHEN m.team1_id = :team THEN mp.score_team2 ELSE mp.score_team1 END AS rounds_against,
                CASE
                    WHEN m.team1_id = :team AND mp.score_team1 > mp.score_team2 THEN 1
                    WHEN m.team2_id = :team AND mp.score_team2 > mp.score_team1 THEN 1
                    ELSE 0
                END AS win,
                1 AS game,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id = :team
                ) THEN 1 ELSE 0 END AS own_pick,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id IS NOT NULL
                      AND v.selected_by_team_id <> :team
                ) THEN 1 ELSE 0 END AS opp_pick
            FROM my_matches m
            JOIN maps mp
              ON mp.match_id = m.match_id
             AND mp.round_index IS NOT NULL
        ),
        -- Omat dropit indeksoituna (1./2. ban)
        team_drops AS (
            SELECT
                v.match_id,
                v.map_name,
                v.selected_by_team_id,
                v.round_num,
                ROW_NUMBER() OVER (
                    PARTITION BY v.match_id, v.selected_by_team_id
                    ORDER BY COALESCE(v.round_num, 999), v.map_name
                ) AS drop_idx
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop'
              AND v.selected_by_team_id = :team
        ),
        -- Vastustajan dropit niissä matseissa joissa :team pelasi
        opp_drops AS (
            SELECT v.match_id, v.map_name
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop'
              AND (
                    (m.team1_id = :team AND v.selected_by_team_id = m.team2_id) OR
                    (m.team2_id = :team AND v.selected_by_team_id = m.team1_id)
                  )
        ),
        ban_counts AS (
            SELECT
                am.map,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 1), 0) AS ban1,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 2), 0) AS ban2,
                COALESCE((SELECT COUNT(*) FROM opp_drops od WHERE od.map_name = am.map), 0) AS opp_ban,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx IN (1,2)), 0) AS total_own_ban
            FROM allmaps am
        ),
        -- Joukkueen KD/ADR karttatasolla
        perf AS (
            SELECT
                mp.map_name AS map,
                SUM(ps.kills)  AS kills,
                SUM(ps.deaths) AS deaths,
                SUM( (COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) * COALESCE(ps.adr,0) ) AS adr_weighted,
                SUM(  COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0) )                          AS rounds_weight
            FROM player_stats ps
            JOIN my_matches m
              ON m.match_id = ps.match_id
            JOIN maps mp
              ON mp.match_id   = ps.match_id
             AND mp.round_index = ps.round_index
            WHERE ps.team_id = :team
            GROUP BY mp.map_name
        ),

        decov AS (
            SELECT
                v.map_name AS map,
                COUNT(*)   AS decov_cnt
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) IN ('decider','overflow')
            GROUP BY v.map_name
        )

        SELECT
            am.map                                                        AS map,
            COALESCE(COUNT(tm.map), 0)                                    AS played,
            COALESCE(SUM(tm.own_pick), 0)                                 AS picks,
            COALESCE(SUM(tm.opp_pick), 0)                                 AS opp_picks,

            COALESCE(SUM(tm.win), 0)                                      AS wins,
            COALESCE(SUM(tm.game), 0)                                     AS games,
            CASE WHEN COALESCE(SUM(tm.game),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(tm.win) / SUM(tm.game) END              AS wr,

            COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_own,
            COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0) AS games_own,
            CASE WHEN COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(CASE WHEN tm.own_pick=1 THEN tm.win ELSE 0 END)
                              / SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END) END AS wr_own,

            COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_opp,
            COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0) AS games_opp,
            CASE WHEN COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(CASE WHEN tm.opp_pick=1 THEN tm.win ELSE 0 END)
                              / SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END) END AS wr_opp,

            COALESCE(SUM(tm.rounds_for), 0) - COALESCE(SUM(tm.rounds_against), 0) AS rd,

            COALESCE(bc.ban1, 0)                                          AS ban1,
            COALESCE(bc.ban2, 0)                                          AS ban2,
            COALESCE(bc.opp_ban, 0)                                       AS opp_ban,
            COALESCE(bc.total_own_ban, 0)                                 AS total_own_ban,

            COALESCE(1.0 * p.kills / NULLIF(p.deaths,0), 0.0)             AS kd,
            COALESCE(1.0 * p.adr_weighted / NULLIF(p.rounds_weight,0), 0.0) AS adr,

            COALESCE(dc.decov_cnt, 0)                                     AS decov

        FROM allmaps am
        LEFT JOIN team_maps tm ON tm.map = am.map
        LEFT JOIN ban_counts bc ON bc.map = am.map
        LEFT JOIN perf p        ON p.map  = am.map
        LEFT JOIN decov dc      ON dc.map = am.map
        GROUP BY am.map
        ORDER BY am.map
    """

    rows = query(con, sql, {"champ": championship_id, "team": team_id})

    # pretty names
    catalog = get_maps_catalog_lookup(con)
    out = []
    for r in rows:
        mid = r.get("map")
        pretty = catalog.get(mid, {}).get("pretty_name")
        r["map_pretty"] = pretty if pretty else (mid or "").replace("de_", "").title()
        out.append(r)
    return out

def compute_champ_map_summary_data(con: sqlite3.Connection, division_id: int) -> dict:
    played_rows = query(con, """
        SELECT mp.map_name AS map_name, COUNT(*) AS c
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ? AND mp.map_name IS NOT NULL
        AND mp.map_name <> 'forfeit'
        GROUP BY mp.map_name
        ORDER BY c DESC, mp.map_name ASC
        LIMIT 4
    """, (division_id,))
    top_played = [(r["map_name"], r["c"]) for r in played_rows]

    ban_rows = query(con, """
        SELECT v.map_name AS map_name, COUNT(*) AS c
        FROM map_votes v
        JOIN matches m ON m.match_id = v.match_id
        WHERE m.championship_id = ?
          AND v.status = 'drop'
          AND v.map_name IS NOT NULL
        GROUP BY v.map_name
        ORDER BY c DESC, v.map_name ASC
        LIMIT 4
    """, (division_id,))
    top_banned = [(r["map_name"], r["c"]) for r in ban_rows]

    return {"top_played": top_played, "top_banned": top_banned}


def compute_champ_thresholds_data(con: sqlite3.Connection, division_id: int) -> dict:
    rows = query(con, """
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
        SUM(COALESCE(ps.cl_1v1_wins,0))   AS cl_1v1_wins,
        SUM(COALESCE(ps.cl_1v1_attempts,0))   AS cl_1v1_attempts,
        SUM(COALESCE(ps.cl_1v2_wins,0))   AS cl_1v2_wins,
        SUM(COALESCE(ps.cl_1v2_attempts,0))   AS cl_1v2_attempts,
        SUM(COALESCE(ps.enemies_flashed,0)) AS enemies_flashed,
        SUM(COALESCE(ps.flash_count,0))     AS flash_count,
        SUM(COALESCE(ps.flash_successes,0)) AS flash_successes
      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      JOIN maps mp   ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      WHERE m.championship_id = ?
      GROUP BY ps.player_id
    """, (division_id,))

    def _percentile(lst, q):
        lst = sorted(lst)
        if not lst:
            return 0.0
        pos = (len(lst) - 1) * q
        i = int(pos)
        frac = pos - i
        if i + 1 < len(lst):
            return lst[i] + frac * (lst[i + 1] - lst[i])
        return lst[i]

    def pack(lst, fallback=(0.0, 0.5, 1.0)):
        lst = [v for v in lst if v is not None]
        if not lst:
            return fallback
        p25 = _percentile(lst, 0.25)
        p50 = _percentile(lst, 0.50)
        p75 = _percentile(lst, 0.75)
        if p25 == p75:
            p25 = min(p25, p25 * 0.9)
            p75 = max(p75, p75 * 1.1 if p75 != 0 else 0.1)
        return (p25, p50, p75)

    kd_vals, adr_vals, kr_vals, hs_pct_vals, udpr_vals = [], [], [], [], []
    entrywr_vals, c11_vals, c12_vals, enem_per_flash_vals, survival_vals, rating1_vals = [], [], [], [], [], []
    flash_succ_vals = []

    for r in rows:
        kills  = r["kills"] or 0
        deaths = r["deaths"] or 0
        kd = (kills / deaths) if deaths else float(kills)

        adr = r["adr"] or 0.0
        kr  = r["kr"] or 0.0
        hs_pct = r["hs_pct"] or 0.0

        rounds = r["rounds"] or 0
        util   = r["util"] or 0
        udpr   = (util / rounds) if rounds else 0.0

        deaths_per_round = (deaths / rounds) if rounds else 0.0
        survival = max(0.0, 1.0 - deaths_per_round) * 100.0

        survival_ratio = survival / 100.0
        rating1 = ((kr / 0.679) + (survival_ratio / 0.317) + (adr / 79.9)) / 3.0

        ewin = r["entry_wins"]  or 0
        eatt = r["entry_count"] or 0
        entry_wr = (100.0 * ewin / eatt) if eatt else None

        c11_att = r.get("cl_1v1_attempts", 0) or 0
        c11_win = r.get("cl_1v1_wins", 0) or 0
        c11_wr = (c11_win / c11_att * 100.0) if c11_att else 0.0

        c12_att = r.get("cl_1v2_attempts", 0) or 0
        c12_win = r.get("cl_1v2_wins", 0) or 0
        c12_wr = (c12_win / c12_att * 100.0) if c12_att else 0.0

        efl = r["enemies_flashed"] or 0
        fct = r["flash_count"]     or 0
        enem_per_flash = (efl / fct) if fct else None

        fsu = r["flash_successes"] or 0
        flash_succ = (100.0 * fsu / fct) if fct else None  # percent 0..100

        kd_vals.append(kd)
        adr_vals.append(adr)
        kr_vals.append(kr)
        hs_pct_vals.append(hs_pct)
        udpr_vals.append(udpr)
        entrywr_vals.append(entry_wr)
        c11_vals.append(c11_wr)
        c12_vals.append(c12_wr)
        survival_vals.append(survival)
        rating1_vals.append(rating1)
        if enem_per_flash is not None:
            enem_per_flash_vals.append(enem_per_flash)
        if flash_succ is not None:
            flash_succ_vals.append(flash_succ)

    return {
        "kd":       pack(kd_vals),
        "adr":      pack(adr_vals),
        "kr":       pack(kr_vals),
        "hs_pct":   pack(hs_pct_vals),
        "udpr":     pack(udpr_vals),
        "entry_wr": pack(entrywr_vals, fallback=(30.0, 50.0, 70.0)),
        "c11_wr":   pack(c11_vals,    fallback=(30.0, 50.0, 70.0)),
        "c12_wr":   pack(c12_vals,    fallback=(30.0, 50.0, 70.0)),
        "enemies_per_flash": pack(enem_per_flash_vals, fallback=(0.3, 0.6, 0.9)),
        "flash_successes": pack(flash_succ_vals, fallback=(20.0, 40.0, 60.0)),  # Flash Succ %
        "survival": pack(survival_vals, fallback=(30.0, 50.0, 70.0)),
        "rating1":  pack(rating1_vals,  fallback=(0.85, 1.00, 1.15)),
    }

def upsert_match(con: sqlite3.Connection, row: dict) -> None:
    """
    Upsert 'matches' header. last_seen_at päivittyy aina.
    Ei tallenna joukkueiden nimiä; nimet haetaan teams-taulusta.
    """
    sql = """
    INSERT INTO matches(
      match_id, championship_id,
      best_of,
      configured_at, started_at, finished_at, scheduled_at, status,
      last_seen_at,
      team1_id, team2_id, winner_team_id
    ) VALUES (
      :match_id, :championship_id,
      :best_of,
      :configured_at, :started_at, :finished_at, :scheduled_at, :status,
      strftime('%s','now'),
      :team1_id, :team2_id, :winner_team_id
    )
    ON CONFLICT(match_id) DO UPDATE SET
      best_of       = COALESCE(excluded.best_of,       matches.best_of),

      configured_at = COALESCE(excluded.configured_at, matches.configured_at),
      started_at    = COALESCE(excluded.started_at,    matches.started_at),
      finished_at   = COALESCE(excluded.finished_at,   matches.finished_at),
      scheduled_at  = COALESCE(excluded.scheduled_at,  matches.scheduled_at),
      status        = COALESCE(excluded.status,        matches.status),

      team1_id      = COALESCE(excluded.team1_id,      matches.team1_id),
      team2_id      = COALESCE(excluded.team2_id,      matches.team2_id),
      winner_team_id= COALESCE(excluded.winner_team_id, matches.winner_team_id),

      last_seen_at  = strftime('%s','now')
    """
    con.execute(sql, row)

def upsert_maps(con, match_id: str, rounds: list[dict]):
    sql = """
    INSERT INTO maps(match_id, round_index, map_name, score_team1, score_team2, winner_team_id)
    VALUES(:match_id, :round_index, :map_name, :score_team1, :score_team2, :winner_team_id)
    ON CONFLICT(match_id, round_index) DO UPDATE SET
      map_name=excluded.map_name,
      score_team1=excluded.score_team1,
      score_team2=excluded.score_team2,
      winner_team_id=excluded.winner_team_id
    """
    payload = [{**r, "match_id": match_id} for r in rounds]
    con.executemany(sql, payload)

def upsert_map_votes(con, match_id: str, votes: list[dict]):
    """
    Replace all veto rows for a match to avoid duplicates between sync runs.
    votes: {round_num, map_name, status, selected_by_faction, selected_by_team_id}
    """
    con.execute("DELETE FROM map_votes WHERE match_id = ?", (match_id,))
    sql = """
    INSERT INTO map_votes(match_id, round_num, map_name, status, selected_by_faction, selected_by_team_id)
    VALUES(:match_id, :round_num, :map_name, :status, :selected_by_faction, :selected_by_team_id)
    """
    payload = [{**v, "match_id": match_id} for v in votes]
    con.executemany(sql, payload)

def upsert_player_stats(con, match_id: str, rows: list[dict]):
    sql = """
    INSERT INTO player_stats(
      match_id, round_index, player_id, team_id,
      kills, deaths, assists, kd, kr, adr, hs_pct, mvps, sniper_kills, utility_damage,
      enemies_flashed, flash_count, flash_successes,
      mk_2k, mk_3k, mk_4k, mk_5k,
      clutch_kills, cl_1v1_attempts, cl_1v1_wins, cl_1v2_attempts, cl_1v2_wins,
      entry_count, entry_wins, pistol_kills, damage
    )
    VALUES(
      :match_id, :round_index, :player_id, :team_id,
      :kills, :deaths, :assists, :kd, :kr, :adr, :hs_pct, :mvps, :sniper_kills, :utility_damage,
      :enemies_flashed, :flash_count, :flash_successes,
      :mk_2k, :mk_3k, :mk_4k, :mk_5k,
      :clutch_kills, :cl_1v1_attempts, :cl_1v1_wins, :cl_1v2_attempts, :cl_1v2_wins,
      :entry_count, :entry_wins, :pistol_kills, :damage
    )
    ON CONFLICT(match_id, round_index, player_id) DO UPDATE SET
      team_id=excluded.team_id,
      kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists, kd=excluded.kd,
      kr=excluded.kr, adr=excluded.adr, hs_pct=excluded.hs_pct, mvps=excluded.mvps,
      sniper_kills=excluded.sniper_kills, utility_damage=excluded.utility_damage,
      enemies_flashed=excluded.enemies_flashed, flash_count=excluded.flash_count, flash_successes=excluded.flash_successes,
      mk_2k=excluded.mk_2k, mk_3k=excluded.mk_3k, mk_4k=excluded.mk_4k, mk_5k=excluded.mk_5k,
      clutch_kills=excluded.clutch_kills, cl_1v1_attempts=excluded.cl_1v1_attempts, cl_1v1_wins=excluded.cl_1v1_wins,
      cl_1v2_attempts=excluded.cl_1v2_attempts, cl_1v2_wins=excluded.cl_1v2_wins,
      entry_count=excluded.entry_count, entry_wins=excluded.entry_wins,
      pistol_kills=excluded.pistol_kills, damage=excluded.damage
    """
    payload = [{**r, "match_id": match_id} for r in rows]
    con.executemany(sql, payload)

def get_team_matches_mirror(con: sqlite3.Connection, championship_id: int, team_id: str) -> list[dict]:
    cur = con.cursor()
    sql = """
    WITH my_matches AS (
      SELECT
        m.match_id, m.championship_id, m.team1_id, m.team2_id,
        m.best_of, m.status,
        COALESCE(m.started_at, m.scheduled_at, m.configured_at, 0) AS ts,
        CASE WHEN m.finished_at IS NOT NULL THEN 1 ELSE 0 END AS played
      FROM matches m
      WHERE m.championship_id = :champ
        AND (:team = m.team1_id OR :team = m.team2_id)
    ),
    mp AS (
      SELECT
        mm.match_id, mm.team1_id, mm.team2_id,
        mm.best_of, mm.status, mm.ts, mm.played,
        ma.round_index, ma.map_name, ma.score_team1, ma.score_team2
      FROM my_matches mm
      LEFT JOIN maps ma ON ma.match_id = mm.match_id
    ),
    ps_agg AS (
      SELECT
        ps.match_id, ps.round_index, ps.team_id,
        SUM(COALESCE(ps.kills,0))   AS kills,
        SUM(COALESCE(ps.deaths,0))  AS deaths,
        SUM(COALESCE(ps.damage,0))  AS dmg,
        AVG(NULLIF(ps.adr,0))       AS adr_avg
      FROM player_stats ps
      JOIN my_matches m ON m.match_id = ps.match_id
      GROUP BY ps.match_id, ps.round_index, ps.team_id
    ),
    picks AS (
      SELECT v.match_id, v.map_name,
             MAX(v.selected_by_team_id) AS pick_team_id
      FROM map_votes v
      JOIN my_matches m ON m.match_id = v.match_id
      WHERE v.status = 'pick'
      GROUP BY v.match_id, v.map_name
    )
    SELECT
      mp.match_id, mp.ts, mp.status, mp.best_of, mp.played,
      mp.team1_id, mp.team2_id,
      t1.name AS team1_name, t2.name AS team2_name,
      t1.avatar AS t1_avatar, t2.avatar AS t2_avatar,
      mp.round_index, mp.map_name, mp.score_team1, mp.score_team2,
      pk.pick_team_id,
      COALESCE(ps1.kills, 0)      AS t1_kills,
      COALESCE(ps1.deaths, 0)     AS t1_deaths,
      COALESCE(ps1.adr_avg, 0.0)  AS t1_adr,
      COALESCE(ps1.dmg, 0)        AS t1_dmg,
      COALESCE(ps2.kills, 0)      AS t2_kills,
      COALESCE(ps2.deaths, 0)     AS t2_deaths,
      COALESCE(ps2.adr_avg, 0.0)  AS t2_adr,
      COALESCE(ps2.dmg, 0)        AS t2_dmg
    FROM mp
    LEFT JOIN ps_agg ps1 ON ps1.match_id=mp.match_id AND ps1.round_index=mp.round_index AND ps1.team_id=mp.team1_id
    LEFT JOIN ps_agg ps2 ON ps2.match_id=mp.match_id AND ps2.round_index=mp.round_index AND ps2.team_id=mp.team2_id
    LEFT JOIN picks pk    ON pk.match_id=mp.match_id AND pk.map_name=mp.map_name
    LEFT JOIN teams t1    ON t1.team_id = mp.team1_id
    LEFT JOIN teams t2    ON t2.team_id = mp.team2_id
    ORDER BY (mp.ts IS NULL) ASC, mp.ts ASC, mp.match_id ASC, mp.round_index ASC
    """
    rows = cur.execute(sql, {"champ": championship_id, "team": team_id}).fetchall()

    out: dict[str, dict] = {}
    for r in rows:
        mid = r["match_id"]
        if mid not in out:
            me_on_left = (r["team1_id"] == team_id)
            opp_id = r["team2_id"] if me_on_left else r["team1_id"]
            opp_name  = (r["team2_name"] if me_on_left else r["team1_name"])
            opp_avatar= (r["t2_avatar"]  if me_on_left else r["t1_avatar"])
            my_name   = (r["team1_name"] if me_on_left else r["team2_name"])

            out[mid] = {
                "match_id": mid,
                "status": r["status"],
                "best_of": r["best_of"],
                "ts": r["ts"],
                "played": int(r["played"] or 0),
                "left":  {"team_id": team_id, "team_name": my_name or ""},
                "right": {"team_id": opp_id,   "team_name": opp_name or "", "avatar": opp_avatar},
                "maps": []
            }

        if r["round_index"] is None:
            continue

        me_is_t1 = (r["team1_id"] == team_id)
        rf = (r["score_team1"] if me_is_t1 else r["score_team2"])
        ra = (r["score_team2"] if me_is_t1 else r["score_team1"])

        me_kills  = (r["t1_kills"]  if me_is_t1 else r["t2_kills"])  or 0
        me_deaths = (r["t1_deaths"] if me_is_t1 else r["t2_deaths"]) or 0
        me_adr    = (r["t1_adr"]    if me_is_t1 else r["t2_adr"])    or 0.0
        me_damage = (r["t1_dmg"]    if me_is_t1 else r["t2_dmg"])    or 0

        opp_kills  = (r["t2_kills"]  if me_is_t1 else r["t1_kills"])  or 0
        opp_deaths = (r["t2_deaths"] if me_is_t1 else r["t1_deaths"]) or 0
        opp_adr    = (r["t2_adr"]    if me_is_t1 else r["t1_adr"])    or 0.0
        opp_damage = (r["t2_dmg"]    if me_is_t1 else r["t1_dmg"])    or 0

        me_kd  = (float(me_kills) / me_deaths) if me_deaths else float(me_kills)
        opp_kd = (float(opp_kills) / opp_deaths) if opp_deaths else float(opp_kills)

        out[mid]["maps"].append({
            "round_index": r["round_index"],
            "map": r["map_name"],
            "rf": rf if rf is not None else 0,
            "ra": ra if ra is not None else 0,
            "pick_team_id": r["pick_team_id"],
            "left":  {"adr": float(me_adr or 0.0),  "kd": float(me_kd),  "dmg": int(me_damage),  "kills": int(me_kills),  "deaths": int(me_deaths)},
            "right": {"adr": float(opp_adr or 0.0), "kd": float(opp_kd), "dmg": int(opp_damage), "kills": int(opp_kills), "deaths": int(opp_deaths)}
        })

    return [out[mid] for mid in sorted(out, key=lambda k: (out[k]["ts"] is None, out[k]["ts"] or 0, k))]

# --- Maps catalog helpers ----------------------------------------------------

def normalize_map_id(name: str) -> str:
    """Return canonical map_id like 'de_ancient' from variants like 'Ancient', 'de_ancient', 'ancient'."""
    if not name:
        return ""
    s = str(name).strip().lower().replace(" ", "")
    if not s.startswith("de_"):
        s = "de_" + s
    return s

def upsert_map_catalog(con: sqlite3.Connection, row: dict) -> None:
    """
    row: {map_id, pretty_name, image_sm, image_lg}
    """
    sql = """
    INSERT INTO maps_catalog (map_id, pretty_name, image_sm, image_lg, first_seen_at, last_seen_at)
    VALUES (:map_id, :pretty_name, :image_sm, :image_lg, strftime('%s','now'), strftime('%s','now'))
    ON CONFLICT(map_id) DO UPDATE SET
      pretty_name = COALESCE(excluded.pretty_name, maps_catalog.pretty_name),
      image_sm    = COALESCE(NULLIF(excluded.image_sm,''), maps_catalog.image_sm),
      image_lg    = COALESCE(NULLIF(excluded.image_lg,''), maps_catalog.image_lg),
      last_seen_at= strftime('%s','now')
    """
    con.execute(sql, row)


def add_map_to_season_pool(con: sqlite3.Connection, season: int, map_id: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO map_pool_seasons (season, map_id) VALUES (?, ?)",
        (int(season), map_id)
    )

def get_map_art(con: sqlite3.Connection, map_name_or_id: str) -> dict | None:
    """
    Return {'map_id','pretty_name','image_sm','image_lg'} for given map name/id, or None.
    """
    mid = normalize_map_id(map_name_or_id)
    cur = con.execute("SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog WHERE map_id=?", (mid,))
    row = cur.fetchone()
    if not row:
        return None
    return {"map_id": row[0], "pretty_name": row[1], "image_sm": row[2], "image_lg": row[3]}

def get_season_map_pool(con: sqlite3.Connection, championship_id: int) -> list[dict]:
    """
    Palauttaa [{map_id, pretty_name}] championshipin seasonin map-poolista.
    """
    sql = """
    SELECT mc.map_id,
           COALESCE(NULLIF(mc.pretty_name,''), mc.map_id) AS pretty_name
    FROM championships c
    JOIN map_pool_seasons s ON s.season = c.season
    JOIN maps_catalog mc    ON mc.map_id = s.map_id
    WHERE c.championship_id = ?
    ORDER BY pretty_name COLLATE NOCASE
    """
    return query(con, sql, (championship_id,))

def get_maps_catalog_lookup(con: sqlite3.Connection) -> dict[str, dict]:
    """
    map_id -> {pretty_name, image_sm, image_lg}
    """
    rows = query(con, "SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog", ())
    return {r["map_id"]: r for r in rows}

def get_division_generated_ts(con: sqlite3.Connection, championship_id: str) -> int | None:
    """
    Returns latest sync timestamp (epoch seconds) for given championship,
    based on matches.last_seen_at MAX.
    """
    cur = con.execute(
        "SELECT MAX(last_seen_at) AS ts FROM matches WHERE championship_id = ?",
        (championship_id,)
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

def get_max_last_seen_for_champs(con: sqlite3.Connection, champ_ids: list[str]) -> int | None:
    """
    Returns MAX(last_seen_at) across the given championships.
    If list is empty or no data, returns None.
    """
    if not champ_ids:
        return None
    placeholders = ",".join(["?"] * len(champ_ids))
    sql = f"SELECT MAX(last_seen_at) FROM matches WHERE championship_id IN ({placeholders})"
    cur = con.execute(sql, champ_ids)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

_TS_EXPR = "COALESCE(m.finished_at, m.started_at, m.scheduled_at, m.configured_at, m.last_seen_at, 0)"


def _get_team_last_prev_ts(con: sqlite3.Connection, division_id: int, team_id: str) -> tuple[int | None, int | None]:
    """
    Returns (curr_ts, prev_ts) for this team within the championship,
    based on the timestamp expression used throughout the site.
    curr_ts = timestamp of most recent match the team played (with at least one map row)
    prev_ts = previous one, or None if not available
    """
    rows = query(con, f"""
        SELECT DISTINCT { _TS_EXPR } AS ts
        FROM matches m
        WHERE m.championship_id=? AND (m.team1_id=? OR m.team2_id=?)
          AND EXISTS (SELECT 1 FROM maps mp WHERE mp.match_id = m.match_id)
        ORDER BY ts ASC
    """, (division_id, team_id, team_id))
    if not rows:
        return (None, None)
    curr_ts = rows[-1]["ts"]
    prev_ts = rows[-2]["ts"] if len(rows) >= 2 else None
    return (curr_ts, prev_ts)

def compute_team_summary_with_delta(con: sqlite3.Connection, division_id: int, team_id: str) -> dict:
    """
    Team summary delta = (agg <= curr_ts) - (agg <= curr_ts-1)
    Uses true per-round metrics for KR/ADR.
    """
    curr_ts, _ = _get_team_last_prev_ts(con, division_id, team_id)

    def _summary_until(cutoff: int | None) -> dict:
        if cutoff is None:
            return {"matches_played":0,"maps_played":0,"w":0,"l":0,"rd":0,"kd":0.0,"kr":0.0,"adr":0.0,"util":0}

        rows = query(con, f"""
            SELECT m.match_id, m.team1_id, m.team2_id,
                   mp.score_team1, mp.score_team2, mp.winner_team_id
            FROM matches m
            JOIN maps mp ON mp.match_id = m.match_id
            WHERE m.championship_id=? AND (m.team1_id=? OR m.team2_id=?)
              AND { _TS_EXPR } <= ?
              AND mp.map_name <> 'forfeit'
        """, (division_id, team_id, team_id, cutoff))
        mids = {r["match_id"] for r in rows}
        maps_played = len(rows)
        matches_played = len(mids)
        maps_w = sum(1 for r in rows if r.get("winner_team_id") == team_id)

        rd = 0
        for r in rows:
            s1 = r.get("score_team1") or 0
            s2 = r.get("score_team2") or 0
            if r["team1_id"] == team_id:
                rd += (s1 - s2)
            elif r["team2_id"] == team_id:
                rd += (s2 - s1)

        if maps_played == 0:
            return {"matches_played":0,"maps_played":0,"w":0,"l":0,"rd":0,"kd":0.0,"kr":0.0,"adr":0.0,"util":0}

        rounds_sum = sum(((r.get("score_team1") or 0) + (r.get("score_team2") or 0)) for r in rows)

        agg = query(con, f"""
            SELECT
            SUM(COALESCE(ps.kills,0))           AS kills,
            SUM(COALESCE(ps.deaths,0))          AS deaths,
            SUM(COALESCE(ps.damage,0))          AS damage,
            SUM(COALESCE(ps.utility_damage,0))  AS util
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            WHERE ps.team_id=? AND m.championship_id=? AND { _TS_EXPR } <= ?
        """, (team_id, division_id, cutoff))[0]

        kills   = int(agg["kills"] or 0)
        deaths  = int(agg["deaths"] or 0)
        damage  = int(agg["damage"] or 0)
        rounds  = int(rounds_sum)
        util    = int(agg["util"] or 0)

        kd  = (kills / deaths) if deaths else (float(kills) if rounds else 0.0)
        kr  = (kills / rounds) if rounds else 0.0
        adr = (damage / rounds) if rounds else 0.0

        return {
            "matches_played": matches_played,
            "maps_played": maps_played,
            "w": maps_w,
            "l": maps_played - maps_w,
            "rd": rd,
            "kd": float(kd),
            "kr": float(kr),
            "adr": float(adr),
            "util": util
        }

    if curr_ts is None:
        zero = _summary_until(None)
        return {"curr": zero, "prev": None, "delta": None}

    prev_cutoff = max(0, int(curr_ts) - 1)

    prev = _summary_until(prev_cutoff)
    curr = _summary_until(curr_ts)

    # Jos ennen viimeisintä ei ollut dataa → delta None
    if (prev["matches_played"]==0 and prev["maps_played"]==0 and prev["w"]==0 and prev["l"]==0 and
        prev["rd"]==0 and prev["kd"]==0.0 and prev["kr"]==0.0 and prev["adr"]==0.0 and prev["util"]==0):
        return {"curr": curr, "prev": None, "delta": None}

    delta = {k: (curr[k] - prev[k]) for k in ["matches_played","maps_played","w","l","rd","util","kd","kr","adr"]}
    return {"curr": curr, "prev": prev, "delta": delta}

# -----------------------------
# Player deltas (row aggregates)
# -----------------------------

def _player_agg_until(con: sqlite3.Connection, division_id: int, team_id: str, player_id: str, cutoff: int | None) -> dict:
    """
    Aggregates player stats up to cutoff. Returns 0/None defaults for missing data.
    Uses per-round true metrics:
      - adr = total_damage / total_rounds
      - kr  = total_kills  / total_rounds
      - kd  = kills / deaths  (kills if deaths=0 and rounds>0 else 0.0)
    """
    if cutoff is None:
        return {
            "maps_played": 0, "rounds": 0,
            "kills": 0, "deaths": 0, "assists": 0, "damage": 0,
            "adr": 0.0, "kr": 0.0, "kd": 0.0,
            "hs_pct": 0.0, "k2": 0, "k3": 0, "k4": 0, "k5": 0,
            "mvps": 0, "util": 0, "udpr": 0.0,
            "flashed": 0, "flash_count": 0, "flash_successes": 0,
            "entry_count": 0, "entry_wins": 0,
            "clutch_kills": 0, "c11_att": 0, "c11_win": 0, "c12_att": 0, "c12_win": 0,
            "awp": 0, "pistol_kills": 0
        }

    row = query(con, f"""
        SELECT
          COUNT(*) AS maps_played,
          SUM(COALESCE(mp.score_team1,0) + COALESCE(mp.score_team2,0)) AS rounds,
          SUM(COALESCE(ps.kills,0))          AS kills,
          SUM(COALESCE(ps.deaths,0))         AS deaths,
          SUM(COALESCE(ps.assists,0))        AS assists,
          SUM(COALESCE(ps.damage,0))         AS damage,
          AVG(COALESCE(ps.hs_pct,0))         AS hs_pct,
          SUM(COALESCE(ps.mk_2k,0))          AS k2,
          SUM(COALESCE(ps.mk_3k,0))          AS k3,
          SUM(COALESCE(ps.mk_4k,0))          AS k4,
          SUM(COALESCE(ps.mk_5k,0))          AS k5,
          SUM(COALESCE(ps.mvps,0))           AS mvps,
          SUM(COALESCE(ps.utility_damage,0)) AS util,
          SUM(COALESCE(ps.enemies_flashed,0)) AS flashed,
          SUM(COALESCE(ps.flash_count,0))     AS flash_count,
          SUM(COALESCE(ps.flash_successes,0)) AS flash_successes,
          SUM(COALESCE(ps.entry_count,0))     AS entry_count,
          SUM(COALESCE(ps.entry_wins,0))      AS entry_wins,
          SUM(COALESCE(ps.clutch_kills,0))    AS clutch_kills,
          SUM(COALESCE(ps.cl_1v1_attempts,0)) AS c11_att,
          SUM(COALESCE(ps.cl_1v1_wins,0))     AS c11_win,
          SUM(COALESCE(ps.cl_1v2_attempts,0)) AS c12_att,
          SUM(COALESCE(ps.cl_1v2_wins,0))     AS c12_win,
          SUM(COALESCE(ps.sniper_kills,0))    AS awp,
          SUM(COALESCE(ps.pistol_kills,0))    AS pistol_kills
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE m.championship_id=? AND ps.team_id=? AND ps.player_id=? AND { _TS_EXPR } <= ?
    """, (division_id, team_id, player_id, cutoff))[0]

    rounds = int(row["rounds"] or 0)
    kills  = int(row["kills"]  or 0)
    deaths = int(row["deaths"] or 0)
    damage = int(row["damage"] or 0)

    kd  = (kills / deaths) if deaths else (float(kills) if rounds else 0.0)
    kr  = (kills / rounds) if rounds else 0.0
    adr = (damage / rounds) if rounds else 0.0
    udpr = (float(row["util"] or 0) / rounds) if rounds else 0.0

    return {
        "maps_played": int(row["maps_played"] or 0),
        "rounds": rounds,
        "kills": kills, "deaths": deaths, "assists": int(row["assists"] or 0),
        "damage": damage,
        "adr": float(adr), "kr": float(kr), "kd": float(kd),
        "hs_pct": float(row["hs_pct"] or 0.0),
        "k2": int(row["k2"] or 0), "k3": int(row["k3"] or 0), "k4": int(row["k4"] or 0), "k5": int(row["k5"] or 0),
        "mvps": int(row["mvps"] or 0),
        "util": int(row["util"] or 0), "udpr": float(udpr),
        "flashed": int(row["flashed"] or 0), "flash_count": int(row["flash_count"] or 0), "flash_successes": int(row["flash_successes"] or 0),
        "entry_count": int(row["entry_count"] or 0), "entry_wins": int(row["entry_wins"] or 0),
        "clutch_kills": int(row["clutch_kills"] or 0),
        "c11_att": int(row["c11_att"] or 0), "c11_win": int(row["c11_win"] or 0),
        "c12_att": int(row["c12_att"] or 0), "c12_win": int(row["c12_win"] or 0),
        "awp": int(row["awp"] or 0),
        "pistol_kills": int(row["pistol_kills"] or 0),
    }

def compute_player_deltas(con: sqlite3.Connection, division_id: int, team_id: str) -> dict[str, dict]:
    """
    Delta = (agg <= curr_ts) - (agg <= curr_ts-1)
    eli viimeisimmän matsin nettovaikutus kumulatiivisiin arvoihin.
    """
    curr_ts, _ = _get_team_last_prev_ts(con, division_id, team_id)
    if curr_ts is None:
        return {}

    prev_cutoff = max(0, int(curr_ts) - 1)

    # Kauden pelaajat (joilta on havaittu statsia)
    pids = [r["player_id"] for r in query(con, """
      SELECT DISTINCT ps.player_id
      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      WHERE m.championship_id=? AND ps.team_id=?
    """, (division_id, team_id))]

    out: dict[str, dict] = {}
    for pid in pids:
        prev = _player_agg_until(con, division_id, team_id, pid, prev_cutoff)
        curr = _player_agg_until(con, division_id, team_id, pid, curr_ts)

        # Jos ennen viimeisintä ei ollut mitään, näytä prev=None, delta=None (UI näyttää "(no prev)")
        if prev["maps_played"] == 0 and prev["rounds"] == 0 and prev["kills"] == 0 and prev["deaths"] == 0 and prev["assists"] == 0:
            out[pid] = {"curr": curr, "prev": None, "delta": None}
        else:
            delta = {}
            for k in curr.keys():
                delta[k] = (curr[k] - prev[k]) if isinstance(curr[k], (int, float)) else None
            out[pid] = {"curr": curr, "prev": prev, "delta": delta}
    return out

# -----------------------------
# Map deltas (per map rows)
# -----------------------------

def compute_map_stats_table_data_until(con: sqlite3.Connection, championship_id: int, team_id: str, cutoff_ts: int) -> list[dict]:
    """
    Same as compute_map_stats_table_data but only counting matches where _TS_EXPR <= cutoff_ts.
    Note: 'dates' removed; only 'decov' is returned.
    """
    pool = get_season_map_pool(con, championship_id)
    if pool:
        all_maps = [r["map_id"] for r in pool]
    else:
        rows = query(con, "SELECT DISTINCT map_id FROM maps_catalog", ())
        all_maps = [r["map_id"] for r in rows] if rows else [
            "de_nuke","de_inferno","de_mirage","de_overpass","de_dust2","de_ancient","de_train","de_anubis"
        ]
    values_sql = ", ".join([f"('{m}')" for m in all_maps])

    sql = f"""
        WITH allmaps(map) AS ( VALUES {values_sql} ),

        my_matches AS (
            SELECT m.*
            FROM matches m
            WHERE m.championship_id = :champ
              AND (:team = m.team1_id OR :team = m.team2_id)
              AND { _TS_EXPR } <= :cutoff
        ),

        team_maps AS (
            SELECT
                mp.map_name AS map,
                CASE WHEN m.team1_id = :team THEN mp.score_team1 ELSE mp.score_team2 END AS rounds_for,
                CASE WHEN m.team1_id = :team THEN mp.score_team2 ELSE mp.score_team1 END AS rounds_against,
                CASE
                    WHEN m.team1_id = :team AND mp.score_team1 > mp.score_team2 THEN 1
                    WHEN m.team2_id = :team AND mp.score_team2 > mp.score_team1 THEN 1
                    ELSE 0
                END AS win,
                1 AS game,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id = :team
                ) THEN 1 ELSE 0 END AS own_pick,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id IS NOT NULL
                      AND v.selected_by_team_id <> :team
                ) THEN 1 ELSE 0 END AS opp_pick
            FROM my_matches m
            JOIN maps mp ON mp.match_id = m.match_id AND mp.round_index IS NOT NULL
        ),

        team_drops AS (
            SELECT
                v.match_id,
                v.map_name,
                v.selected_by_team_id,
                v.round_num,
                ROW_NUMBER() OVER (
                    PARTITION BY v.match_id, v.selected_by_team_id
                    ORDER BY COALESCE(v.round_num, 999), v.map_name
                ) AS drop_idx
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop' AND v.selected_by_team_id = :team
        ),

        opp_drops AS (
            SELECT v.match_id, v.map_name
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop' AND (
                (m.team1_id = :team AND v.selected_by_team_id = m.team2_id) OR
                (m.team2_id = :team AND v.selected_by_team_id = m.team1_id)
            )
        ),

        ban_counts AS (
            SELECT am.map,
                   COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 1), 0) AS ban1,
                   COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 2), 0) AS ban2,
                   COALESCE((SELECT COUNT(*) FROM opp_drops  od WHERE od.map_name = am.map), 0) AS opp_ban,
                   COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx IN (1,2)), 0) AS total_own_ban
            FROM allmaps am
        ),

        perf AS (
            SELECT mp.map_name AS map,
                   SUM(ps.kills) AS kills,
                   SUM(ps.deaths) AS deaths,
                   SUM( (COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) * COALESCE(ps.adr,0) ) AS adr_weighted,
                   SUM( COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0) ) AS rounds_weight
            FROM player_stats ps
            JOIN my_matches m ON m.match_id = ps.match_id
            JOIN maps mp       ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
            WHERE ps.team_id = :team
            GROUP BY mp.map_name
        ),

        decov AS (
            SELECT
                v.map_name AS map,
                COUNT(*)   AS decov_cnt
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) IN ('decider','overflow')
            GROUP BY v.map_name
        )

        SELECT am.map AS map,

               COALESCE(COUNT(tm.map), 0) AS played,
               COALESCE(SUM(tm.own_pick), 0) AS picks,
               COALESCE(SUM(tm.opp_pick), 0) AS opp_picks,

               COALESCE(SUM(tm.win), 0) AS wins,
               COALESCE(SUM(tm.game), 0) AS games,
               CASE WHEN COALESCE(SUM(tm.game),0)=0 THEN 0.0
                    ELSE 100.0 * SUM(tm.win) / SUM(tm.game) END AS wr,

               COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_own,
               COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0) AS games_own,
               CASE WHEN COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                    ELSE 100.0 * SUM(CASE WHEN tm.own_pick=1 THEN tm.win ELSE 0 END)
                                 / SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END) END AS wr_own,

               COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_opp,
               COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0) AS games_opp,
               CASE WHEN COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                    ELSE 100.0 * SUM(CASE WHEN tm.opp_pick=1 THEN tm.win ELSE 0 END)
                                 / SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END) END AS wr_opp,

               COALESCE(SUM(tm.rounds_for), 0) - COALESCE(SUM(tm.rounds_against), 0) AS rd,

               COALESCE(bc.ban1, 0) AS ban1,
               COALESCE(bc.ban2, 0) AS ban2,
               COALESCE(bc.opp_ban, 0) AS opp_ban,
               COALESCE(bc.total_own_ban, 0) AS total_own_ban,

               COALESCE(1.0 * p.kills / NULLIF(p.deaths, 0), 0.0)            AS kd,
               COALESCE(1.0 * p.adr_weighted / NULLIF(p.rounds_weight, 0), 0.0) AS adr,

               COALESCE(dc.decov_cnt, 0) AS decov

        FROM allmaps am
        LEFT JOIN team_maps tm ON tm.map = am.map
        LEFT JOIN ban_counts bc ON bc.map = am.map
        LEFT JOIN perf p        ON p.map  = am.map
        LEFT JOIN decov dc      ON dc.map = am.map
        GROUP BY am.map
        ORDER BY am.map COLLATE NOCASE
    """

    res = con.execute(sql, {"champ": championship_id, "team": team_id, "cutoff": cutoff_ts}).fetchall()
    out = [dict(r) for r in res]
    return out

def compute_map_stats_with_delta(con: sqlite3.Connection, championship_id: int, team_id: str) -> dict[str, dict]:
    """
    Map-delta = (agg <= curr_ts) - (agg <= curr_ts-1)
    """
    curr_ts, _ = _get_team_last_prev_ts(con, championship_id, team_id)
    if curr_ts is None:
        return {}

    prev_cutoff = max(0, int(curr_ts) - 1)

    curr = compute_map_stats_table_data_until(con, championship_id, team_id, curr_ts)
    prev = compute_map_stats_table_data_until(con, championship_id, team_id, prev_cutoff)

    curr_by = {r["map"]: r for r in curr}
    prev_by = {r["map"]: r for r in prev}

    out: dict[str, dict] = {}
    for m, c in curr_by.items():
        p = prev_by.get(m)
        if not p:
            out[m] = {"curr": c, "prev": None, "delta": None}
        else:
            d = {}
            for k, v in c.items():
                if isinstance(v, (int, float)):
                    d[k] = v - (p.get(k) or 0)
            out[m] = {"curr": c, "prev": p, "delta": d}
    return out

def get_champ_last_seen(con: sqlite3.Connection, championship_id: int | str) -> int | None:
    """
    Returns the latest 'last_seen_at' timestamp for a championship from matches table.
    Fallback to max of configured/started/finished/scheduled if last_seen_at is NULLs.
    """
    row = con.execute("""
        SELECT MAX(COALESCE(last_seen_at, configured_at, started_at, finished_at, scheduled_at, 0)) AS ts
        FROM matches
        WHERE championship_id = ?
    """, (championship_id,)).fetchone()
    ts = row["ts"] if row else None
    return int(ts) if ts else None

def upsert_players_bulk(con: sqlite3.Connection, players: list[dict]) -> None:
    """
    Bulk upsert for players to reduce per-row INSERT/UPDATE overhead.
    players: list of dicts with keys {player_id, nickname, updated_at?}
    """
    if not players:
        return
    payload = []
    for p in players:
        payload.append({
            "player_id":  p.get("player_id"),
            "nickname":   (p.get("nickname") or ""),
            "updated_at": p.get("updated_at"),
        })
    sql = """
    INSERT INTO players (player_id, nickname, updated_at)
    VALUES (:player_id, :nickname, COALESCE(:updated_at, strftime('%s','now')))
    ON CONFLICT(player_id) DO UPDATE SET
      nickname   = CASE WHEN players.nickname IS NULL OR players.nickname='' THEN excluded.nickname ELSE players.nickname END,
      updated_at = COALESCE(excluded.updated_at, players.updated_at)
    """
    con.executemany(sql, payload)
