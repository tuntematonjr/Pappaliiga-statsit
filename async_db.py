# async_db.py
# Async SQLite helpers for Pappaliiga CS (championship-centric).
# Async versions of all db.py functions using aiosqlite

from __future__ import annotations
import aiosqlite
import asyncio
import json
from pathlib import Path
from collections.abc import Collection
from typing import Any, Dict, Iterable, List, Optional, AsyncContextManager
from contextlib import asynccontextmanager

from stats_utils import weighted_percentile

# Import original schema and constants from db.py
from db import SCHEMA_PATH, DEFAULT_TEAM_AVATAR, has_column

# Global connection pool
_connection_pool: Optional[AsyncConnectionPool] = None

class AsyncConnectionPool:
    # Async SQLite connection pool with resource management
    
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
        self._connections: List[aiosqlite.Connection] = []
        self._available = asyncio.Queue()
        self._initialized = False

    async def initialize(self):
        # Initialize the connection pool
        if self._initialized:
            return
            
        for _ in range(self.max_connections):
            conn = await self._create_connection()
            self._connections.append(conn)
            await self._available.put(conn)
        
        self._initialized = True

    async def _create_connection(self) -> aiosqlite.Connection:
        # Create a properly configured async SQLite connection
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        
        # Apply same performance pragmas as sync version
        await conn.execute("PRAGMA foreign_keys = ON;")
        
        try:
            await conn.execute("PRAGMA journal_mode=WAL;")
            await conn.execute("PRAGMA synchronous=NORMAL;")
            await conn.execute("PRAGMA temp_store=MEMORY;")
            await conn.execute("PRAGMA mmap_size=1073741824;")  # 1 GiB
        except Exception:
            pass  # Ignore pragma errors, use defaults
        
        return conn

    @asynccontextmanager
    async def get_connection(self) -> AsyncContextManager[aiosqlite.Connection]:
        # Get a connection from the pool with proper resource management
        async with self._semaphore:
            if not self._initialized:
                await self.initialize()
            
            conn = await self._available.get()
            try:
                yield conn
            finally:
                await self._available.put(conn)

    async def close_all(self):
        # Close all connections in the pool
        if not self._initialized:
            return
            
        for conn in self._connections:
            await conn.close()
        
        self._connections.clear()
        self._initialized = False

# Global pool management functions
async def get_async_pool(db_path: str = None) -> AsyncConnectionPool:
    # Get or create the global async connection pool
    global _connection_pool
    
    if _connection_pool is None:
        if db_path is None:
            raise ValueError("db_path required for first pool initialization")
        _connection_pool = AsyncConnectionPool(db_path)
        await _connection_pool.initialize()
    
    return _connection_pool

async def close_async_pool():
    # Close the global connection pool
    global _connection_pool
    if _connection_pool:
        await _connection_pool.close_all()
        _connection_pool = None

# Async query functions
async def query_async(pool: AsyncConnectionPool, sql: str, params: tuple = ()) -> list[dict]:
    # Async version of query() function
    async with pool.get_connection() as conn:
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()
        out = []
        # rows should be aiosqlite.Row, not sqlite3.Row
        for row in rows:
            if hasattr(row, 'keys'):
                out.append({k: row[k] for k in row.keys()})
            elif isinstance(row, dict):
                out.append(row)
            else:
                out.append(dict(enumerate(row)))
        return out


def _prepare_excluded(
    excluded: Collection[str] | None,
    ignore: Iterable[str] = (),
) -> list[str]:
    """Normalise a collection of team IDs for exclusion clauses."""

    ignore_set = {str(tid) for tid in ignore if tid}
    seen: set[str] = set()
    prepared: list[str] = []
    for tid in excluded or []:
        if not tid:
            continue
        tid_str = str(tid)
        if tid_str in ignore_set or tid_str in seen:
            continue
        seen.add(tid_str)
        prepared.append(tid_str)
    return prepared


def _build_exclusion_clause(
    excluded: Collection[str] | None,
    alias: str = "m",
    param_prefix: str = "ex",
) -> tuple[str, dict[str, str]]:
    """Return a SQL snippet that excludes matches involving ``excluded`` team IDs."""

    ids = _prepare_excluded(excluded)
    if not ids:
        return "", {}

    placeholders = ", ".join(f":{param_prefix}{idx}" for idx in range(len(ids)))
    clause = (
        f" AND {alias}.team1_id NOT IN ({placeholders})"
        f" AND {alias}.team2_id NOT IN ({placeholders})"
    )
    params = {f"{param_prefix}{idx}": tid for idx, tid in enumerate(ids)}
    return clause, params

async def execute_async(pool: AsyncConnectionPool, sql: str, params: dict = None) -> None:
    # Execute a single SQL statement asynchronously
    async with pool.get_connection() as conn:
        if params:
            await conn.execute(sql, params)
        else:
            await conn.execute(sql)
        await conn.commit()

# Async versions of main query functions from db.py
async def get_teams_in_championship_async(pool: AsyncConnectionPool, division_id: int, season: int = None) -> list[dict]:
    # Async version using team_seasons as source of truth
    if season:
        sql = """
        WITH team_ids AS (
          SELECT DISTINCT team1_id AS team_id FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
          UNION
          SELECT DISTINCT team2_id AS team_id FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
        )
        SELECT x.team_id,
               COALESCE(ts.name, '') AS team_name,
               COALESCE(ts.avatar, ?) AS avatar
        FROM team_ids x
        LEFT JOIN team_seasons ts ON ts.team_id = x.team_id AND ts.season = ?
        ORDER BY team_name COLLATE NOCASE
        """
        DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"
        rows = await query_async(pool, sql, (division_id, division_id, DEFAULT_AVATAR, season))
    else:
        sql = """
        WITH team_ids AS (
          SELECT DISTINCT team1_id AS team_id FROM matches WHERE championship_id=? AND team1_id IS NOT NULL
          UNION
          SELECT DISTINCT team2_id AS team_id FROM matches WHERE championship_id=? AND team2_id IS NOT NULL
        )
        SELECT x.team_id,
               COALESCE((SELECT ts.name FROM team_seasons ts WHERE ts.team_id = x.team_id ORDER BY ts.season DESC LIMIT 1), '') AS team_name,
               COALESCE((SELECT ts.avatar FROM team_seasons ts WHERE ts.team_id = x.team_id ORDER BY ts.season DESC LIMIT 1), ?) AS avatar
        FROM team_ids x
        ORDER BY team_name COLLATE NOCASE
        """
        DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"
        rows = await query_async(pool, sql, (division_id, division_id, DEFAULT_AVATAR))
    return [r for r in rows if r["team_id"]]

async def compute_team_summary_data_async(
    pool: AsyncConnectionPool,
    team_id: str,
    division_id: int,
    excluded_team_ids: Collection[str] | None = None,
) -> dict:
    # Async version of compute_team_summary_data
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    # Fetch only played maps (join maps); all summaries derive from these
    rows = await query_async(
        pool,
        f"""
        SELECT m.match_id, m.team1_id, m.team2_id,
               p.round_index, p.map_name, p.score_team1, p.score_team2, p.winner_team_id
        FROM matches m
        JOIN maps p ON p.match_id = m.match_id
        WHERE m.championship_id=:champ AND (:team = m.team1_id OR :team = m.team2_id)
          AND m.is_forfeit = 0{excl_clause}
    """,
        {"champ": division_id, "team": team_id, **excl_params},
    )

    # Matches played = distinct match_id values from the map rows
    matches_played = len({r["match_id"] for r in rows})
    maps_played = len(rows)
    maps_w = sum(1 for r in rows if r.get("winner_team_id") == team_id)

    # Round difference from the team's perspective
    rd = 0
    for r in rows:
        s1 = r.get("score_team1") or 0
        s2 = r.get("score_team2") or 0
        if r["team1_id"] == team_id:
            rd += (s1 - s2)
        elif r["team2_id"] == team_id:
            rd += (s2 - s1)

    # Aggregates directly from player_stats (no team_stats table)
    agg = await query_async(
        pool,
        f"""
        SELECT
          SUM(ps.kills)           AS kills,
          SUM(ps.deaths)          AS deaths,
          AVG(COALESCE(ps.kr,0))  AS kr,
          AVG(COALESCE(ps.adr,0)) AS adr,
          SUM(COALESCE(ps.utility_damage,0)) AS util
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        WHERE ps.team_id=:team AND m.championship_id=:champ{excl_clause}
    """,
        {"team": team_id, "champ": division_id, **excl_params},
    )
    
    agg_row = agg[0] if agg else {}
    kills = agg_row.get("kills") or 0
    deaths = agg_row.get("deaths") or 0
    kd = (kills / deaths) if deaths else float(kills)

    return {
        "matches_played": matches_played,
        "maps_played": maps_played,
        "w": maps_w,
        "l": maps_played - maps_w,
        "rd": rd,
        "kd": kd,
        "kr": agg_row.get("kr") or 0.0,
        "adr": agg_row.get("adr") or 0.0,
        "util": agg_row.get("util") or 0,
    }

async def compute_player_table_data_async(
    pool: AsyncConnectionPool,
    division_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    # Async version of compute_player_table_data (mirrors db.py implementation).
    async with pool.get_connection() as conn:
        # Column feature flags
        async def has_col(table: str, col: str) -> bool:
            cur = await conn.execute(f"PRAGMA table_info({table})")
            rows = await cur.fetchall()
            return any((r[1] if isinstance(r, tuple) else r["name"]) == col for r in rows)

        HAS_PISTOL = await has_col("player_stats", "pistol_kills")
        HAS_FLASH = (await has_col("player_stats", "enemies_flashed")) and (await has_col("player_stats", "flash_count"))
        HAS_FLASH_SUCC = await has_col("player_stats", "flash_successes")
        HAS_MVPS = await has_col("player_stats", "mvps")

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

    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    sql = f"""
      SELECT
        {", ".join(select_cols)}
      FROM player_stats ps
      JOIN matches m
        ON m.match_id = ps.match_id AND m.is_forfeit = 0
      JOIN maps mp
        ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      LEFT JOIN players pl
        ON pl.player_id = ps.player_id
      WHERE m.championship_id = :champ AND ps.team_id = :team{excl_clause}
      GROUP BY ps.player_id
      ORDER BY kills DESC
    """
    rows = await query_async(pool, sql, {"champ": division_id, "team": team_id, **excl_params})

    out = []
    for r in rows:
        kills = r.get("kills", 0) or 0
        deaths = r.get("deaths", 0) or 0
        assists = r.get("assists", 0) or 0
        kd = (kills / deaths) if deaths else float(kills)
        rounds = r.get("rounds", 0) or 0
        maps_played = r.get("maps_played", 0) or 0
        rpm = (rounds / maps_played) if maps_played else 0.0

        row = {
            "player_id": r.get("player_id"),
            "nickname": r.get("nickname_display", ""),
            "maps_played": maps_played,
            "rounds": rounds,
            "rpm": rpm,
            "kd": kd,
            "adr": r.get("adr", 0.0) or 0.0,
            "kr": r.get("kr", 0.0) or 0.0,
            "kill": kills,
            "death": deaths,
            "assist": assists,
            "mvps": r.get("mvps", 0) or 0,
            "hs_pct": r.get("hs_pct", 0.0) or 0.0,
            "awp_kills": r.get("awp_kills", 0) or 0,
            "k2": r.get("k2", 0) or 0,
            "k3": r.get("k3", 0) or 0,
            "k4": r.get("k4", 0) or 0,
            "k5": r.get("k5", 0) or 0,
            "util": r.get("util", 0) or 0,
            "clutch_kills": r.get("clutch_kills", 0) or 0,
            "c11_att": r.get("c11_att", 0) or 0,
            "c11_win": r.get("c11_win", 0) or 0,
            "c12_att": r.get("c12_att", 0) or 0,
            "c12_win": r.get("c12_win", 0) or 0,
            "entry_count": r.get("entry_count", 0) or 0,
            "entry_win": r.get("entry_win", 0) or 0,
            "damage": r.get("damage", 0) or 0,
        }
        if HAS_PISTOL:
            row["pistol_kills"] = r.get("pistol_kills", 0) or 0
        if HAS_FLASH:
            row["flashed"] = r.get("flashed", 0) or 0
            row["flash_count"] = r.get("flash_count", 0) or 0
        if HAS_FLASH_SUCC:
            row["flash_successes"] = r.get("flash_successes", 0) or 0

        out.append(row)

    return out

# Initialize additional async functions as needed
# This is a foundation - more functions will be added as we convert the rendering pipeline

async def init_async_db(pool: AsyncConnectionPool, schema_path: Path = SCHEMA_PATH) -> None:
    # Async version of init_db
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    schema_sql = schema_path.read_text(encoding="utf-8")
    async with pool.get_connection() as conn:
        await conn.executescript(schema_sql)
        await conn.commit()

async def compute_champ_map_avgs_data_async(
    pool: AsyncConnectionPool,
    division_id: int,
    excluded_team_ids: Collection[str] | None = None,
) -> dict[str, tuple[float, float]]:
    # Async version of compute_champ_map_avgs_data
    excl_clause, excl_params = _build_exclusion_clause(excluded_team_ids)

    sql = f"""
        SELECT
          mp.map_name                                     AS map,
          SUM(ps.kills)                                   AS kills,
          SUM(ps.deaths)                                  AS deaths,
          SUM( (COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) * COALESCE(ps.adr,0) ) AS adr_w,
          SUM(  COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0) )                        AS rw
        FROM player_stats ps
        JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        JOIN matches m  ON m.match_id  = ps.match_id
        WHERE m.championship_id = :champ{excl_clause}
        AND m.is_forfeit = 0
        GROUP BY mp.map_name
    """
    rows = await query_async(pool, sql, {"champ": division_id, **excl_params})
    
    out: dict[str, tuple[float, float]] = {}
    for r in rows:
        kills = r["kills"] or 0
        deaths = r["deaths"] or 0
        kd = (kills / deaths) if deaths else float(kills)
        adr = (r["adr_w"] / r["rw"]) if (r["rw"] or 0) > 0 else 0.0
        out[r["map"]] = (kd, adr)
    return out

async def compute_champ_thresholds_data_async(
    pool: AsyncConnectionPool,
    division_id: int,
    excluded_team_ids: Collection[str] | None = None,
) -> dict:
    # Async version of compute_champ_thresholds_data
    excl_clause, excl_params = _build_exclusion_clause(excluded_team_ids)

    rows = await query_async(pool, f"""
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
      WHERE m.championship_id = :champ{excl_clause}
      GROUP BY ps.player_id
    """, {"champ": division_id, **excl_params})

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

async def get_division_generated_ts_async(pool: AsyncConnectionPool, division_id: int) -> int | None:
    # Async version of get_division_generated_ts
    sql = "SELECT MAX(last_seen_at) AS ts FROM matches WHERE championship_id = ?"
    rows = await query_async(pool, sql, (division_id,))
    return rows[0]["ts"] if rows and rows[0]["ts"] else None

# (duplicate removed) compute_player_table_data_async is implemented above using pure async SQL

async def compute_player_deltas_async(pool: AsyncConnectionPool, championship_id: int, team_id: str) -> dict:
    # NOT IMPLEMENTED: This function must be rewritten to use aiosqlite only.
    raise NotImplementedError("compute_player_deltas_async must be implemented with aiosqlite only.")

async def compute_champ_player_summary_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    min_rounds: int = 20,
    min_flashes: int = 10,
    excluded_team_ids: Collection[str] | None = None,
) -> dict:
    # Division summary + Leaders (async implementation).
    # Main player data query
    excl_clause, excl_params = _build_exclusion_clause(excluded_team_ids)

    rows = await query_async(pool, f"""
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
        SUM(COALESCE(ps.damage,0))          AS total_damage,
        AVG(COALESCE(ps.hs_pct,0))          AS hs_pct,
        SUM(COALESCE(ps.mvps,0))            AS mvps_total,
        SUM(COALESCE(ps.pistol_kills,0))    AS pistol_kills_total,
        SUM(COALESCE(ps.sniper_kills,0))    AS sniper_kills_total,
        SUM(COALESCE(ps.clutch_kills,0))    AS clutch_kills_total,

        -- kierrokset painotuksiin
        SUM(mp.score_team1 + mp.score_team2)                             AS rounds,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.adr,0) )    AS adr_weighted,
        SUM( (mp.score_team1 + mp.score_team2) * COALESCE(ps.kr,0) )     AS kr_weighted

      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
      LEFT JOIN players pl ON pl.player_id = ps.player_id
      LEFT JOIN championships c ON c.championship_id = m.championship_id
      LEFT JOIN team_seasons t ON t.team_id = ps.team_id AND t.season = c.season
      WHERE m.championship_id = :champ{excl_clause}
      GROUP BY ps.player_id
    """, {"champ": championship_id, **excl_params})

    # Aggregates query
    agg = await query_async(pool, f"""
      WITH
      team_ids AS (
        SELECT m.team1_id AS tid FROM matches m WHERE m.championship_id=:champ AND m.team1_id IS NOT NULL{excl_clause}
        UNION
        SELECT m.team2_id AS tid FROM matches m WHERE m.championship_id=:champ AND m.team2_id IS NOT NULL{excl_clause}
      ),
      rounds_cte AS (
        SELECT SUM(mp.score_team1 + mp.score_team2) AS total_rounds
        FROM maps mp JOIN matches m ON m.match_id=mp.match_id
        WHERE m.championship_id=:champ{excl_clause}
      ),
      maps_cte AS (
        SELECT COUNT(*) AS maps_cnt
        FROM maps mp JOIN matches m ON m.match_id=mp.match_id
        WHERE m.championship_id=:champ AND m.is_forfeit = 0{excl_clause}
      )
      SELECT
        (SELECT COUNT(*) FROM team_ids)                       AS teams,
        (SELECT maps_cnt FROM maps_cte)                       AS maps,
        (SELECT total_rounds FROM rounds_cte)                 AS rounds
    """, {"champ": championship_id, **excl_params})
    
    teams = int((agg[0]["teams"] or 0)) if agg else 0
    maps_cnt = int((agg[0]["maps"] or 0)) if agg else 0
    total_rounds = int((agg[0]["rounds"] or 0)) if agg else 0

    # Process player data for distributions and leaders
    kd_vals, kd_w = [], []
    adr_vals, adr_w = [], []
    kr_vals, kr_w = [], []
    surv_vals, surv_w = [], []
    r1_vals, r1_w = [], []

    leaders_pool = []
    totals_kills = []
    totals_deaths = []

    for r in rows:
        nick = r["nick"] or r["player_id"]
        team = r.get("team_name") or "-"
        rounds = r["rounds"] or 0

        kills = r["kills"] or 0
        deaths = r["deaths"] or 0
        assists = r["assists"] or 0

        adr = (r["adr_weighted"] / rounds) if rounds else 0.0
        kr = (kills / rounds) if rounds else 0.0
        kd = (kills / deaths) if deaths else float(kills)

        deaths_pr = (deaths / rounds) if rounds else 0.0
        survival_pct = max(0.0, 1.0 - deaths_pr) * 100.0
        surv_ratio = survival_pct / 100.0
        rating1 = ((kr / 0.679) + (surv_ratio / 0.317) + (adr / 79.9)) / 3.0 if rounds else 0.0

        if rounds > 0:
            kd_vals.append(kd); kd_w.append(rounds)
            adr_vals.append(adr); adr_w.append(rounds)
            kr_vals.append(kr); kr_w.append(rounds)
            surv_vals.append(survival_pct); surv_w.append(rounds)
            r1_vals.append(rating1); r1_w.append(rounds)

        totals_kills.append((nick, team, kills))
        totals_deaths.append((nick, team, deaths))

        if rounds >= min_rounds:
            udpr = (r["util_total"] or 0) / rounds
            flashed_pr = (r["flashed_total"] or 0) / rounds
            assist_pr = assists / rounds

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
                "assist_pr": assist_pr,
                "entry_wr": entry_wr,
                "clutch_wr": clutch_wr,
                "survival_rate": survival_pct,
                "rating1": rating1,
                "assists_total": assists,
                "flashes_total": r["flash_cnt_total"] or 0,
                "total_damage": r["total_damage"] or 0,
                "hs_pct": r["hs_pct"] or 0.0,
                "mvps_total": r["mvps_total"] or 0,
                "pistol_kills_total": r["pistol_kills_total"] or 0,
                "sniper_kills_total": r["sniper_kills_total"] or 0,
                "enemies_flashed_total": r["flashed_total"] or 0,
                "clutch_kills_total": r["clutch_kills_total"] or 0,
            })

    def _wperc(vals, w, p):
        return weighted_percentile(vals, w, p) if vals else 0.0

    kd_p50, kd_p25, kd_p75 = _wperc(kd_vals, kd_w, 50), _wperc(kd_vals, kd_w, 25), _wperc(kd_vals, kd_w, 75)
    adr_p50, adr_p25, adr_p75 = _wperc(adr_vals, adr_w, 50), _wperc(adr_vals, adr_w, 25), _wperc(adr_vals, adr_w, 75)
    kr_p50, kr_p25, kr_p75 = _wperc(kr_vals, kr_w, 50), _wperc(kr_vals, kr_w, 25), _wperc(kr_vals, kr_w, 75)
    surv_p50, surv_p25, surv_p75 = _wperc(surv_vals, surv_w, 50), _wperc(surv_vals, surv_w, 25), _wperc(surv_vals, surv_w, 75)
    r1_p50, r1_p25, r1_p75 = _wperc(r1_vals, r1_w, 50), _wperc(r1_vals, r1_w, 25), _wperc(r1_vals, r1_w, 75)

    def _best(metric):
        if not leaders_pool:
            return ("-", "-", 0.0)
        valid = [x for x in leaders_pool if x[metric] is not None and x[metric] >= 0]
        if not valid:
            return ("-", "-", 0.0)
        b = max(valid, key=lambda x: x[metric])
        return (b["nick"], b["team"], b[metric])

    top_frg_total = max(totals_kills, key=lambda x: x[2]) if totals_kills else ("-", "-", 0)
    most_deaths_total = max(totals_deaths, key=lambda x: x[2]) if totals_deaths else ("-", "-", 0)
    most_assists_total = max([(p["nick"], p["team"], p["assists_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_rounds_played = max([(p["nick"], p["team"], p["rounds"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_flashes_thrown = max([(p["nick"], p["team"], p["flashes_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    best_kd_ratio = max([(p["nick"], p["team"], p["kd"]) for p in leaders_pool if p["rounds"] >= 40], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_total_damage = max([(p["nick"], p["team"], p["total_damage"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_mvps = max([(p["nick"], p["team"], p["mvps_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_pistol_kills = max([(p["nick"], p["team"], p["pistol_kills_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_sniper_kills = max([(p["nick"], p["team"], p["sniper_kills_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_enemies_flashed = max([(p["nick"], p["team"], p["enemies_flashed_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)
    most_clutch_kills = max([(p["nick"], p["team"], p["clutch_kills_total"]) for p in leaders_pool], key=lambda x: x[2]) if leaders_pool else ("-", "-", 0)

    leaders = {
        "top_frg_total": top_frg_total,
        "most_deaths_total": most_deaths_total,
        "most_assists_total": most_assists_total,
        "most_rounds_played": most_rounds_played,
        "most_flashes_thrown": most_flashes_thrown,
        "best_kd_ratio": best_kd_ratio,
        "most_total_damage": most_total_damage,
        "most_mvps": most_mvps,
        "most_pistol_kills": most_pistol_kills,
        "most_sniper_kills": most_sniper_kills,
        "most_enemies_flashed": most_enemies_flashed,
        "most_clutch_kills": most_clutch_kills,
        "adr": _best("adr"),
        "kd": _best("kd"),
        "kr": _best("kr"),
        "udpr": _best("udpr"),
        "enemies_per_flash": _best("enemies_per_flash"),
        "assist_pr": _best("assist_pr"),
        "entry_wr": _best("entry_wr"),
        "clutch_wr": _best("clutch_wr"),
        "survival_rate": _best("survival_rate"),
        "rating1": _best("rating1"),
        "hs_pct": _best("hs_pct"),
    }

    return {
        "players": len(rows),
        "teams": teams,
        "maps": maps_cnt,
        "rounds": total_rounds,
        "kd_p50": kd_p50, "kd_p25": kd_p25, "kd_p75": kd_p75,
        "adr_p50": adr_p50, "adr_p25": adr_p25, "adr_p75": adr_p75,
        "kr_p50": kr_p50, "kr_p25": kr_p25, "kr_p75": kr_p75,
        "surv_p50": surv_p50, "surv_p25": surv_p25, "surv_p75": surv_p75,
        "r1_p50": r1_p50, "r1_p25": r1_p25, "r1_p75": r1_p75,
        "leaders": leaders,
    }

async def compute_champ_map_summary_data_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    excluded_team_ids: Collection[str] | None = None,
) -> dict:
    # Get map summary stats for division: all played and banned maps.
    # All played maps
    excl_clause, excl_params = _build_exclusion_clause(excluded_team_ids)

    played_sql = f"""
    SELECT map_name, COUNT(*) as count
    FROM maps mp
    JOIN matches m ON m.match_id = mp.match_id
    WHERE m.championship_id = :champ AND mp.map_name IS NOT NULL{excl_clause}
    AND m.is_forfeit = 0
    GROUP BY map_name
    ORDER BY count DESC, mp.map_name ASC
    """
    played_rows = await query_async(pool, played_sql, {"champ": championship_id, **excl_params})
    top_played = [(row["map_name"], row["count"]) for row in played_rows]

    # All banned maps (using map_votes table)
    banned_sql = f"""
    SELECT v.map_name AS map_name, COUNT(*) AS count
    FROM map_votes v
    JOIN matches m ON m.match_id = v.match_id
    WHERE m.championship_id = :champ{excl_clause}
      AND v.status = 'drop'
      AND v.map_name IS NOT NULL
    GROUP BY v.map_name
    ORDER BY count DESC, v.map_name ASC
    """
    banned_rows = await query_async(pool, banned_sql, {"champ": championship_id, **excl_params})
    top_banned = [(row["map_name"], row["count"]) for row in banned_rows]
    
    return {
        "top_played": top_played,
        "top_banned": top_banned
    }

def normalize_map_id(name: str) -> str:
    # Return canonical map_id like 'de_ancient' from variants like 'Ancient', 'de_ancient', 'ancient'.
    if not name:
        return ""
    s = str(name).strip().lower().replace(" ", "")
    if not s.startswith("de_"):
        s = "de_" + s
    return s

async def get_map_art_async(pool: AsyncConnectionPool, map_name_or_id: str) -> dict | None:
    # Return {'map_id','pretty_name','image_sm','image_lg'} for given map name/id, or None.
    mid = normalize_map_id(map_name_or_id)
    rows = await query_async(pool, "SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog WHERE map_id=?", (mid,))
    if not rows:
        return None
    row = rows[0]
    return {"map_id": row["map_id"], "pretty_name": row["pretty_name"], "image_sm": row["image_sm"], "image_lg": row["image_lg"]}

async def map_pretty_name_async(pool: AsyncConnectionPool, raw: str) -> str:
    # Return the prettified name from maps_catalog or a solid fallback.
    art = await get_map_art_async(pool, raw)
    if art and (art.get("pretty_name")):
        return art["pretty_name"]
    if not raw:
        return "—"
    slug = normalize_map_id(raw).replace("de_", "").replace("_", " ")
    return slug.title()

async def compute_map_stats_table_data_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    # Async version of compute_map_stats_table_data
    # Get season map pool
    pool_rows = await query_async(pool, """
        SELECT DISTINCT mp.map_name AS map_id
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ?
            AND mp.map_name IS NOT NULL AND mp.map_name <> ''
            AND m.is_forfeit = 0
    """, (championship_id,))
    
    if pool_rows:
        all_maps = [r["map_id"] for r in pool_rows]
    else:
        all_maps = ["de_nuke","de_inferno","de_mirage","de_overpass","de_dust2","de_ancient","de_train","de_anubis"]

    values_sql = ", ".join([f"('{m}')" for m in all_maps])

    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    sql = f"""
        WITH allmaps(map) AS (
            VALUES {values_sql}
        ),
        my_matches AS (
            SELECT m.*
            FROM matches m
            WHERE m.championship_id = :champ
              AND (:team = m.team1_id OR :team = m.team2_id)
              {excl_clause}
        ),
        team_maps AS (
            -- Played maps + W/L and pick provenance
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
        -- Team drops indexed (1st/2nd ban)
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
        -- Opponent drops in matches where :team played
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

    rows = await query_async(pool, sql, {"champ": championship_id, "team": team_id, **excl_params})

    # Add pretty names - simplified version for now
    out = []
    for r in rows:
        mid = r.get("map")
        r["map_pretty"] = (mid or "").replace("de_", "").title() if mid else ""
        out.append(r)
    return out

# Helper constants and functions for async database operations
_TS_EXPR = "COALESCE(m.finished_at, m.started_at, m.scheduled_at, m.configured_at, m.last_seen_at, 0)"

async def _get_team_last_prev_ts_async(
    pool: AsyncConnectionPool,
    division_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> tuple[int | None, int | None]:
    # Async version of _get_team_last_prev_ts
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    rows = await query_async(
        pool,
        f"""
        SELECT DISTINCT { _TS_EXPR } AS ts
        FROM matches m
        WHERE m.championship_id=:champ AND (:team = m.team1_id OR :team = m.team2_id)
          AND EXISTS (SELECT 1 FROM maps mp WHERE mp.match_id = m.match_id){excl_clause}
        ORDER BY ts ASC
    """,
        {"champ": division_id, "team": team_id, **excl_params},
    )
    if not rows:
        return (None, None)
    curr_ts = rows[-1]["ts"]
    prev_ts = rows[-2]["ts"] if len(rows) >= 2 else None
    return (curr_ts, prev_ts)

async def compute_map_stats_table_data_until_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    team_id: str,
    cutoff_ts: int,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    # Async version of compute_map_stats_table_data_until
    # Get season map pool
    pool_rows = await query_async(pool, """
        SELECT DISTINCT mp.map_name AS map_id
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ?
            AND mp.map_name IS NOT NULL AND mp.map_name <> ''
            AND m.is_forfeit = 0
    """, (championship_id,))
    
    if pool_rows:
        all_maps = [r["map_id"] for r in pool_rows]
    else:
        all_maps = ["de_nuke","de_inferno","de_mirage","de_overpass","de_dust2","de_ancient","de_train","de_anubis"]

    values_sql = ", ".join([f"('{m}')" for m in all_maps])

    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    sql = f"""
        WITH allmaps(map) AS (
            VALUES {values_sql}
        ),
        my_matches AS (
            SELECT m.*
            FROM matches m
            WHERE m.championship_id = :champ
              AND (:team = m.team1_id OR :team = m.team2_id)
              AND { _TS_EXPR } <= :cutoff
              {excl_clause}
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
            JOIN maps mp
              ON mp.match_id = m.match_id
             AND mp.round_index IS NOT NULL
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
            WHERE LOWER(v.status) = 'drop'
              AND v.selected_by_team_id = :team
        ),
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

    rows = await query_async(
        pool,
        sql,
        {"champ": championship_id, "team": team_id, "cutoff": cutoff_ts, **excl_params},
    )
    out = [dict(r) for r in rows]
    return out

async def compute_map_stats_with_delta_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> dict[str, dict]:
    # Async version of compute_map_stats_with_delta
    # Map-delta = (agg <= curr_ts) - (agg <= curr_ts-1)
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    curr_ts, _ = await _get_team_last_prev_ts_async(pool, championship_id, team_id, excluded)
    if curr_ts is None:
        return {}

    prev_cutoff = max(0, int(curr_ts) - 1)

    curr = await compute_map_stats_table_data_until_async(
        pool, championship_id, team_id, curr_ts, excluded
    )
    prev = await compute_map_stats_table_data_until_async(
        pool, championship_id, team_id, prev_cutoff, excluded
    )

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

async def _player_agg_until_async(
    pool: AsyncConnectionPool,
    division_id: int,
    team_id: str,
    player_id: str,
    cutoff: int | None,
    excluded_team_ids: Collection[str] | None = None,
) -> dict:
    # Async version of _player_agg_until
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

    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    rows = await query_async(pool, f"""
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
        JOIN matches m ON m.match_id = ps.match_id AND m.is_forfeit = 0
        JOIN maps    mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE m.championship_id=:champ AND ps.team_id=:team AND ps.player_id=:player
          AND { _TS_EXPR } <= :cutoff{excl_clause}
    """,
        {"champ": division_id, "team": team_id, "player": player_id, "cutoff": cutoff, **excl_params},
    )
    
    if not rows:
        return await _player_agg_until_async(pool, division_id, team_id, player_id, None, excluded_team_ids)
    
    row = rows[0]
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

async def compute_player_deltas_async(
    pool: AsyncConnectionPool,
    division_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> dict[str, dict]:
    # Async version of compute_player_deltas
    # Delta = (agg <= curr_ts) - (agg <= curr_ts-1)
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    curr_ts, _ = await _get_team_last_prev_ts_async(pool, division_id, team_id, excluded)
    if curr_ts is None:
        return {}

    prev_cutoff = max(0, int(curr_ts) - 1)

    # Kauden pelaajat (joilta on havaittu statsia)
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    pid_rows = await query_async(pool, f"""
      SELECT DISTINCT ps.player_id
      FROM player_stats ps
      JOIN matches m ON m.match_id = ps.match_id
      WHERE m.championship_id=:champ AND ps.team_id=:team{excl_clause}
    """,
        {"champ": division_id, "team": team_id, **excl_params},
    )
    pids = [r["player_id"] for r in pid_rows]

    out: dict[str, dict] = {}
    for pid in pids:
        prev = await _player_agg_until_async(pool, division_id, team_id, pid, prev_cutoff, excluded)
        curr = await _player_agg_until_async(pool, division_id, team_id, pid, curr_ts, excluded)

        # If nothing existed before the latest match, expose prev=None and delta=None (UI shows "(no prev)")
        if prev["maps_played"] == 0 and prev["rounds"] == 0 and prev["kills"] == 0 and prev["deaths"] == 0 and prev["assists"] == 0:
            out[pid] = {"curr": curr, "prev": None, "delta": None}
        else:
            delta = {}
            for k in curr.keys():
                delta[k] = (curr[k] - prev[k]) if isinstance(curr[k], (int, float)) else None
            out[pid] = {"curr": curr, "prev": prev, "delta": delta}
    return out

# =============================================
# Async Team Rendering Helpers
# =============================================

async def render_team_card_async(
    pool: AsyncConnectionPool,
    div: dict,
    team: dict,
    team_index: int,
    teams: list,
    thresholds: dict,
    div_avgs: dict,
    banned_team_ids: Collection[str] | None = None,
) -> list[str]:
    # Render a complete team card with players table, maps table, and matches section.
    # Returns list of HTML lines for the team card.
    team_id = team["team_id"]
    team_name = team.get("display_name") or team.get("team_name") or team["team_id"]
    
    # Team avatar and logo
    team_avatar = next((t.get("avatar") for t in teams if t["team_id"] == team_id), None)
    logo = f'<img class="logo team-logo" src="{team_avatar}" alt="">' if team_avatar else ''
    
    html = []
    
    # Load all team data concurrently
    excluded = [] if team.get("is_banned") else _prepare_excluded(banned_team_ids, ignore=[team_id])

    players_task = compute_player_table_data_async(
        pool, div["championship_id"], team_id, excluded
    )
    player_deltas_task = compute_player_deltas_async(
        pool, div["championship_id"], team_id, excluded
    )
    team_summary_task = compute_team_summary_data_async(
        pool, team_id, div["championship_id"], excluded
    )
    maps_task = compute_map_stats_table_data_async(
        pool, div["championship_id"], team_id, excluded
    )
    map_deltas_task = compute_map_stats_with_delta_async(
        pool, div["championship_id"], team_id, excluded
    )
    
    players, player_deltas, team_summary, maps, map_deltas = await asyncio.gather(
        players_task, player_deltas_task, team_summary_task, maps_task, map_deltas_task
    )
    
    # Helper functions for delta calculations
    def _pd(pid: str) -> dict | None:
        return player_deltas.get(pid)

    def _dval(d: dict | None, key: str):
        if not d or not d.get("delta"):
            return None, None
        prev = None if d.get("prev") is None else d["prev"].get(key)
        return d["delta"].get(key), prev

    def _signed(x, prec=2):
        if x is None: return ""
        s = "+" if x >= 0 else ""
        fmt = f"{{:{'.'+str(prec)+'f' if prec else ''}}}"
        return s + fmt.format(x)

    def _arrow(val: float | int | None) -> str:
    # Small arrow for change direction (empty if no delta).
        if val is None:
            return ""
        if val > 0:
            return " ▲"
        if val < 0:
            return " ▼"
        return ""
    
    # Calculate team stats from players if needed
    has_flash = any(("flashed" in p and "flash_count" in p) for p in players)
    has_pistol = any(("pistol_kills" in p) for p in players)
    
    # Calculate fallback stats from players
    tot_k = sum(p.get("kill", 0) for p in players)
    tot_d = sum(p.get("death", 0) for p in players)
    tot_r = sum(p.get("rounds", 0) for p in players)
    tot_util = sum(p.get("util", 0) for p in players)
    
    # ADR weighted by rounds
    adr_weighted_sum = sum((p.get("adr", 0.0) * p.get("rounds", 0)) for p in players)
    fallback_stats = {
        "kd": (tot_k / tot_d) if tot_d else float(tot_k),
        "kr": (tot_k / tot_r) if tot_r else 0.0,
        "adr": (adr_weighted_sum / tot_r) if tot_r else 0.0,
        "util": float(tot_util),
    }
    
    # Patch missing stats with calculated values
    for k in ("kd", "kr", "adr", "util"):
        if (k not in team_summary) or (team_summary[k] in (None, 0) and tot_r > 0):
            team_summary[k] = fallback_stats[k]
    
    # Generate team card HTML
    html.extend(await _render_team_card_header_async(team_id, team_name, logo, team.get("is_banned")))
    html.extend(await _render_team_players_section_async(
        pool, div, team, players, player_deltas, team_summary,
        team_index, thresholds, _pd, _dval, _signed, _arrow, has_flash, has_pistol
    ))
    html.extend(await _render_team_maps_section_async(
        pool, div, team, maps, map_deltas, div_avgs, team_index, 
        _signed, _arrow
    ))
    # Render team matches section
    html.extend(await _render_team_matches_section_async(pool, div, team, excluded))
    html.extend(_render_team_card_footer())
    
    return html

async def _render_team_card_header_async(team_id: str, team_name: str, logo: str, is_banned: bool = False) -> list[str]:
    # Render team card header with collapsible details.
    from html import escape
    classes = ["card", "team-section"]
    if is_banned:
        classes.append("is-banned")
    class_attr = " ".join(classes)
    return [
        f'''<details class="{class_attr}" id="team-{team_id}" open>''',
        f'  <summary>',
        f'    <div class="card-head team">',
        f'      <span class="chev">›</span>',
        f'      <span class="title">{logo}{escape(team_name)}</span>',
        f'      <span class="hint">Click to expand</span>',
        f'    </div>',
        f'  </summary>',
        f'  <div class="card-content">',
    ]

def _render_team_card_footer() -> list[str]:
    # Render team card footer closing tags.
    return [
        '  </div>',  # /card-content
        '</details>'  # /team section
    ]


async def _render_team_players_section_async(pool: AsyncConnectionPool, div: dict, team: dict,
                                           players: list, player_deltas: dict, team_summary: dict,
                                           team_index: int, thresholds: dict,
                                           _pd, _dval, _signed, _arrow, has_flash: bool, has_pistol: bool) -> list[str]:
    # Render the players section with basic and advanced tables.
    from html import escape
    
    html = []
    
    # Calculate derived metrics for players
    for p in players:
        # Win rates (%)
        c11_att = p.get("c11_att", 0) or 0
        c11_win = p.get("c11_win", 0) or 0
        p["c11_wr"] = (c11_win / c11_att * 100.0) if c11_att else 0.0

        c12_att = p.get("c12_att", 0) or 0
        c12_win = p.get("c12_win", 0) or 0
        p["c12_wr"] = (c12_win / c12_att * 100.0) if c12_att else 0.0

        entry_count = p.get("entry_count", 0) or 0
        entry_win = p.get("entry_win", 0) or 0
        p["entry_wr"] = (entry_win / entry_count * 100.0) if entry_count else 0.0

        # Utility damage per round
        rounds = p.get("rounds", 0) or 0
        util = p.get("util", 0) or 0
        p["udpr"] = (util / rounds) if rounds else 0.0

        # Impact-proxy: 2*KR + 0.42*AR - 0.41*DR
        kr = p.get("kr", 0.0) or 0.0
        ar = (p.get("assist", 0) or 0) / rounds if rounds else 0.0
        dr = (p.get("death", 0) or 0) / rounds if rounds else 0.0
        p["impact"] = 2.0*kr + 0.42*ar - 0.41*dr

        # Survival% and Rating1 (HLTV 1.0 approximation)
        death = p.get("death", 0) or 0
        adr = p.get("adr", 0.0) or 0.0
        surv_ratio = 1.0 - ((death / rounds) if rounds else 0.0)
        surv_ratio = max(0.0, min(1.0, surv_ratio))  # clamp 0..1
        p["survival_pct"] = surv_ratio * 100.0
        p["rating1"] = ((kr / 0.679) + (surv_ratio / 0.317) + (adr / 79.9)) / 3.0 if rounds else 0.0

        # Enemies per flash (if data available)
        fc = p.get("flash_count", 0) or 0
        if has_flash:
            p["enemies_per_flash"] = (p.get("flashed", 0) or 0) / fc if fc else 0.0
        else:
            p["enemies_per_flash"] = None

        # Flash success percentage
        fsu = p.get("flash_successes", p.get("flash_succ", 0)) or 0
        p["flash_succ_pct"] = (100.0 * fsu / fc) if fc else 0.0
    
    # Player chips for summary stats
    player_chips = [
        f'<span class="chip">Team KD {team_summary["kd"]:.2f}</span>',
        f'<span class="chip">Team KR {team_summary["kr"]:.2f}</span>',
        f'<span class="chip">Team ADR {team_summary["adr"]:.1f}</span>',
        f'<span class="chip">Team Util {int(team_summary["util"])}</span>',
    ]
    
    tab_root_id = f"tabs-{team['team_id'][:8]}"
    
    # Players section header
    html.extend(_render_card_header('players', 'Pelaajat'))
    html.append('<div class="chips">' + " ".join(player_chips) + '</div>')
    html.append('<div class="muted">Joillakin arvoilla on tooltip missä lisää tietoa.<br></div>')
    html.append(f'''
      <div id="{tab_root_id}" class="tabs">
        <div class="tab-nav">
          <button class="tab-btn active" data-target="basic"
                  onclick="switchTab('{tab_root_id}','basic')">Basic</button>
          <button class="tab-btn" data-target="advanced"
                  onclick="switchTab('{tab_root_id}','advanced')">Advanced</button>
        </div>
    ''')
    
    # Basic table
    html.extend(await _render_players_basic_table_async(
        players, team_index, thresholds, _pd, _dval, _signed, _arrow
    ))
    
    # Advanced table  
    html.extend(await _render_players_advanced_table_async(
        players, team_index, thresholds, _pd, _dval, _signed, _arrow, has_flash
    ))
    
    html.append("</div>")  # /tabs
    html.extend(_render_card_footer())  # /players-section
    
    return html

def _render_card_header(section_type: str, title: str) -> list[str]:
    # Helper function to generate card header HTML lines
    return [
        f'<details class="card {section_type}-section" open>',
        '  <summary>',
        f'    <div class="card-head">',
        f'      <span class="chev">›</span>',
        f'      <span class="title">{title}</span>',
        f'      <span class="hint">Click to expand</span>',
        f'    </div>',
        f'  </summary>',
        f'  <div class="card-content">',
    ]

def _render_card_footer() -> list[str]:
    # Helper function to generate card footer HTML lines
    return [
        '  </div>',  # /card-content
        '</details>'  # /section
    ]

# Note: These functions would need the actual table rendering implementations
# For now, they're placeholders that delegate to sync versions
async def _render_players_basic_table_async(players, team_index, thresholds, _pd, _dval, _signed, _arrow) -> list[str]:
    # Render basic players table with core stats and delta information.
    from html import escape
    
    html = []
    tid_basic = f"players-basic-{team_index}"
    
    html.append(f'<div class="tab-panel active" data-tab="basic">')
    html.append(f'<table id="{tid_basic}" data-sort-col="3" data-sort-dir="desc">')
    
    # Table headers
    html.append(f'''<thead><tr>
      <th data-sortable onclick="sortTable('{tid_basic}',0,false)"  title="Player nickname (Faceit)">Nickname</th>
      <th data-sortable onclick="sortTable('{tid_basic}',1,true)"   title="Maps played">Maps</th>
      <th data-sortable onclick="sortTable('{tid_basic}',2,true)"   title="Total rounds played">Rounds</th>
      <th data-sortable onclick="sortTable('{tid_basic}',3,true)"   title="Kills divided by deaths">KD</th>
      <th data-sortable onclick="sortTable('{tid_basic}',4,true)"   title="Average damage per round">ADR</th>
      <th data-sortable onclick="sortTable('{tid_basic}',5,true)"   title="Kills per round">KR</th>
      <th data-sortable onclick="sortTable('{tid_basic}',6,true)"   title="Total damage dealt">Damage</th>
      <th data-sortable onclick="sortTable('{tid_basic}',7,true)"   title="Total kills">Kills</th>
      <th data-sortable onclick="sortTable('{tid_basic}',8,true)"   title="Total deaths">Deaths</th>
      <th data-sortable onclick="sortTable('{tid_basic}',9,true)"   title="Total assists">Assists</th>
      <th data-sortable onclick="sortTable('{tid_basic}',10,true)"  title="Headshot percentage">HS%</th>
      <th data-sortable onclick="sortTable('{tid_basic}',11,true)"  title="Rounds with exactly 2 kills (multi-kill 2K)">2K</th>
      <th data-sortable onclick="sortTable('{tid_basic}',12,true)"  title="Rounds with exactly 3 kills (multi-kill 3K)">3K</th>
      <th data-sortable onclick="sortTable('{tid_basic}',13,true)"  title="Rounds with exactly 4 kills (multi-kill 4K)">4K</th>
      <th data-sortable onclick="sortTable('{tid_basic}',14,true)"  title="Rounds with 5 kills (ace)">ACE</th>
      <th data-sortable onclick="sortTable('{tid_basic}',15,true)"  title="Match MVP awards">MVPs</th>
      </tr></thead><tbody>''')
    
    # Table rows
    for p in players:
        deltas = _pd(p["player_id"])
        d_kd,  prev_kd  = _dval(deltas, "kd")
        d_adr, prev_adr = _dval(deltas, "adr")
        d_kr,  prev_kr  = _dval(deltas, "kr")
        d_dmg, prev_dmg = _dval(deltas, "damage")
        d_k,   prev_k   = _dval(deltas, "kills")
        d_d,   prev_d   = _dval(deltas, "deaths")
        d_a,   prev_a   = _dval(deltas, "assists")
        d_hs,  prev_hs  = _dval(deltas, "hs_pct")
        d_k2,  prev_k2  = _dval(deltas, "k2")
        d_k3,  prev_k3  = _dval(deltas, "k3")
        d_k4,  prev_k4  = _dval(deltas, "k4")
        d_k5,  prev_k5  = _dval(deltas, "k5")
        d_mv,  prev_mv  = _dval(deltas, "mvps")

        html.append(f'''<tr>
          <td>{p["nickname"]}</td>
          <td title="Δ vs prev: {_signed(deltas['delta']['maps_played'] if deltas and deltas.get('delta') else 0, 0)} (prev {int(deltas['prev']['maps_played']) if deltas and deltas.get('prev') else 0})">{p["maps_played"]}</td>
          <td title="Rounds/Map: {p['rpm']:.1f} — Δ rounds: {_signed(d_d if d_d is not None else 0, 0)} (prev {int(prev_d) if prev_d is not None else 0})">{p["rounds"]}</td>
          <td title="Δ vs prev: {_signed(d_kd)} (prev {(prev_kd if prev_kd is not None else 0.0):.2f})">{p["kd"]:.2f}{_arrow(d_kd)}</td>
          <td title="Δ vs prev: {_signed(d_adr,1)} (prev {(prev_adr if prev_adr is not None else 0.0):.1f})">{p["adr"]:.1f}{_arrow(d_adr)}</td>
          <td title="Δ vs prev: {_signed(d_kr)} (prev {(prev_kr if prev_kr is not None else 0.0):.2f})">{p["kr"]:.2f}{_arrow(d_kr)}</td>
          <td title="Δ vs prev: {_signed(d_dmg,0)} (prev {int(prev_dmg) if prev_dmg is not None else 0})">{p["damage"]}{_arrow(d_dmg)}</td>
          <td title="Δ vs prev: {_signed(d_k,0)} (prev {int(prev_k) if prev_k is not None else 0})">{p["kill"]}{_arrow(d_k)}</td>
          <td title="Δ vs prev: {_signed(d_d,0)} (prev {int(prev_d) if prev_d is not None else 0})">{p["death"]}{_arrow(d_d)}</td>
          <td title="Δ vs prev: {_signed(d_a,0)} (prev {int(prev_a) if prev_a is not None else 0})">{p["assist"]}{_arrow(d_a)}</td>
          <td title="Δ vs prev: {_signed(d_hs,1)} (prev {(prev_hs if prev_hs is not None else 0.0):.1f})">{p["hs_pct"]:.1f}{_arrow(d_hs)}</td>
          <td title="Δ vs prev: {_signed(d_k2,0)} (prev {int(prev_k2) if prev_k2 is not None else 0})">{p["k2"]}{_arrow(d_k2)}</td>
          <td title="Δ vs prev: {_signed(d_k3,0)} (prev {int(prev_k3) if prev_k3 is not None else 0})">{p["k3"]}{_arrow(d_k3)}</td>
          <td title="Δ vs prev: {_signed(d_k4,0)} (prev {int(prev_k4) if prev_k4 is not None else 0})">{p["k4"]}{_arrow(d_k4)}</td>
          <td title="Δ vs prev: {_signed(d_k5,0)} (prev {int(prev_k5) if prev_k5 is not None else 0})">{p["k5"]}{_arrow(d_k5)}</td>
          <td title="Δ vs prev: {_signed(d_mv,0)} (prev {int(prev_mv) if prev_mv is not None else 0})">{p["mvps"]}{_arrow(d_mv)}</td>
        </tr>''')

    html.append("</tbody></table>")
    
    # JavaScript for table processing
    html.append(f'''
    <script>
    window.addEventListener('DOMContentLoaded', function(){{
        postProcessTable('{tid_basic}', {{
          color: [
            {{col:3, p:[{thresholds['kd'][0]:.4f}, {thresholds['kd'][1]:.4f}, {thresholds['kd'][2]:.4f}] }},
            {{col:4, p:[{thresholds['adr'][0]:.4f}, {thresholds['adr'][1]:.4f}, {thresholds['adr'][2]:.4f}] }},
            {{col:5, p:[{thresholds['kr'][0]:.4f}, {thresholds['kr'][1]:.4f}, {thresholds['kr'][2]:.4f}]  }},
            {{col:10, p:[{thresholds['hs_pct'][0]:.4f}, {thresholds['hs_pct'][1]:.4f}, {thresholds['hs_pct'][2]:.4f}]  }}
          ],
          defaultSort: {{col:0, dir:'asc'}},
        }});
    }});
    </script>
    ''')
    
    html.append("</div>")  # /tab-panel basic
    return html

async def _render_players_advanced_table_async(players, team_index, thresholds, _pd, _dval, _signed, _arrow, has_flash) -> list[str]:
    # Render advanced players table with clutch, utility, and flash stats.
    from html import escape
    
    # Import tooltip constant needed for this table
    TOOLTIP_RATING1 = (
        "Rating1 ≈ HLTV 1.0:\n"
        "  ( KR/0.679 + SURV/0.317 + ADR/79.9 ) / 3\n"
        "Missä:\n"
        "  KR   = Kills per Round (kills / rounds)\n"
        "  SURV = Survived per Round = 1 - (deaths / rounds)\n"
        "  ADR  = Average Damage per Round\n"
        "Baselinet on kalibroitu niin, että ~1.00 ≈ sarjan keskitason suoritus."
    )
    
    def esc_title(s: str) -> str:
        return (s or "").replace("'", "").replace("\n", "&#10;")
    
    html = []
    tid_adv = f"players-adv-{team_index}"
    
    html.append(f'<div class="tab-panel" data-tab="advanced">')
    html.append(f'<table id="{tid_adv}" data-sort-col="7" data-sort-dir="desc">')

    # Advanced table headers
    html.append("<thead><tr>")
    col_idx = 0
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},false)\">Nickname</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Clutch-fragit 1vX-tilanteissa'>Clutch Kills</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='1v1 clutch winrate (W–L, %)'>1v1 WR</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='1v2 clutch winrate (W–L, %)'>1v2 WR</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Entry duels winrate (W–L, %)'>Entry WR</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Total utility damage'>Util dmg</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Utility damage per round'>UDPR</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Percentage of rounds survived'>Survival %</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='{esc_title(TOOLTIP_RATING1)}'>Rating1</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Successful flashes out of all thrown (successes / throws). Cell shows S/T and % as a bar.'>Flash Succ (S/T)</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Total enemies blinded by the player&#39;s flashes'>Flashed</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Enemies blinded per flash thrown'>Enem/Flash</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of pistol kills'>Pistol Kills</th>"); col_idx += 1
    html.append(f"<th data-sortable onclick=\"sortTable('{tid_adv}',{col_idx},true)\" title='Number of sniper kills'>Sniper Kills</th>"); col_idx += 1
    html.append("</tr></thead><tbody>")

    # Advanced table rows - simplified version focusing on core logic
    for p in players:
        deltas = _pd(p["player_id"])
        # Extract deltas for advanced stats
        d_ck,  prev_ck  = _dval(deltas, "clutch_kills")
        d_c11a, prev_c11a = _dval(deltas, "c11_att")
        d_c11w, prev_c11w = _dval(deltas, "c11_win")
        d_c12a, prev_c12a = _dval(deltas, "c12_att")
        d_c12w, prev_c12w = _dval(deltas, "c12_win")
        d_ea,   prev_ea   = _dval(deltas, "entry_count")
        d_ew,   prev_ew   = _dval(deltas, "entry_win")
        d_util, prev_util = _dval(deltas, "util")
        d_udpr, prev_udpr = _dval(deltas, "udpr")
        d_fsucc, prev_fsucc = _dval(deltas, "flash_successes")
        d_fcnt,  prev_fcnt  = _dval(deltas, "flash_count")
        d_flashed, prev_flashed = _dval(deltas, "flashed")
        d_pistol, prev_pistol = _dval(deltas, "pistol_kills")
        d_awp,    prev_awp    = _dval(deltas, "awp_kills")

        html.append("<tr>")
        html.append(f"<td>{p['nickname']}</td>")
        html.append(f"<td title='Δ vs prev: {_signed(d_ck,0)} (prev {int(prev_ck) if prev_ck is not None else 0})'>{p['clutch_kills']}{_arrow(d_ck)}</td>")

        # Win rate cells with delta calculations
        c11_wr_prev = (100.0 * (prev_c11w or 0) / (prev_c11a or 0)) if (prev_c11a or 0) > 0 else 0.0
        c11_wr_delta = p['c11_wr'] - c11_wr_prev
        html.append(f"<td class='wr' data-zero='show' data-g='{p['c11_att']}' data-w='{p['c11_win']}' data-pct='{p['c11_wr']:.1f}' title='Attempts: {p['c11_att']} (Δ {_signed(d_c11a,0)}), Wins: {p['c11_win']} (Δ {_signed(d_c11w,0)}), Δ WR: {_signed(c11_wr_delta,1)} pp'></td>")

        c12_wr_prev = (100.0 * (prev_c12w or 0) / (prev_c12a or 0)) if (prev_c12a or 0) > 0 else 0.0
        c12_wr_delta = p['c12_wr'] - c12_wr_prev
        html.append(f"<td class='wr' data-zero='show' data-g='{p['c12_att']}' data-w='{p['c12_win']}' data-pct='{p['c12_wr']:.1f}' title='Attempts: {p['c12_att']} (Δ {_signed(d_c12a,0)}), Wins: {p['c12_win']} (Δ {_signed(d_c12w,0)}), Δ WR: {_signed(c12_wr_delta,1)} pp'></td>")

        entry_wr_prev = (100.0 * (prev_ew or 0) / (prev_ea or 0)) if (prev_ea or 0) > 0 else 0.0
        entry_wr_delta = p['entry_wr'] - entry_wr_prev
        html.append(f"<td class='wr' data-zero='show' data-g='{p['entry_count']}' data-w='{p['entry_win']}' data-pct='{p['entry_wr']:.1f}' title='Attempts: {p['entry_count']} (Δ {_signed(d_ea,0)}), Wins: {p['entry_win']} (Δ {_signed(d_ew,0)}), Δ WR: {_signed(entry_wr_delta,1)} pp'></td>")

        html.append(f"<td title='Δ vs prev: {_signed(d_util,0)} (prev {int(prev_util) if prev_util is not None else 0})'>{int(p['util'])}{_arrow(d_util)}</td>")
        html.append(f"<td title='Δ vs prev: {_signed(d_udpr)} (prev {(prev_udpr if prev_udpr is not None else 0.0):.2f})'>{p['udpr']:.2f}{_arrow(d_udpr)}</td>")
        html.append(f"<td>{p['survival_pct']:.0f}</td>")
        html.append(f"<td title='{esc_title(TOOLTIP_RATING1)}'>{p['rating1']:.2f}</td>")

        # Flash stats
        _s = int(p.get("flash_successes", p.get("flash_succ", 0)) or 0)
        _c = int(p.get("flash_count", 0) or 0)
        _pct = (100.0 * _s / _c) if _c else 0.0
        html.append(f"<td class='wr' data-mode='ratio' data-zero='show' data-g='{_c}' data-w='{_s}' data-pct='{_pct:.1f}' title='Successes: {_s} (Δ {_signed(d_fsucc,0)}), Throws: {_c} (Δ {_signed(d_fcnt,0)})'></td>")

        html.append(f"<td title='Δ vs prev: {_signed(d_flashed,0)} (prev {int(prev_flashed) if prev_flashed is not None else 0})'>{p.get('flashed', 0)}{_arrow(d_flashed)}</td>")

        # Enemies per flash efficiency
        _curr_eff = p.get("enemies_per_flash", None)
        _prev_eff = ((prev_flashed or 0) / (prev_fcnt or 0)) if (prev_fcnt or 0) > 0 else 0.0
        if _curr_eff is None:
            html.append("<td class='muted' title='No flash data'>—</td>")
        else:
            _delta_eff = _curr_eff - _prev_eff
            html.append(f"<td title='Δ vs prev: {_signed(_delta_eff,2)} (prev {_prev_eff:.2f})'>{_curr_eff:.2f}{_arrow(_delta_eff)}</td>")

        # Pistol & AWP kills
        html.append(f"<td title='Δ vs prev: {_signed(d_pistol,0)} (prev {int(prev_pistol) if prev_pistol is not None else 0})'>{p.get('pistol_kills',0)}{_arrow(d_pistol)}</td>")
        html.append(f"<td title='Δ vs prev: {_signed(d_awp,0)} (prev {int(prev_awp) if prev_awp is not None else 0})'>{p.get('awp_kills',0)}{_arrow(d_awp)}</td>")

        html.append("</tr>")

    html.append("</tbody></table>")
    
    # JavaScript for advanced table
    html.append(f'''
    <script>
    window.addEventListener('DOMContentLoaded', function(){{
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
    }});
    </script>
    ''')
    
    html.append("</div>")  # /tab-panel advanced
    return html

async def _render_team_maps_section_async(pool: AsyncConnectionPool, div: dict, team: dict,
                                        maps: list, map_deltas: dict, div_avgs: dict,
                                        team_index: int, _signed, _arrow) -> list[str]:
    # Render team maps section with map statistics table.
    html = []

    # Precompute highlights for maps section
    best_wr = max((r for r in maps if r["played"] > 0), key=lambda r: r["wr"], default=None)
    most_pick = max(maps, key=lambda r: r["picks"], default=None)
    most_ban = max(maps, key=lambda r: r["total_own_ban"], default=None)
    played_rows = [r for r in maps if r["played"] >= 2]
    avoid = min(played_rows, key=lambda r: r["wr"], default=None)

    tid2 = f"maps-{team_index}"

    html.extend(_render_card_header('maps', 'Kartta tilastot'))
    
    # Maps chips
    html.append('<div class="chips">')
    if most_ban and most_ban["total_own_ban"]>0:
        map_name = await map_pretty_name_async(pool, most_ban["map"])
        html.append(f'<span class="chip">Most banned: {map_name} ({most_ban["total_own_ban"]}×)</span>')
    if most_pick and most_pick["picks"]>0:
        map_name = await map_pretty_name_async(pool, most_pick["map"])
        html.append(f'<span class="chip">Most picked: {map_name} ({most_pick["picks"]}×)</span>')
    if best_wr and best_wr["wr"]>0:
        map_name = await map_pretty_name_async(pool, best_wr["map"])
        html.append(f'<span class="chip">Best WR: {map_name} ({best_wr["wr"]:.0f}%)</span>')
    if avoid:
        map_name = await map_pretty_name_async(pool, avoid["map"])
        html.append(f'<span class="chip">Map to avoid: {map_name} ({avoid["wr"]:.0f}%)</span>')
    html.append('</div>')

    # Toolbar (filter + CSV + column toggles)
    html.append(f'''
    <div class="toolbar">
      <label><input type="checkbox" id="{tid2}-played-only"> Show played only</label>
    </div>
    ''')
    
    # Maps table
    html.append(f'<table id="{tid2}" data-sort-col="0" data-sort-dir="asc">')
    html.append(f'''
    <thead><tr>
    <th data-sortable title="Map name" onclick="sortTable('{tid2}',0,false)">Map</th>
    <th data-sortable title="Maps played" onclick="sortTable('{tid2}',1,true)">Played</th>
    <th data-sortable title="Matches this map was your pick" onclick="sortTable('{tid2}',2,true)">Picks</th>
    <th data-sortable title="Matches this map was opponent pick" onclick="sortTable('{tid2}',3,true)">Opp picks</th>
    <th data-sortable title="Winrate on this map" onclick="sortTable('{tid2}',4,true)">WR %</th>
    <th data-sortable title="Winrate when you picked" onclick="sortTable('{tid2}',5,true)">WR own pick %</th>
    <th data-sortable title="Winrate when opponent picked" onclick="sortTable('{tid2}',6,true)">WR opp pick %</th>
    <th data-sortable title="Team K/D on this map" onclick="sortTable('{tid2}',7,true)">KD</th>
    <th data-sortable title="Average Damage / Round" onclick="sortTable('{tid2}',8,true)">ADR</th>
    <th data-sortable title="Round diff (won - lost)" onclick="sortTable('{tid2}',9,true)">±RD</th>
    <th data-sortable title="Times this map was your first ban" onclick="sortTable('{tid2}',10,true)">1st ban</th>
    <th data-sortable title="Times this map was your second ban" onclick="sortTable('{tid2}',11,true)">2nd ban</th>
    <th data-sortable title="Matches where opponent banned this map" onclick="sortTable('{tid2}',12,true)">Opp ban</th>
    <th data-sortable title="Your total bans (1st+2nd)" onclick="sortTable('{tid2}',13,true)">Total own ban</th>
    <th title="Times this map was BO3 decider or BO2 overflow" onclick="sortTable('{tid2}',14,true)">Dec/Overflow</th>
    </tr></thead><tbody>
    ''')

    # Map table rows
    for r in maps:
        md = map_deltas.get(r["map"])
        prev = md["prev"] if md else None
        dlt  = md["delta"] if md else None

        # Δ vs division avg
        dkd_div = 0.0; dadr_div = 0.0
        if r["map"] in div_avgs:
            dkd_div  = (r["kd"] or 0.0) - div_avgs[r["map"]][0]
            dadr_div = (r["adr"] or 0.0) - div_avgs[r["map"]][1]

        # Delta helper function
        def _pp(k, prec=0):
            if not dlt: return f"(no prev)"
            dv = dlt.get(k)
            pv = prev.get(k) if prev else None
            if isinstance(dv, float):
                s = f"{dv:+.{prec}f}"
            else:
                s = f"{int(dv) if dv is not None else 0:+d}"
            ptxt = f"{prev[k]:.{prec}f}" if (prev and isinstance(prev.get(k), float)) else f"{int(prev.get(k) or 0)}" if prev else "0"
            return f"Δ vs prev: {s} (prev {ptxt})"

        # WR tooltips with delta calculations
        prev_wr = (100.0 * (prev["wins"] or 0) / (prev["games"] or 0)) if (prev and prev["games"]) else 0.0
        wr_delta = r["wr"] - prev_wr

        prev_wr_own = (100.0 * (prev["wins_own"] or 0) / (prev["games_own"] or 0)) if (prev and prev["games_own"]) else 0.0
        wr_own_delta = r["wr_own"] - prev_wr_own

        prev_wr_opp = (100.0 * (prev["wins_opp"] or 0) / (prev["games_opp"] or 0)) if (prev and prev["games_opp"]) else 0.0
        wr_opp_delta = r["wr_opp"] - prev_wr_opp

        map_name = await map_pretty_name_async(pool, r["map"])
        html.append(f'''<tr>
        <td>{map_name}</td>
        <td title="{_pp('played',0)}">{r["played"]}{_arrow(dlt.get('played') if dlt else None)}</td>
        <td title="{_pp('picks',0)}">{r["picks"]}{_arrow(dlt.get('picks') if dlt else None)}</td>
        <td title="{_pp('opp_picks',0)}">{r["opp_picks"]}{_arrow(dlt.get('opp_picks') if dlt else None)}</td>
        <td class="wr" data-w="{r['wins']}" data-g="{r['games']}" data-pct="{r['wr']:.1f}" title="Δ WR: {wr_delta:+.1f} pp; prev {prev['wins'] if prev else 0}-{(prev['games']-(prev['wins'] or 0)) if prev else 0}"></td>
        <td class="wr" data-w="{r['wins_own']}" data-g="{r['games_own']}" data-pct="{r['wr_own']:.1f}" title="Δ WR own: {wr_own_delta:+.1f} pp; prev {prev['wins_own'] if prev else 0}/{prev['games_own'] if prev else 0}"></td>
        <td class="wr" data-w="{r['wins_opp']}" data-g="{r['games_opp']}" data-pct="{r['wr_opp']:.1f}" title="Δ WR opp: {wr_opp_delta:+.1f} pp; prev {prev['wins_opp'] if prev else 0}/{prev['games_opp'] if prev else 0}"></td>
        <td title="{_pp('kd',2)}; Δ vs div avg: {dkd_div:+.2f}">{r["kd"]:.2f}{_arrow(dlt.get('kd') if dlt else None)}</td>
        <td title="{_pp('adr',1)}; Δ vs div avg: {dadr_div:+.1f}">{r["adr"]:.1f}{_arrow(dlt.get('adr') if dlt else None)}</td>
        <td title="{_pp('rd',0)}">{r["rd"]}{_arrow(dlt.get('rd') if dlt else None)}</td>
        <td title="{_pp('ban1',0)}">{r["ban1"]}{_arrow(dlt.get('ban1') if dlt else None)}</td>
        <td title="{_pp('ban2',0)}">{r["ban2"]}{_arrow(dlt.get('ban2') if dlt else None)}</td>
        <td title="{_pp('opp_ban',0)}">{r["opp_ban"]}{_arrow(dlt.get('opp_ban') if dlt else None)}</td>
        <td title="{_pp('total_own_ban',0)}">{r["total_own_ban"]}{_arrow(dlt.get('total_own_ban') if dlt else None)}</td>
        <td title="{_pp('decov',0)}">{r.get("decov", 0)}{_arrow(dlt.get('decov') if dlt else None)}</td>
        </tr>''')

    html.append("</tbody></table>")
    
    # JavaScript for maps table - using hardcoded thresholds for now
    html.append(f'''
<script>
window.addEventListener('DOMContentLoaded', function(){{
postProcessTable('{tid2}', {{
      wrbars: [4,5,6],
      color: [
        {{col:7, p:[0.85, 1.00, 1.15] }},
        {{col:8, p:[65.0, 75.0, 85.0] }},
      ],
      defaultSort: {{col:0, dir:'asc'}},
}});
bindPlayedOnly('{tid2}', '{tid2}-played-only');
}});
</script>
''')
    html.extend(_render_card_footer())  # /maps-section
    return html

# =============================================
# Async Team Matches Functions
# =============================================

async def get_team_matches_mirror_async(
    pool: AsyncConnectionPool,
    championship_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    """Async version of get_team_matches_mirror"""
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded, alias="m")

    sql = f"""
    WITH my_matches AS (
      SELECT
        m.match_id, m.championship_id, m.team1_id, m.team2_id,
        m.best_of, m.status,
        COALESCE(m.started_at, m.scheduled_at, m.configured_at, 0) AS ts,
        CASE WHEN m.finished_at IS NOT NULL THEN 1 ELSE 0 END AS played
      FROM matches m
      WHERE m.championship_id = :champ AND (:team = m.team1_id OR :team = m.team2_id){excl_clause}
    ),
    mp AS (
      SELECT
        mm.match_id, mm.team1_id, mm.team2_id,
        mm.best_of, mm.status, mm.ts, mm.played,
        ma.round_index, ma.map_name, ma.score_team1, ma.score_team2,
        COALESCE(ma.is_forfeit, 0) AS map_is_forfeit
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
      mp.map_is_forfeit,
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
    JOIN my_matches mm_season ON mm_season.match_id = mp.match_id
    JOIN championships c ON c.championship_id = mm_season.championship_id
    LEFT JOIN ps_agg ps1 ON ps1.match_id=mp.match_id AND ps1.round_index=mp.round_index AND ps1.team_id=mp.team1_id
    LEFT JOIN ps_agg ps2 ON ps2.match_id=mp.match_id AND ps2.round_index=mp.round_index AND ps2.team_id=mp.team2_id
    LEFT JOIN picks pk    ON pk.match_id=mp.match_id AND pk.map_name=mp.map_name
    LEFT JOIN team_seasons t1 ON t1.team_id = mp.team1_id AND t1.season = c.season
    LEFT JOIN team_seasons t2 ON t2.team_id = mp.team2_id AND t2.season = c.season
    ORDER BY (mp.ts IS NULL) ASC, mp.ts ASC, mp.match_id ASC, mp.round_index ASC
    """
    
    rows = await query_async(pool, sql, {"champ": championship_id, "team": team_id, **excl_params})

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
                # Add avatars on both sides for visual parity
                "left":  {"team_id": team_id, "team_name": my_name or "", "avatar": (r["t1_avatar"] if me_on_left else r["t2_avatar"])},
                "right": {"team_id": opp_id,   "team_name": opp_name or "", "avatar": opp_avatar},
                # Precompute FACEIT URL when possible (match_id is FACEIT room id)
                "faceit_url": (f"https://www.faceit.com/cs2/room/{mid}" if mid else ""),
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
            "is_forfeit": bool(r["map_is_forfeit"]),
            "pick_team_id": r["pick_team_id"],
            "left":  {"adr": float(me_adr or 0.0),  "kd": float(me_kd),  "dmg": int(me_damage),  "kills": int(me_kills),  "deaths": int(me_deaths)},
            "right": {"adr": float(opp_adr or 0.0), "kd": float(opp_kd), "dmg": int(opp_damage), "kills": int(opp_kills), "deaths": int(opp_deaths)}
        })

    return [out[mid] for mid in sorted(out, key=lambda k: (out[k]["ts"] is None, out[k]["ts"] or 0, k))]

async def _render_team_matches_section_async(
    pool: AsyncConnectionPool,
    div: dict,
    team: dict,
    excluded_team_ids: Collection[str] | None = None,
) -> list[str]:
    """Render the team matches section (Ottelut tab)"""
    from html import escape
    
    team_id = team["team_id"]
    team_name = team["team_name"] or team["team_id"]
    
    # Get team matches data
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    matches = await get_team_matches_mirror_async(
        pool, div["championship_id"], team_id, excluded
    )

    # Calculate summary chips (Ottelut, Kartat, W-L, ±RD)
    stat_matches = matches if team.get("is_banned") else matches
    if not team.get("is_banned") and excluded:
        stat_matches = [
            m
            for m in matches
            if (m["left"]["team_id"] not in excluded and m["right"]["team_id"] not in excluded)
        ]

    num_matches = len(stat_matches)
    # Only count non-forfeit maps for "Kartat" statistic
    num_maps = sum(len([mp for mp in m["maps"] if not mp.get("is_forfeit", False)]) for m in stat_matches)
    # Include all maps (including forfeits) for W-L and RD calculations
    w = sum(1 for m in stat_matches for mp in m["maps"] if mp["rf"] > mp["ra"])
    l = sum(1 for m in stat_matches for mp in m["maps"] if mp["rf"] < mp["ra"])
    rd = sum(mp["rf"] - mp["ra"] for m in stat_matches for mp in m["maps"])
    
    if not matches:
        return [
            '<details class="card matches-mirror">',
            '  <summary>',
            '    <div class="card-head">',
            '      <span class="chev">›</span>',
            '      <span class="title">Ottelut</span>',
            '      <span class="hint">Ei otteluita</span>',
            '    </div>',
            '  </summary>',
            '  <div class="card-content">',
            f'    <div class="chips"><span class="chip">Ottelut 0</span> <span class="chip">Kartat 0</span> <span class="chip">W-L 0-0</span> <span class="chip">±RD 0</span></div>',
            '  </div>',
            '</details>'
        ]
    
    html = []
    html.append(f'<details class="card matches-mirror" data-team-id="{team_id}">')
    html.append('  <summary>')
    html.append('    <div class="card-head">')
    html.append('      <span class="chev">›</span>')
    html.append('      <span class="title">Ottelut</span>')
    html.append('      <span class="hint">Click to expand</span>')
    html.append('    </div>')
    html.append('  </summary>')
    html.append('  <div class="card-content">')
    html.append(f'    <div class="chips"><span class="chip">Ottelut {num_matches}</span> <span class="chip">Kartat {num_maps}</span> <span class="chip">W-L {w}-{l}</span> <span class="chip">±RD {rd}</span></div>')
    html.append(f'    <div class="toolbar">')
    html.append(f'      <label><input type="checkbox" id="only-played-{team_id}"> Näytä vain pelatut</label>')
    html.append(f'    </div>')
    html.append(f'    <div class="matches-list" id="matches-{team_id}">')
    for match in matches:
        await _render_single_match_async(pool, html, match, team_id)
    html.append('    </div>')
    # Inline script to bind "Show played only" behavior for this matches list (legacy-compatible)
    html.append(f'''    <script>
window.addEventListener('DOMContentLoaded', function(){{
    var root = document.getElementById('matches-{team_id}');
    if(!root) return;
    var box = root.parentElement.querySelector('#only-played-{team_id}');
    if(!box) return;
    function apply(){{
        var rows = root.querySelectorAll('.match-row');
        for (var i=0;i<rows.length;i++){{
            var row=rows[i]; var played=(row.getAttribute('data-played')==='1');
            row.style.display = (box.checked && !played)?'none':'';
        }}
    }}
    box.addEventListener('change', apply);
    apply();
}});
    </script>''')
    html.append('  </div>')
    html.append('</details>')
    return html

async def _render_single_match_async(pool: AsyncConnectionPool, html: list[str], match: dict, team_id: str) -> None:
    """Render a single match in the matches section"""
    from html import escape
    import datetime
    
    # Format timestamp
    ts = match.get("ts")
    if ts:
        try:
            dt = datetime.datetime.fromtimestamp(int(ts))
            date_str = dt.strftime("%d.%m.%Y %H:%M")
        except:
            date_str = "—"
    else:
        date_str = "—"

    left_team = match["left"]
    right_team = match["right"]
    left_logo = left_team.get("avatar") or ""
    right_logo = right_team.get("avatar") or ""
    left_name = escape(left_team["team_name"])
    right_name = escape(right_team["team_name"])
    left_link = f'<a href="#team-{left_team["team_id"]}">{left_name}</a>'
    right_link = f'<a href="#team-{right_team["team_id"]}">{right_name}</a>'

    # Determine match result (W/L/D)
    maps = match["maps"]
    maps_w = sum(1 for m in maps if m["rf"] > m["ra"])
    maps_l = sum(1 for m in maps if m["rf"] < m["ra"])
    maps_d = sum(1 for m in maps if m["rf"] == m["ra"])
    scoreline = f"{maps_w}–{maps_l}"
    result_chip = ''
    if maps_w > maps_l:
        result_chip = '<span class="chip result-win">W</span>'
    elif maps_w < maps_l:
        result_chip = '<span class="chip result-loss">L</span>'
    else:
        result_chip = '<span class="chip result-draw">D</span>'

    # FACEIT link (if available)
    faceit_url = match.get("faceit_url") or ""
    faceit_link = f'<a class="faceit-link" href="{faceit_url}" target="_blank" rel="noopener">Open on FACEIT</a>' if faceit_url else ''

    html.append(f'  <details class="match-row" data-played={int(match.get("played", 0))}>')
    html.append(f'    <summary class="match-summary" role="button">')
    left_logo_html = f'<img class="logo" src="{left_logo}" alt="">' if left_logo else ""
    html.append(f'      <div class="team side-left">{left_logo_html}<div class="name">{left_link}</div></div>')
    html.append(f'      <div class="center">')
    html.append(f'        <div class="meta"><span class="date">{date_str}</span></div>')
    status_text = match.get("status") or ""
    if status_text:
        status_text = str(status_text).capitalize()
    html.append(f'        <div class="result-row"><span class="stage-chip">{escape(status_text)}</span></div>')
    html.append(f'        <div class="result-row">{result_chip}</div>')
    html.append(f'        <div class="scoreline"><span class="maps-score">{scoreline}</span></div>')
    html.append(f'        {faceit_link}')
    html.append(f'      </div>')
    right_logo_html = f'<img class="logo" src="{right_logo}" alt="">' if right_logo else ""
    html.append(f'      <div class="team side-right"><div class="name">{right_link}</div>{right_logo_html}</div>')
    html.append(f'    </summary>')
    html.append(f'    <div class="match-details">')
    # Render individual maps and accumulate totals
    total_w = total_l = 0
    total_rd = 0
    total_kills = 0
    total_deaths = 0
    for map_data in maps:
        await _render_single_map_async(pool, html, map_data, team_id)
        rf, ra = map_data.get("rf", 0), map_data.get("ra", 0)
        if rf > ra:
            total_w += 1
        elif rf < ra:
            total_l += 1
        total_rd += (rf - ra)
        left_stats = map_data.get("left", {})
        total_kills += int(left_stats.get("kills", 0))
        total_deaths += int(left_stats.get("deaths", 0))
    # Totals row (match aggregate)
    kd_total = (float(total_kills) / total_deaths) if total_deaths else float(total_kills)
    html.append('      <div class="aggregate">')
    html.append('        <div class="totals">')
    html.append('          <span class="label">Team Totals:</span>')
    html.append(f'          <span>Maps {len(maps)} ({total_w}-{total_l})</span>')
    html.append(f'          <span>RD {total_rd:+d}</span>')
    html.append(f'          <span>K/D {kd_total:.2f}</span>')
    html.append('        </div>')
    html.append('      </div>')
    html.append(f'    </div>')
    html.append(f'  </details>')

async def _render_single_map_async(pool: AsyncConnectionPool, html: list[str], map_data: dict, team_id: str) -> None:
    """Render a single map within a match"""
    from html import escape
    
    map_name = map_data["map"]
    pretty_name = await map_pretty_name_async(pool, map_name)
    art = await get_map_art_async(pool, map_name)
    img_src = (art.get("image_lg") if art else None) or ""
    
    # Score and stats
    rf, ra = int(map_data.get("rf", 0)), int(map_data.get("ra", 0))
    left_stats = map_data.get("left", {})
    right_stats = map_data.get("right", {})
    
    # Round chip class by outcome for left side
    if rf > ra:
        round_cls_left = 'win'; round_cls_right = 'loss'
    elif rf < ra:
        round_cls_left = 'loss'; round_cls_right = 'win'
    else:
        round_cls_left = round_cls_right = 'draw'

    # Pick chip placement
    pick_team_id = map_data.get("pick_team_id")
    left_pick = (pick_team_id == team_id)
    right_pick = (pick_team_id is not None and pick_team_id != team_id)

    # Build chip content inline for legacy match
    left_chips = []
    left_chips.append(f'<span class="chip round {round_cls_left}">R {rf}</span>')
    left_chips.append(f'<span class="chip stat"><span class="stat-label">ADR</span> {float(left_stats.get("adr", 0.0)):.1f}</span>')
    left_chips.append(f'<span class="chip stat"><span class="stat-label">K/D</span> {float(left_stats.get("kd", 0.0)):.2f}</span>')
    left_chips.append(f'<span class="chip stat"><span class="stat-label">DMG</span> {int(left_stats.get("dmg", 0))}</span>')
    if left_pick:
        left_chips.append('<span class="chip stat pick">Pick</span>')

    right_chips = []
    right_chips.append(f'<span class="chip round {round_cls_right}">R {ra}</span>')
    right_chips.append(f'<span class="chip stat"><span class="stat-label">ADR</span> {float(right_stats.get("adr", 0.0)):.1f}</span>')
    right_chips.append(f'<span class="chip stat"><span class="stat-label">K/D</span> {float(right_stats.get("kd", 0.0)):.2f}</span>')
    right_chips.append(f'<span class="chip stat"><span class="stat-label">DMG</span> {int(right_stats.get("dmg", 0))}</span>')
    if right_pick:
        right_chips.append('<span class="chip stat pick">Pick</span>')

    # Render map row with inline chips (legacy format)
    html.append('        <div class="map-row">')
    html.append(f'          <div class="map-side side-left">{" ".join(left_chips)}</div>')
    html.append(f'          <div class="map-name">{escape(pretty_name)}')
    if img_src:
        html.append(f'            <img class="map-img" src="{img_src}" alt="{escape(pretty_name)}" onerror="this.style.display=\'none\'">')
    html.append('          </div>')
    html.append(f'          <div class="map-side side-right">{" ".join(right_chips)}</div>')
    html.append('        </div>')