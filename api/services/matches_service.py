from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any, Tuple
import httpx

from db_async import query_async

from api.exceptions import NotFoundError
from api.services.cache_helpers import (
    get_championship_revision,
    get_global_revision,
    get_season_revision,
)
from api.services.player_stats_payload import build_player_stats_payload
from api.utils.cache import AsyncTTLCache

_MATCH_LIST_CACHE = AsyncTTLCache(ttl_seconds=21600, maxsize=256)
_UPCOMING_MATCH_CACHE = AsyncTTLCache(ttl_seconds=21600, maxsize=256)
_DEMO_LIST_CACHE = AsyncTTLCache(ttl_seconds=300, maxsize=4096)
_DEMO_PROBE_SEMAPHORE = asyncio.Semaphore(4)
_DEMO_CACHE_VERSION = 3

_UPCOMING_STATUSES = ("CONFIGURED", "PENDING", "READY", "SCHEDULED")


def build_demo_url(championship_id: str, match_id: str, demo_index: int) -> str:
    return f"https://pappa.aukko.net/demos/{championship_id}/{match_id}_{demo_index}.zst"


def _normalize_veto_action(status_value: Any) -> str | None:
    raw = str(status_value or "").strip().lower()
    if not raw:
        return None
    if (
        "ban" in raw
        or "drop" in raw
        or "remove" in raw
        or "eliminat" in raw
    ):
        return "ban"
    if "pick" in raw or raw in {"picked", "select", "selected"}:
        return "pick"
    if "decider" in raw or raw == "decide":
        return "decider"
    if "overflow" in raw:
        return "overflow"
    return None


def _build_etag(championship_id: str, revision: Any, total: int, limit: int, offset: int) -> str:
    source = f"{championship_id}:{revision}:{total}:{limit}:{offset}"
    return hashlib.md5(source.encode("utf-8")).hexdigest()


def _build_upcoming_etag(
    revision: Any,
    total: int,
    limit: int,
    offset: int,
    championship_id: str | None,
    team_id: str | None,
    season: int | None,
    include_playoffs: bool,
) -> str:
    source = f"{revision}:{total}:{limit}:{offset}:{championship_id}:{team_id}:{season}:{include_playoffs}"
    return hashlib.md5(source.encode("utf-8")).hexdigest()


def _coerce_epoch_ms(value: Any) -> int | None:
    if value in (None, 0):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return int(value.timestamp() * 1000)
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if abs(numeric) < 1_000_000_000_000:
        numeric *= 1000
    return numeric


def _iso_from_epoch(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


async def get_division_matches(
    championship_id: str,
    *,
    limit: int,
    offset: int,
) -> Tuple[list[dict[str, Any]], int, str | None, str]:
    revision_rows = await query_async(
        """
        SELECT MAX(updated_at) AS revision
        FROM matches
        WHERE championship_id = :champ_id
        """,
        {"champ_id": championship_id},
    )
    revision = revision_rows[0].get("revision") if revision_rows else None

    async def producer() -> Tuple[list[dict[str, Any]], int, str | None, str]:
        rows = await query_async(
            """
            SELECT
                m.match_id,
                m.championship_id,
                m.best_of,
                m.status,
                m.finished_at,
                m.team1_id,
                m.team2_id,
                m.is_forfeit,
                m.ignored_due_ban,
                COALESCE(tc1.team_name, t1.name) AS team1_name,
                COALESCE(tc2.team_name, t2.name) AS team2_name,
                t1.avatar AS team1_avatar,
                t2.avatar AS team2_avatar,
                COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team1_id THEN 1 ELSE 0 END), 0) AS team1_score,
                COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team2_id THEN 1 ELSE 0 END), 0) AS team2_score,
                m.activity_ts
            FROM matches m
            LEFT JOIN teams t1 ON t1.team_id = m.team1_id
            LEFT JOIN teams t2 ON t2.team_id = m.team2_id
            LEFT JOIN team_championships tc1 ON tc1.team_id = m.team1_id AND tc1.championship_id = :champ_id
            LEFT JOIN team_championships tc2 ON tc2.team_id = m.team2_id AND tc2.championship_id = :champ_id
            LEFT JOIN maps mp ON mp.match_id = m.match_id
            WHERE m.championship_id = :champ_id
            GROUP BY
                m.match_id,
                m.championship_id,
                m.best_of,
                m.status,
                m.finished_at,
                m.team1_id,
                m.team2_id,
                m.is_forfeit,
                m.ignored_due_ban,
                COALESCE(tc1.team_name, t1.name),
                COALESCE(tc2.team_name, t2.name),
                m.activity_ts
            ORDER BY m.activity_ts DESC, m.match_id
            LIMIT :limit OFFSET :offset
            """,
            {"champ_id": championship_id, "limit": limit, "offset": offset},
        )

        count_rows = await query_async(
            "SELECT COUNT(*) AS total FROM matches WHERE championship_id = :champ_id",
            {"champ_id": championship_id},
        )
        total = int(count_rows[0].get("total") or 0) if count_rows else 0
        etag = _build_etag(championship_id, revision, total, limit, offset)
        return rows, total, revision, etag

    cache_key = (championship_id, limit, offset, revision)
    cached_value, _ = await _MATCH_LIST_CACHE.get_or_set(cache_key, producer)
    return cached_value


async def get_match_details(match_id: str) -> dict[str, Any]:
    match_rows = await query_async(
        """
        SELECT
               m.match_id,
               m.championship_id,
               m.best_of,
               m.status,
               m.finished_at,
               COALESCE(m.started_at, m.scheduled_at, m.configured_at, m.activity_ts, m.finished_at, 0) AS ts,
               m.team1_id,
               m.team2_id,
               m.is_forfeit,
               m.ignored_due_ban,
               COALESCE(tc1.team_name, t1.name) AS team1_name,
               COALESCE(tc2.team_name, t2.name) AS team2_name,
               t1.avatar AS team1_avatar,
               t2.avatar AS team2_avatar
        FROM matches m
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
        LEFT JOIN team_championships tc1 ON tc1.team_id = m.team1_id AND tc1.championship_id = m.championship_id
        LEFT JOIN team_championships tc2 ON tc2.team_id = m.team2_id AND tc2.championship_id = m.championship_id
        WHERE m.match_id = :match_id
        """,
        {"match_id": match_id},
    )

    if not match_rows:
        raise NotFoundError(f"Match '{match_id}' not found")

    match = match_rows[0]

    map_rows = await query_async(
        """
        WITH ps_agg AS (
            SELECT
                ps.match_id,
                ps.round_index,
                ps.team_id,
                SUM(COALESCE(ps.kills, 0)) AS kills,
                SUM(COALESCE(ps.deaths, 0)) AS deaths,
                SUM(COALESCE(ps.damage, 0)) AS dmg,
                AVG(NULLIF(ps.adr, 0)) AS adr_avg
            FROM player_stats ps
            WHERE ps.match_id = :match_id
            GROUP BY ps.match_id, ps.round_index, ps.team_id
        ),
        picks AS (
            SELECT
                mv.match_id,
                mv.map_name,
                MAX(mv.selected_by_team_id) AS pick_team_id
            FROM map_votes mv
            WHERE mv.match_id = :match_id
              AND LOWER(COALESCE(mv.status, '')) IN ('pick', 'picked')
            GROUP BY mv.match_id, mv.map_name
        )
        SELECT
            ma.round_index,
            ma.map_name,
            ma.score_team1,
            ma.score_team2,
            ma.winner_team_id,
            ma.is_forfeit,
            mc.image_sm,
            mc.image_lg,
            pk.pick_team_id,
            COALESCE(ps1.kills, 0) AS t1_kills,
            COALESCE(ps1.deaths, 0) AS t1_deaths,
            COALESCE(ps1.adr_avg, 0.0) AS t1_adr,
            COALESCE(ps1.dmg, 0) AS t1_dmg,
            COALESCE(ps2.kills, 0) AS t2_kills,
            COALESCE(ps2.deaths, 0) AS t2_deaths,
            COALESCE(ps2.adr_avg, 0.0) AS t2_adr,
            COALESCE(ps2.dmg, 0) AS t2_dmg
        FROM maps ma
        JOIN matches m ON m.match_id = ma.match_id
        LEFT JOIN maps_catalog mc ON LOWER(mc.map_id) = LOWER(ma.map_name)
        LEFT JOIN picks pk ON pk.match_id = ma.match_id AND pk.map_name = ma.map_name
        LEFT JOIN ps_agg ps1 ON ps1.match_id = ma.match_id AND ps1.round_index = ma.round_index AND ps1.team_id = m.team1_id
        LEFT JOIN ps_agg ps2 ON ps2.match_id = ma.match_id AND ps2.round_index = ma.round_index AND ps2.team_id = m.team2_id
        WHERE ma.match_id = :match_id
        ORDER BY ma.round_index
        """,
        {"match_id": match_id},
    )

    veto_rows = await query_async(
        """
        SELECT
            mv.vote_id,
            mv.map_name,
            mv.status,
            mv.selected_by_team_id,
            COALESCE(tc.team_name, t.name) AS selected_by_team_name
        FROM map_votes mv
        LEFT JOIN matches m ON m.match_id = mv.match_id
        LEFT JOIN teams t ON t.team_id = mv.selected_by_team_id
        LEFT JOIN team_championships tc ON tc.team_id = mv.selected_by_team_id AND tc.championship_id = m.championship_id
        WHERE mv.match_id = :match_id
        ORDER BY mv.vote_id ASC
        """,
        {"match_id": match_id},
    )

    normalized_maps: list[dict[str, Any]] = []
    for row in map_rows:
        t1_kills = int(float(row.get("t1_kills") or 0))
        t1_deaths = int(float(row.get("t1_deaths") or 0))
        t2_kills = int(float(row.get("t2_kills") or 0))
        t2_deaths = int(float(row.get("t2_deaths") or 0))
        t1_kd = (float(t1_kills) / t1_deaths) if t1_deaths else float(t1_kills)
        t2_kd = (float(t2_kills) / t2_deaths) if t2_deaths else float(t2_kills)

        normalized_maps.append(
            {
                "round_index": int(row.get("round_index") or 0),
                "map_name": row.get("map_name"),
                "map": row.get("map_name"),
                "score_team1": int(float(row.get("score_team1") or 0)),
                "score_team2": int(float(row.get("score_team2") or 0)),
                "winner_team_id": row.get("winner_team_id"),
                "is_forfeit": bool(row.get("is_forfeit")),
                "image_sm": row.get("image_sm"),
                "image_lg": row.get("image_lg"),
                "pick_team_id": row.get("pick_team_id"),
                "left": {
                    "kills": t1_kills,
                    "deaths": t1_deaths,
                    "adr": float(row.get("t1_adr") or 0.0),
                    "kd": float(t1_kd),
                    "dmg": int(float(row.get("t1_dmg") or 0)),
                },
                "right": {
                    "kills": t2_kills,
                    "deaths": t2_deaths,
                    "adr": float(row.get("t2_adr") or 0.0),
                    "kd": float(t2_kd),
                    "dmg": int(float(row.get("t2_dmg") or 0)),
                },
            }
        )

    veto_steps: list[dict[str, Any]] = []
    step_order = 1
    for row in veto_rows:
        action = _normalize_veto_action(row.get("status"))

        if not action:
            continue

        if action == "pick":
            label = "Pick"
        elif action == "ban":
            label = "Ban"
        elif action == "decider":
            label = "Decider"
        else:
            label = "Overflow"

        veto_steps.append(
            {
                "step": step_order,
                "action": action,
                "label": label,
                "mapName": row.get("map_name") or "Kartta",
                "teamId": row.get("selected_by_team_id"),
                "teamName": row.get("selected_by_team_name") or "",
            }
        )
        step_order += 1

    best_of = int(match.get("best_of") or 0)
    veto_entry = {
        "matchId": match.get("match_id"),
        "format": f"bo{best_of}" if best_of else "",
        "steps": veto_steps,
    }

    return {
        "match": match,
        "maps": normalized_maps,
        "veto_entry": veto_entry,
    }


async def get_match_bundle(match_id: str) -> dict[str, Any]:
    details = await get_match_details(match_id)
    player_stats = await get_match_player_stats(match_id)
    return {
        "details": details,
        "player_stats": player_stats,
    }


async def get_match_player_stats(match_id: str) -> list[dict[str, Any]]:
    match_rows = await query_async(
        "SELECT match_id FROM matches WHERE match_id = :match_id",
        {"match_id": match_id},
    )
    if not match_rows:
        raise NotFoundError(f"Match '{match_id}' not found")

    rows = await query_async(
        """
        SELECT
            ps.round_index,
            ps.map_id,
            mp.map_name,
            ps.player_id,
            COALESCE(pc.player_name, p.nickname) AS nickname,
            ps.team_id,
            ps.opponent_team_id,
            ps.is_forfeit_map,
            ps.kills, ps.deaths, ps.assists, ps.mvps, ps.headshots, ps.damage,
            ps.sniper_kills, ps.pistol_kills, ps.knife_kills, ps.zeus_kills, ps.first_kills,
            ps.enemies_flashed, ps.flash_count, ps.flash_successes, ps.utility_damage,
            ps.utility_count, ps.utility_successes, ps.utility_enemies,
            ps.mk_2k, ps.mk_3k, ps.mk_4k, ps.mk_5k,
            ps.clutch_kills, ps.cl_1v1_attempts, ps.cl_1v1_wins, ps.cl_1v2_attempts, ps.cl_1v2_wins,
            ps.entry_count, ps.entry_wins,
            ps.kd, ps.kr, ps.adr, ps.hs_pct, ps.result
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        LEFT JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN player_championships pc ON pc.player_id = ps.player_id AND pc.championship_id = m.championship_id
        LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE ps.match_id = :match_id
        ORDER BY ps.round_index, ps.player_id
        """,
        {"match_id": match_id},
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        stats_raw = build_player_stats_payload(row)
        normalized.append(
            {
                "round_index": int(row.get("round_index") or 0),
                "map_id": row.get("map_id"),
                "map_name": row.get("map_name"),
                "player_id": row.get("player_id"),
                "nickname": row.get("nickname"),
                "team_id": row.get("team_id"),
                "opponent_team_id": row.get("opponent_team_id"),
                "is_forfeit_map": bool(row.get("is_forfeit_map")),
                "stats": stats_raw or {},
            }
        )
    return normalized


async def get_upcoming_matches(
    *,
    championship_id: str | None = None,
    team_id: str | None = None,
    season: int | None = None,
    include_playoffs: bool = True,
    limit: int,
    offset: int,
) -> Tuple[list[dict[str, Any]], int, str | None, str]:
    if championship_id:
        revision = await get_championship_revision(championship_id)
    elif season is not None:
        revision = await get_season_revision(season)
    else:
        revision = await get_global_revision()

    statuses_sql = ", ".join([f"'{status}'" for status in _UPCOMING_STATUSES])
    where_clauses = [f"UPPER(COALESCE(m.status, '')) IN ({statuses_sql})"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if championship_id:
        where_clauses.append("m.championship_id = :champ_id")
        params["champ_id"] = championship_id
    if team_id:
        where_clauses.append("(m.team1_id = :team_id OR m.team2_id = :team_id)")
        params["team_id"] = team_id
    if season is not None:
        where_clauses.append("c.season = :season")
        params["season"] = season
    if not include_playoffs:
        where_clauses.append("c.is_playoffs = 0")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    async def producer() -> Tuple[list[dict[str, Any]], int, str | None, str]:
        rows = await query_async(
            f"""
            SELECT
                m.match_id,
                m.championship_id,
                c.season,
                c.division_num,
                c.is_playoffs,
                c.name AS division_name,
                c.slug AS division_slug,
                m.status,
                m.team1_id,
                m.team2_id,
                COALESCE(tc1.team_name, t1.name) AS team1_name,
                COALESCE(tc2.team_name, t2.name) AS team2_name,
                t1.avatar AS team1_avatar,
                t2.avatar AS team2_avatar,
                NULLIF(m.scheduled_at, 0) AS scheduled_at,
                NULLIF(m.configured_at, 0) AS configured_at,
                NULLIF(m.started_at, 0) AS started_at,
                COALESCE(
                    NULLIF(m.scheduled_at, 0),
                    NULLIF(m.configured_at, 0),
                    NULLIF(m.started_at, 0),
                    NULLIF(m.activity_ts, 0)
                ) AS scheduled_ts
            FROM matches m
            JOIN championships c ON c.championship_id = m.championship_id
            LEFT JOIN teams t1 ON t1.team_id = m.team1_id
            LEFT JOIN teams t2 ON t2.team_id = m.team2_id
            LEFT JOIN team_championships tc1 ON tc1.team_id = m.team1_id AND tc1.championship_id = m.championship_id
            LEFT JOIN team_championships tc2 ON tc2.team_id = m.team2_id AND tc2.championship_id = m.championship_id
            WHERE {where_sql}
            ORDER BY (scheduled_ts IS NULL) ASC, scheduled_ts ASC, m.match_id ASC
            LIMIT :limit OFFSET :offset
            """,
            params,
        )

        count_rows = await query_async(
            f"""
            SELECT COUNT(*) AS total
            FROM matches m
            JOIN championships c ON c.championship_id = m.championship_id
            WHERE {where_sql}
            """,
            {k: v for k, v in params.items() if k not in {"limit", "offset"}},
        )
        total = int(count_rows[0].get("total") or 0) if count_rows else 0

        normalized: list[dict[str, Any]] = []
        for row in rows:
            scheduled_ms = _coerce_epoch_ms(row.get("scheduled_ts"))
            normalized.append(
                {
                    **row,
                    "is_playoffs": bool(row.get("is_playoffs")),
                    "scheduled_ts": scheduled_ms,
                    "scheduled_at": _iso_from_epoch(scheduled_ms),
                    "faceit_url": f"https://www.faceit.com/cs2/room/{row.get('match_id')}" if row.get("match_id") else "",
                }
            )

        etag = _build_upcoming_etag(
            revision,
            total,
            limit,
            offset,
            championship_id,
            team_id,
            season,
            include_playoffs,
        )
        return normalized, total, revision, etag

    cache_key = (championship_id, team_id, season, include_playoffs, limit, offset, revision)
    cached_value, _ = await _UPCOMING_MATCH_CACHE.get_or_set(cache_key, producer)
    return cached_value


def _build_demo_probe_indices(expected_count: int | None) -> list[int]:
    base = int(expected_count or 0)
    if base <= 0:
        base = 2
    limit = min(8, max(2, base))
    return list(range(0, limit))


async def _probe_demo_exists_once(client: httpx.AsyncClient, url: str) -> tuple[bool, int | None]:
    try:
        response = await client.head(url)
        if response.status_code in (200, 206):
            return True, response.status_code
        if response.status_code == 404:
            return False, response.status_code
    except httpx.HTTPError:
        pass

    try:
        response = await client.get(url, headers={"Range": "bytes=0-0"})
        if response.status_code in (200, 206):
            return True, response.status_code
        return False, response.status_code
    except httpx.HTTPError:
        return False, None


async def _probe_demo_exists_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    attempts: int = 3,
) -> tuple[bool, int | None]:
    last_status: int | None = None
    for attempt in range(max(1, attempts)):
        exists, status_code = await _probe_demo_exists_once(client, url)
        last_status = status_code
        if exists:
            return True, status_code
        if status_code == 404:
            return False, status_code
        if attempt < attempts - 1:
            if status_code == 429:
                await asyncio.sleep(0.80 * (attempt + 1))
            else:
                await asyncio.sleep(0.20 * (attempt + 1))
    return False, last_status


async def get_match_demos(
    championship_id: str,
    match_id: str,
    *,
    expected_count: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    normalized_expected = int(expected_count or 0)
    cache_key = (_DEMO_CACHE_VERSION, championship_id, match_id, normalized_expected)

    if not force:
        cached = await _DEMO_LIST_CACHE.get(cache_key)
        if cached is not None:
            return cached

    probe_indices = _build_demo_probe_indices(normalized_expected)
    timeout = httpx.Timeout(10.0)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        async def probe_index(demo_index: int) -> tuple[int, str, bool]:
            url = build_demo_url(championship_id, match_id, demo_index)
            async with _DEMO_PROBE_SEMAPHORE:
                exists, _status_code = await _probe_demo_exists_retry(client, url, attempts=3)
            return demo_index, url, exists

        results = await asyncio.gather(*(probe_index(idx) for idx in probe_indices))

    found_items = [
        {"demo_index": int(demo_index), "url": str(url)}
        for demo_index, url, exists in results
        if exists
    ]
    found_items.sort(key=lambda row: row["demo_index"])

    payload = {
        "championship_id": championship_id,
        "match_id": match_id,
        "items": found_items,
    }
    ttl = 300 if found_items else 30
    await _DEMO_LIST_CACHE.set(cache_key, payload, ttl_seconds=ttl)
    return payload
