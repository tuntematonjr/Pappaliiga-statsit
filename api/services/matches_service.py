from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Tuple

from db_async import query_async

from api.exceptions import NotFoundError
from api.services.cache_helpers import (
    get_championship_revision,
    get_global_revision,
    get_season_revision,
)
from api.services.player_stats_payload import build_player_stats_payload
from api.utils.cache import AsyncTTLCache

_MATCH_LIST_CACHE = AsyncTTLCache(ttl_seconds=30, maxsize=256)
_UPCOMING_MATCH_CACHE = AsyncTTLCache(ttl_seconds=30, maxsize=256)

_UPCOMING_STATUSES = ("CONFIGURED", "PENDING", "READY", "SCHEDULED")


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
               m.finished_at,
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
        SELECT round_index, map_name, score_team1, score_team2, winner_team_id, is_forfeit
        FROM maps
        WHERE match_id = :match_id
        ORDER BY round_index
        """,
        {"match_id": match_id},
    )

    return {
        "match": match,
        "maps": map_rows,
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
            p.nickname,
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
        LEFT JOIN players p ON p.player_id = ps.player_id
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
