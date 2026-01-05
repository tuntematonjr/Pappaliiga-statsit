from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Tuple

from db_async import query_async

from api.exceptions import NotFoundError
from api.utils.cache import AsyncTTLCache

_MATCH_LIST_CACHE = AsyncTTLCache(ttl_seconds=30, maxsize=256)


def _build_etag(championship_id: str, revision: Any, total: int, limit: int, offset: int) -> str:
    source = f"{championship_id}:{revision}:{total}:{limit}:{offset}"
    return hashlib.md5(source.encode("utf-8")).hexdigest()


async def get_division_matches(
    championship_id: str,
    *,
    limit: int,
    offset: int,
) -> Tuple[list[dict[str, Any]], int, str | None, str]:
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
                t1.name AS team1_name,
                t2.name AS team2_name,
                COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team1_id THEN 1 ELSE 0 END), 0) AS team1_score,
                COALESCE(SUM(CASE WHEN mp.winner_team_id = m.team2_id THEN 1 ELSE 0 END), 0) AS team2_score,
                m.activity_ts
            FROM matches m
            LEFT JOIN teams t1 ON t1.team_id = m.team1_id
            LEFT JOIN teams t2 ON t2.team_id = m.team2_id
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
                t1.name,
                t2.name,
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

        revision_rows = await query_async(
            """
            SELECT MAX(updated_at) AS revision
            FROM matches
            WHERE championship_id = :champ_id
            """,
            {"champ_id": championship_id},
        )
        revision = revision_rows[0].get("revision") if revision_rows else None
        etag = _build_etag(championship_id, revision, total, limit, offset)
        return rows, total, revision, etag

    cache_key = (championship_id, limit, offset)
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
               t1.name AS team1_name,
               t2.name AS team2_name,
               t1.avatar AS team1_avatar, t2.avatar AS team2_avatar
        FROM matches m
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
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
            ps.stats_json
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
        stats_raw = row.get("stats_json")
        if isinstance(stats_raw, str):
            try:
                stats_raw = json.loads(stats_raw)
            except Exception:
                stats_raw = {}
        elif stats_raw is None:
            stats_raw = {}
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
