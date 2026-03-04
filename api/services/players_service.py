from __future__ import annotations

import logging
from typing import Any, Dict, Optional


from db_async import compute_player_map_deltas_async, query_async

from api.exceptions import NotFoundError
from api.services.cache_helpers import (
    GLOBAL_CACHE,
    get_championship_revision,
    get_global_revision,
    select_season_cache,
)

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"
LOGGER = logging.getLogger(__name__)


async def fetch_player(player_id: str) -> dict[str, Any]:
    revision = await get_global_revision()
    cache_key = ("fetch_player", player_id, revision)

    async def _compute():
        try:
            rows = await query_async(
                """
                SELECT player_id, nickname, avatar, faceit_url
                FROM players
                WHERE player_id = :player_id
                """,
                {"player_id": player_id},
            )
            if not rows:
                rows = await query_async(
                    """
                    SELECT player_id, nickname, avatar, faceit_url
                    FROM players
                    WHERE LOWER(nickname) = LOWER(:nickname)
                    ORDER BY player_id ASC
                    LIMIT 1
                    """,
                    {"nickname": player_id},
                )
        except Exception:
            # Older schemas may not have all optional columns yet.
            rows = await query_async(
                """
                SELECT player_id, nickname
                FROM players
                WHERE player_id = :player_id
                """,
                {"player_id": player_id},
            )
            if not rows:
                rows = await query_async(
                    """
                    SELECT player_id, nickname
                    FROM players
                    WHERE LOWER(nickname) = LOWER(:nickname)
                    ORDER BY player_id ASC
                    LIMIT 1
                    """,
                    {"nickname": player_id},
                )
        if not rows:
            raise NotFoundError(f"Player '{player_id}' not found")
        player = dict(rows[0])
        player.setdefault("avatar", DEFAULT_AVATAR)
        player.setdefault("faceit_url", None)
        if not player.get("nickname"):
            player["nickname"] = str(player.get("player_id") or player_id)
        return player

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def fetch_player_season_stats(player_id: str) -> list[dict[str, Any]]:
    revision = await get_global_revision()
    cache_key = ("fetch_player_season_stats", player_id, revision)

    async def _compute():
        rows = await query_async(
            """
            SELECT
                m.season,
                m.division_num,
                m.championship_id,
                COALESCE(c.is_playoffs, 0) AS is_playoffs,
                MIN(ps.team_id) AS team_id,
                MIN(t.name) AS team_name,
                MIN(t.avatar) AS team_avatar,
                COUNT(*) AS maps_played,
                SUM(COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0)) AS rounds_played,
                SUM(ps.kills) AS kills,
                SUM(ps.deaths) AS deaths,
                SUM(ps.assists) AS assists,
                SUM(ps.mvps) AS mvps,
                SUM(ps.headshots) AS headshots,
                SUM(ps.damage) AS damage,
                SUM(ps.sniper_kills) AS sniper_kills,
                SUM(ps.pistol_kills) AS pistol_kills,
                SUM(ps.knife_kills) AS knife_kills,
                SUM(ps.zeus_kills) AS zeus_kills,
                SUM(ps.first_kills) AS first_kills,
                SUM(ps.enemies_flashed) AS enemies_flashed,
                SUM(ps.flash_count) AS flash_count,
                SUM(ps.flash_successes) AS flash_successes,
                SUM(ps.utility_damage) AS utility_damage,
                SUM(ps.utility_count) AS utility_count,
                SUM(ps.utility_successes) AS utility_successes,
                SUM(ps.utility_enemies) AS utility_enemies,
                SUM(ps.mk_2k) AS mk_2k,
                SUM(ps.mk_3k) AS mk_3k,
                SUM(ps.mk_4k) AS mk_4k,
                SUM(ps.mk_5k) AS mk_5k,
                SUM(ps.clutch_kills) AS clutch_kills,
                SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
                SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
                SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
                SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
                SUM(ps.entry_count) AS entry_count,
                SUM(ps.entry_wins) AS entry_wins
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            LEFT JOIN championships c ON c.championship_id = m.championship_id
            LEFT JOIN teams t ON t.team_id = ps.team_id
            LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
            WHERE ps.player_id = :player_id
              AND COALESCE(ps.is_forfeit_map, 0) = 0
            GROUP BY m.season, m.division_num, m.championship_id, c.is_playoffs
            ORDER BY m.season DESC, m.division_num ASC, COALESCE(c.is_playoffs, 0) ASC
            """,
            {"player_id": player_id},
        )
        if not rows:
            raise NotFoundError(f"No stats found for player '{player_id}'")
        for row in rows:
            kills = float(row.get("kills") or 0)
            deaths = float(row.get("deaths") or 0)
            rounds_played = float(row.get("rounds_played") or 0)
            damage = float(row.get("damage") or 0)
            headshots = float(row.get("headshots") or 0)
            row["kd"] = (kills / deaths) if deaths > 0 else kills
            row["kr"] = (kills / rounds_played) if rounds_played > 0 else 0.0
            row["adr"] = (damage / rounds_played) if rounds_played > 0 else 0.0
            row["hs_pct"] = ((headshots / kills) * 100.0) if kills > 0 else 0.0
        return rows

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def fetch_player_map_stats(championship_id: str, player_id: str) -> list[dict[str, Any]]:
    revision = await get_championship_revision(championship_id)
    cache_key = ("fetch_player_map_stats", championship_id, player_id, revision)
    champ_rows = await query_async(
        "SELECT season FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    season = int(champ_rows[0]["season"])

    async def _compute():
        map_deltas = await compute_player_map_deltas_async(championship_id, player_id)
        if not map_deltas:
            raise NotFoundError(
                f"No map stats found for player '{player_id}' in championship {championship_id}"
            )

        result: list[dict[str, Any]] = []
        for map_name, data in map_deltas.items():
            result.append(
                {
                    "map_name": map_name,
                    "curr": data["curr"],
                    "prev": data["prev"],
                    "delta": data.get("delta"),
                    "snapshot_ts": data["prev"].get("snapshot_ts") if data.get("prev") else None,
                }
            )
        return result

    # Use season-aware cache
    cache, ttl_seconds = select_season_cache(season)
    if cache is not None:
        cached_value, _ = await cache.get_or_set(cache_key, _compute, ttl_seconds=ttl_seconds)
    else:
        cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def fetch_player_season_progression(
    player_id: str,
    season: int,
    division_num: int,
    *,
    championship_id: str | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            ps.match_id,
            ps.round_index,
            ps.team_id,
            ps.opponent_team_id,
            ps.kills,
            ps.deaths,
            ps.assists,
            ps.mvps,
            ps.headshots,
            ps.sniper_kills,
            ps.pistol_kills,
            ps.knife_kills,
            ps.zeus_kills,
            ps.first_kills,
            ps.enemies_flashed,
            ps.flash_count,
            ps.flash_successes,
            ps.utility_damage,
            ps.utility_count,
            ps.utility_successes,
            ps.utility_enemies,
            ps.mk_2k,
            ps.mk_3k,
            ps.mk_4k,
            ps.mk_5k,
            ps.clutch_kills,
            ps.cl_1v1_attempts,
            ps.cl_1v1_wins,
            ps.cl_1v2_attempts,
            ps.cl_1v2_wins,
            ps.entry_count,
            ps.entry_wins,
            ps.damage,
            m.team1_id AS match_team1_id,
            m.team2_id AS match_team2_id,
            m.winner_team_id,
            m.finished_at,
            c.is_playoffs AS match_is_playoffs,
            ds.created_at AS snapshot_time,
            t_self.name AS team_name,
            t_opp.name AS opponent_team_name,
            t1.name AS team1_name,
            t2.name AS team2_name,
            mp.map_name,
            mp.score_team1,
            mp.score_team2
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        LEFT JOIN championships c ON c.championship_id = m.championship_id
        LEFT JOIN division_snapshots ds
          ON ds.match_id = ps.match_id
         AND ds.season = m.season
         AND ds.division_num = m.division_num
        LEFT JOIN teams t_self ON t_self.team_id = ps.team_id
        LEFT JOIN teams t_opp ON t_opp.team_id = ps.opponent_team_id
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
        LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE ps.player_id = :player_id
          AND ps.season = :season
          AND ps.division_num = :division_num
          {championship_filter}
          AND ps.is_forfeit_map = 0
        ORDER BY
          COALESCE(m.finished_at, ds.created_at) ASC,
          ps.match_id ASC,
          ps.round_index ASC
        """
    params: dict[str, Any] = {"player_id": player_id, "season": season, "division_num": division_num}
    championship_filter = ""
    if championship_id:
        championship_filter = "AND m.championship_id = :championship_id"
        params["championship_id"] = championship_id
    map_rows = await query_async(
        query.format(championship_filter=championship_filter),
        params,
    )
    if not map_rows:
        raise NotFoundError(
            f"No progression snapshots found for player '{player_id}' in season {season} division {division_num}"
        )
    # Diagnostics for investigating missing/duplicated progression points.
    unique_keys: set[tuple[str, int]] = set()
    duplicate_keys = 0
    missing_map_meta = 0
    missing_time = 0
    for row in map_rows:
        key = (str(row.get("match_id") or ""), int(row.get("round_index") or 0))
        if key in unique_keys:
            duplicate_keys += 1
        else:
            unique_keys.add(key)
        if not row.get("map_name") or row.get("score_team1") is None or row.get("score_team2") is None:
            missing_map_meta += 1
        if not row.get("finished_at") and not row.get("snapshot_time"):
            missing_time += 1
    if duplicate_keys or missing_map_meta or missing_time:
        LOGGER.warning(
            "player progression anomalies player=%s season=%s division=%s championship=%s rows=%d unique=%d dup=%d missing_map_meta=%d missing_time=%d",
            player_id,
            season,
            division_num,
            championship_id,
            len(map_rows),
            len(unique_keys),
            duplicate_keys,
            missing_map_meta,
            missing_time,
        )
    else:
        LOGGER.info(
            "player progression source player=%s season=%s division=%s championship=%s rows=%d unique=%d",
            player_id,
            season,
            division_num,
            championship_id,
            len(map_rows),
            len(unique_keys),
        )

    fields = (
        "kills", "deaths", "assists", "mvps", "headshots", "sniper_kills", "pistol_kills",
        "knife_kills", "zeus_kills", "first_kills", "enemies_flashed", "flash_count",
        "flash_successes", "utility_damage", "utility_count", "utility_successes",
        "utility_enemies", "mk_2k", "mk_3k", "mk_4k", "mk_5k", "clutch_kills",
        "cl_1v1_attempts", "cl_1v1_wins", "cl_1v2_attempts", "cl_1v2_wins",
        "entry_count", "entry_wins", "damage",
    )
    totals: dict[str, float] = {field: 0.0 for field in fields}
    maps_played = 0
    rounds_played = 0
    out: list[dict[str, Any]] = []

    for idx, row in enumerate(map_rows, start=1):
        maps_played += 1
        rounds_played += int((row.get("score_team1") or 0) + (row.get("score_team2") or 0))
        for field in fields:
            totals[field] += float(row.get(field) or 0)

        kills = float(totals["kills"])
        deaths = float(totals["deaths"])
        damage = float(totals["damage"])
        headshots = float(totals["headshots"])
        kd = kills / deaths if deaths > 0 else kills
        kr = kills / rounds_played if rounds_played > 0 else 0.0
        adr = damage / rounds_played if rounds_played > 0 else 0.0
        hs_pct = (headshots / kills * 100.0) if kills > 0 else 0.0

        team_id = row.get("team_id")
        winner_team_id = row.get("winner_team_id")
        result = None
        if winner_team_id is not None and team_id is not None:
            result = "win" if str(winner_team_id) == str(team_id) else "loss"

        t1_name = row.get("team1_name")
        t2_name = row.get("team2_name")
        matchup = None
        if t1_name and t2_name:
            matchup = f"{t1_name} vs {t2_name}"
        elif t1_name or t2_name:
            matchup = t1_name or t2_name

        map_name = row.get("map_name") or f"Map {int(row.get('round_index') or idx)}"
        map_score = f"{int(row.get('score_team1') or 0)}:{int(row.get('score_team2') or 0)}"

        out.append(
            {
                "snapshot_ts": idx,
                "snapshot_time": row.get("snapshot_time") or row.get("finished_at"),
                "match_played_at": row.get("finished_at"),
                "round_index": int(row.get("round_index") or 0),
                "match_id": row.get("match_id"),
                "match_team1_id": row.get("match_team1_id"),
                "match_team2_id": row.get("match_team2_id"),
                "team_id": team_id,
                "team_name": row.get("team_name"),
                "opponent_team_id": row.get("opponent_team_id"),
                "opponent_team_name": row.get("opponent_team_name"),
                "matchup": matchup,
                "result": result,
                "match_is_playoffs": bool(row.get("match_is_playoffs")) if row.get("match_is_playoffs") is not None else None,
                "map_names_csv": str(map_name),
                "map_scores_csv": map_score,
                "maps_played": maps_played,
                "rounds_played": rounds_played,
                "kills": int(round(totals["kills"])),
                "deaths": int(round(totals["deaths"])),
                "assists": int(round(totals["assists"])),
                "mvps": int(round(totals["mvps"])),
                "headshots": int(round(totals["headshots"])),
                "sniper_kills": int(round(totals["sniper_kills"])),
                "pistol_kills": int(round(totals["pistol_kills"])),
                "knife_kills": int(round(totals["knife_kills"])),
                "zeus_kills": int(round(totals["zeus_kills"])),
                "first_kills": int(round(totals["first_kills"])),
                "enemies_flashed": int(round(totals["enemies_flashed"])),
                "flash_count": int(round(totals["flash_count"])),
                "flash_successes": int(round(totals["flash_successes"])),
                "utility_damage": int(round(totals["utility_damage"])),
                "utility_count": int(round(totals["utility_count"])),
                "utility_successes": int(round(totals["utility_successes"])),
                "utility_enemies": int(round(totals["utility_enemies"])),
                "mk_2k": int(round(totals["mk_2k"])),
                "mk_3k": int(round(totals["mk_3k"])),
                "mk_4k": int(round(totals["mk_4k"])),
                "mk_5k": int(round(totals["mk_5k"])),
                "clutch_kills": int(round(totals["clutch_kills"])),
                "cl_1v1_attempts": int(round(totals["cl_1v1_attempts"])),
                "cl_1v1_wins": int(round(totals["cl_1v1_wins"])),
                "cl_1v2_attempts": int(round(totals["cl_1v2_attempts"])),
                "cl_1v2_wins": int(round(totals["cl_1v2_wins"])),
                "entry_count": int(round(totals["entry_count"])),
                "entry_wins": int(round(totals["entry_wins"])),
                "kd": float(kd),
                "adr": float(adr),
                "kr": float(kr),
                "hs_pct": float(hs_pct),
                "damage": int(round(totals["damage"])),
            }
        )
    if out:
        last = out[-1]
        LOGGER.info(
            "player progression built player=%s season=%s division=%s championship=%s points=%d maps=%s rounds=%s kills=%s deaths=%s assists=%s mk2=%s mk3=%s mk4=%s mk5=%s",
            player_id,
            season,
            division_num,
            championship_id,
            len(out),
            last.get("maps_played"),
            last.get("rounds_played"),
            last.get("kills"),
            last.get("deaths"),
            last.get("assists"),
            last.get("mk_2k"),
            last.get("mk_3k"),
            last.get("mk_4k"),
            last.get("mk_5k"),
        )
    return out


async def list_players(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    team_id: Optional[str] = None,
    limit: int,
) -> list[dict[str, Any]]:
    if season is not None:
        filters = ["pst.season = :season"]
        query = """
            SELECT DISTINCT
                p.player_id,
                (
                    SELECT pcx.championship_id
                    FROM player_championships pcx
                    JOIN championships cx ON cx.championship_id = pcx.championship_id
                    WHERE pcx.player_id = p.player_id
                      AND cx.season = :season
                      {division_filter_subquery}
                    ORDER BY pcx.updated_at DESC, pcx.created_at DESC
                    LIMIT 1
                ) AS championship_id,
                COALESCE(pc.player_name, p.nickname) AS nickname,
                p.avatar,
                p.faceit_url
            FROM players p
            JOIN player_season_totals pst ON pst.player_id = p.player_id
            LEFT JOIN championships c
                ON c.season = pst.season
               AND c.division_num = pst.division_num
               AND COALESCE(c.is_playoffs, 0) = 0
            LEFT JOIN player_championships pc
                ON pc.player_id = p.player_id
               AND pc.championship_id = c.championship_id
            WHERE {where_clause}
        """
        params: Dict[str, Any] = {"season": season, "limit": limit}
        if division is not None:
            filters.append("pst.division_num = :division")
            params["division"] = division
        if team_id:
            filters.append("pst.team_id = :team_id")
            params["team_id"] = team_id
        division_filter_subquery = "AND cx.division_num = :division" if division is not None else ""
        query = query.format(
            where_clause=" AND ".join(filters),
            division_filter_subquery=division_filter_subquery,
        )
        query += " ORDER BY nickname LIMIT :limit"
        try:
            rows = await query_async(query, params)
        except Exception:
            fallback_query = """
                SELECT DISTINCT
                    p.player_id,
                    (
                        SELECT pcx.championship_id
                        FROM player_championships pcx
                        JOIN championships cx ON cx.championship_id = pcx.championship_id
                        WHERE pcx.player_id = p.player_id
                          AND cx.season = :season
                          {division_filter_subquery}
                        ORDER BY pcx.updated_at DESC, pcx.created_at DESC
                        LIMIT 1
                    ) AS championship_id,
                    COALESCE(pc.player_name, p.nickname) AS nickname
                FROM players p
                JOIN player_season_totals pst ON pst.player_id = p.player_id
                LEFT JOIN championships c
                    ON c.season = pst.season
                   AND c.division_num = pst.division_num
                   AND COALESCE(c.is_playoffs, 0) = 0
                LEFT JOIN player_championships pc
                    ON pc.player_id = p.player_id
                   AND pc.championship_id = c.championship_id
                WHERE {where_clause}
            """
            fallback_query = fallback_query.format(
                where_clause=" AND ".join(filters),
                division_filter_subquery=division_filter_subquery,
            )
            fallback_query += " ORDER BY nickname LIMIT :limit"
            rows = await query_async(fallback_query, params)
    else:
        try:
            rows = await query_async(
                """
                SELECT
                    p.player_id,
                    (
                        SELECT pc.championship_id
                        FROM player_championships pc
                        JOIN championships c ON c.championship_id = pc.championship_id
                        WHERE pc.player_id = p.player_id
                        ORDER BY c.season DESC, c.is_playoffs ASC, pc.updated_at DESC, pc.created_at DESC
                        LIMIT 1
                    ) AS championship_id,
                    p.nickname,
                    p.avatar,
                    p.faceit_url
                FROM players p
                ORDER BY nickname
                LIMIT :limit
                """,
                {"limit": limit},
            )
        except Exception:
            rows = await query_async(
                """
                SELECT
                    p.player_id,
                    (
                        SELECT pc.championship_id
                        FROM player_championships pc
                        JOIN championships c ON c.championship_id = pc.championship_id
                        WHERE pc.player_id = p.player_id
                        ORDER BY c.season DESC, c.is_playoffs ASC, pc.updated_at DESC, pc.created_at DESC
                        LIMIT 1
                    ) AS championship_id,
                    p.nickname
                FROM players p
                ORDER BY nickname
                LIMIT :limit
                """,
                {"limit": limit},
            )

    for row in rows:
        row.setdefault("avatar", DEFAULT_AVATAR)
        row.setdefault("faceit_url", None)
        if not row.get("nickname"):
            row["nickname"] = str(row.get("player_id") or "")
    return rows
