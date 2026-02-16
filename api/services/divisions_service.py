from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Sequence

from db_async import (
    build_played_match_condition,
    count_played_matches,
    count_played_matches_by_championship_ids,
    count_total_matches_by_championship_ids,
    query_async,
)
from division_overrides import combined_status_teams
from division_naming import build_division_name

from api.exceptions import NotFoundError
from api.services.player_counts import get_player_counts
from api.services.season_aggregates import dedupe_team_total
from api.services.cache_helpers import (
    GLOBAL_CACHE,
    get_championship_revision,
    get_global_revision,
    get_season_revision,
    select_season_cache,
)

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


def _apply_division_name(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    is_playoff = data.get("is_playoff")
    if is_playoff is None:
        is_playoff = data.get("is_playoffs")
    data["name"] = build_division_name(data.get("season"), data.get("division_num"), is_playoff)
    return data


def get_excluded_team_ids(championship_id: str) -> set[str]:
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}


async def fetch_seasons() -> List[dict[str, Any]]:
    revision = await get_global_revision()
    cache_key = ("fetch_seasons", revision)

    async def _compute():
        rows = await query_async(
            """
            SELECT DISTINCT season, division_num, championship_id
            FROM championships
            ORDER BY season DESC, division_num
            """
        )

        seasons_map: dict[int, dict[str, Any]] = {}
        for row in rows:
            season = int(row["season"])
            entry = seasons_map.setdefault(
                season,
                {"season": season, "divisions": [], "championship_ids": []},
            )
            entry["divisions"].append(int(row["division_num"]))
            entry["championship_ids"].append(row["championship_id"])
        return list(seasons_map.values())

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def list_divisions(limit: int, offset: int) -> List[dict[str, Any]]:
    rows = await query_async(
        """
        SELECT championship_id, slug, name, season, division_num,
               is_playoffs AS is_playoff, parent_championship_id
        FROM championships
        ORDER BY season DESC, division_num, is_playoffs
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )
    return [_apply_division_name(row) for row in rows]


async def count_divisions(season: Optional[int] = None) -> int:
    if season is None:
        rows = await query_async("SELECT COUNT(*) AS total FROM championships")
    else:
        rows = await query_async(
            "SELECT COUNT(*) AS total FROM championships WHERE season = :season",
            {"season": season},
        )
    return int(rows[0].get("total") or 0) if rows else 0


async def list_divisions_by_season(season: int, limit: int, offset: int) -> List[dict[str, Any]]:
    cache, ttl_seconds = select_season_cache(season)
    revision = await get_season_revision(season)
    cache_key = ("list_divisions_by_season", season, limit, offset, revision)

    async def _compute():
        rows = await query_async(
            """
            WITH team_counts AS (
                SELECT championship_id, COUNT(DISTINCT team_id) AS teams_count
                FROM (
                    SELECT championship_id, team1_id AS team_id
                    FROM matches
                    WHERE team1_id IS NOT NULL
                    UNION ALL
                    SELECT championship_id, team2_id AS team_id
                    FROM matches
                    WHERE team2_id IS NOT NULL
                ) team_lookup
            GROUP BY championship_id
        )
        SELECT
            c.championship_id,
            c.slug,
            c.name,
            c.season,
            c.division_num,
            c.is_playoffs AS is_playoff,
            c.parent_championship_id,
            COALESCE(tc.teams_count, 0) AS teams_count,
            SUM(
                CASE
                    WHEN UPPER(COALESCE(m.status, '')) IN ('LIVE', 'ONGOING', 'IN_PROGRESS', 'STARTED')
                        THEN 1
                    ELSE 0
                END
            ) AS live_matches,
            SUM(
                CASE
                    WHEN UPPER(COALESCE(m.status, '')) IN ('CONFIGURED', 'PENDING', 'READY', 'SCHEDULED')
                        THEN 1
                    ELSE 0
                END
            ) AS upcoming_matches,
            MIN(NULLIF(m.started_at, 0)) AS first_started_at,
            MIN(NULLIF(m.scheduled_at, 0)) AS first_scheduled_at,
            MAX(NULLIF(m.finished_at, 0)) AS last_finished_at,
            MAX(NULLIF(m.scheduled_at, 0)) AS last_scheduled_at,
            MAX(NULLIF(m.activity_ts, 0)) AS last_activity_ts,
            MAX(m.updated_at) AS last_updated
        FROM championships c
        LEFT JOIN matches m ON c.championship_id = m.championship_id
        LEFT JOIN team_counts tc ON tc.championship_id = c.championship_id
        WHERE c.season = :season
        GROUP BY c.championship_id, c.slug, c.name, c.season, c.division_num, c.is_playoffs, c.parent_championship_id, tc.teams_count
        ORDER BY c.division_num ASC
        LIMIT :limit OFFSET :offset
        """,
            {"season": season, "limit": limit, "offset": offset},
        )
        champ_ids = [str(row["championship_id"]) for row in rows]
        played_map = await count_played_matches_by_championship_ids(
            championship_ids=champ_ids,
            include_forfeits=True,
            include_ignored=True,
        )
        total_map = await count_total_matches_by_championship_ids(
            championship_ids=champ_ids,
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            champ_id = str(row["championship_id"])
            played = int(played_map.get(champ_id, 0))
            data = dict(row)
            data["played_matches"] = played
            data["finished_matches"] = played
            data["total_matches"] = int(total_map.get(champ_id, 0))
            result.append(_apply_division_name(data))
        return result

    cached_value, _ = await cache.get_or_set(cache_key, _compute, ttl_seconds=ttl_seconds)
    return cached_value


async def _fetch_champ_row(where_clause: str, params: dict[str, Any]) -> dict[str, Any]:
    rows = await query_async(
        f"""
        SELECT championship_id, slug, name, season, division_num,
            is_playoffs AS is_playoff, parent_championship_id
        FROM championships
        WHERE {where_clause}
        LIMIT 1
        """,
        params,
    )
    if not rows:
        raise NotFoundError("Division not found")
    return _apply_division_name(rows[0])


async def fetch_division_by_slug(slug: str) -> dict[str, Any]:
    return await _fetch_champ_row("slug = :slug", {"slug": slug})


async def fetch_division_by_id(championship_id: str) -> dict[str, Any]:
    revision = await get_championship_revision(championship_id)
    cache_key = ("fetch_division_by_id", championship_id, revision)

    async def _compute():
        return await _fetch_champ_row("championship_id = :champ_id", {"champ_id": championship_id})

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def get_division_details(champ: dict[str, Any]) -> dict[str, Any]:
    championship_id = champ["championship_id"]
    season = int(champ["season"])
    division_num = int(champ["division_num"])

    cache, ttl_seconds = select_season_cache(season)
    revision = await get_championship_revision(championship_id)
    cache_key = ("get_division_details", championship_id, revision)

    async def _compute():
        return await _compute_division_details(championship_id, season, division_num, champ)

    cached_value, _ = await cache.get_or_set(cache_key, _compute, ttl_seconds=ttl_seconds)
    return cached_value


async def _compute_division_details(championship_id: str, season: int, division_num: int, champ: dict[str, Any]) -> dict[str, Any]:
    played_condition = build_played_match_condition(
        alias="dmt",
        include_forfeits=True,
        include_ignored=True,
    )
    team_rows = await query_async(
        """
        WITH division_matches AS (
            SELECT match_id, team1_id, team2_id, winner_team_id, finished_at
            FROM matches
            WHERE championship_id = :champ_id
        ),
        division_match_teams AS (
            SELECT match_id, team1_id AS team_id, winner_team_id, finished_at FROM division_matches
            UNION ALL
            SELECT match_id, team2_id AS team_id, winner_team_id, finished_at FROM division_matches
        ),
        division_maps AS (
            SELECT
                mp.map_id,
                mp.match_id,
                mp.map_name,
                mp.score_team1,
                mp.score_team2,
                mp.winner_team_id,
                m.team1_id,
                m.team2_id
            FROM maps mp
            JOIN division_matches m ON m.match_id = mp.match_id
            WHERE COALESCE(mp.is_forfeit, 0) = 0
        ),
        team_map_rows AS (
            SELECT
                dm.match_id,
                dm.map_id,
                dm.map_name,
                dm.winner_team_id,
                dm.score_team1 AS score_for,
                dm.score_team2 AS score_against,
                dm.score_team1 + dm.score_team2 AS rounds_played,
                dm.team1_id AS team_id
            FROM division_maps dm
            UNION ALL
            SELECT
                dm.match_id,
                dm.map_id,
                dm.map_name,
                dm.winner_team_id,
                dm.score_team2,
                dm.score_team1,
                dm.score_team1 + dm.score_team2,
                dm.team2_id
            FROM division_maps dm
        ),
        match_totals AS (
            SELECT
                dmt.team_id,
                COUNT(DISTINCT dmt.match_id) AS matches_played,
                SUM(CASE WHEN dmt.winner_team_id = dmt.team_id THEN 1 ELSE 0 END) AS matches_won
            FROM division_match_teams dmt
            WHERE dmt.team_id IS NOT NULL
                AND {played_condition}
            GROUP BY dmt.team_id
        ),
        map_totals AS (
            SELECT
                team_id,
                COUNT(*) AS maps_played,
                SUM(CASE WHEN winner_team_id = team_id THEN 1 ELSE 0 END) AS maps_won,
                SUM(score_for) AS rounds_won,
                SUM(score_against) AS rounds_lost
            FROM team_map_rows
            WHERE team_id IS NOT NULL
            GROUP BY team_id
        ),
        player_totals AS (
            SELECT
                                ps.team_id,
                                SUM(ps.kills) AS kills,
                                SUM(ps.deaths) AS deaths,
                                SUM(ps.damage) AS damage
                        FROM player_stats ps
                        JOIN matches m ON m.match_id = ps.match_id
                        WHERE m.championship_id = :champ_id
                            AND COALESCE(ps.is_forfeit_map, 0) = 0
                            AND ps.team_id IS NOT NULL
                        GROUP BY ps.team_id
        ),
        division_teams AS (
            SELECT DISTINCT team1_id AS team_id FROM division_matches WHERE team1_id IS NOT NULL
            UNION
            SELECT DISTINCT team2_id AS team_id FROM division_matches WHERE team2_id IS NOT NULL
            UNION
            SELECT team_id FROM team_championships WHERE championship_id = :champ_id
        )
        SELECT DISTINCT t.team_id,
               COALESCE(tc.team_name, t.name) AS team_name,
               COALESCE(tc.team_name, t.name) AS display_name,
               t.avatar,
               COALESCE(mt.matches_played, 0) AS matches_played,
               COALESCE(mt.matches_won, 0) AS matches_won,
               COALESCE(mp.maps_played, 0) AS maps_played,
               COALESCE(mp.maps_won, 0) AS maps_won,
               COALESCE(mp.rounds_won, 0) AS rounds_won,
               COALESCE(mp.rounds_lost, 0) AS rounds_lost,
               COALESCE(agg.kills, 0) AS kills,
               COALESCE(agg.deaths, 0) AS deaths,
               COALESCE(agg.damage, 0) AS damage
        FROM division_teams dt
        JOIN teams t ON t.team_id = dt.team_id
        LEFT JOIN team_championships tc ON tc.team_id = t.team_id AND tc.championship_id = :champ_id
        LEFT JOIN match_totals mt ON mt.team_id = t.team_id
        LEFT JOIN map_totals mp ON mp.team_id = t.team_id
        LEFT JOIN player_totals agg ON agg.team_id = t.team_id
        ORDER BY team_name, team_id
        """.format(played_condition=played_condition),
        {"champ_id": championship_id},
    )

    excluded = get_excluded_team_ids(championship_id)

    player_rows = await query_async(
        """
        WITH division_maps AS (
            SELECT
                mp.match_id,
                mp.round_index,
                COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0) AS rounds_played
            FROM maps mp
            JOIN matches m ON m.match_id = mp.match_id
            WHERE m.championship_id = :champ_id
              AND COALESCE(mp.is_forfeit, 0) = 0
        )
        SELECT
            ps.team_id,
            ps.player_id,
            COUNT(DISTINCT CONCAT(ps.match_id, ':', ps.round_index)) AS maps_played,
            SUM(COALESCE(dm.rounds_played, 0)) AS rounds_played,
            SUM(ps.kills) AS kills,
            SUM(ps.deaths) AS deaths,
            COALESCE(pc.player_name, p.nickname) AS nickname
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        LEFT JOIN division_maps dm ON dm.match_id = ps.match_id AND dm.round_index = ps.round_index
        JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN player_championships pc ON pc.player_id = ps.player_id AND pc.championship_id = :champ_id
        WHERE m.championship_id = :champ_id
          AND COALESCE(ps.is_forfeit_map, 0) = 0
          AND ps.team_id IS NOT NULL
        GROUP BY ps.team_id, ps.player_id, COALESCE(pc.player_name, p.nickname)
        """,
        {"champ_id": championship_id},
    )

    players_by_team: dict[str, list[dict[str, Any]]] = {}
    unique_player_ids: set[str] = set()
    for prow in player_rows:
        team_id = prow.get("team_id")
        if not team_id:
            continue
        player_id = str(prow.get("player_id") or "")
        if player_id:
            unique_player_ids.add(player_id)
        players_by_team.setdefault(team_id, []).append(
            {
                "player_id": prow.get("player_id"),
                "nickname": prow.get("nickname"),
                "maps_played": int(prow.get("maps_played") or 0),
                "rounds_played": int(prow.get("rounds_played") or 0),
                "kills": int(prow.get("kills") or 0),
                "deaths": int(prow.get("deaths") or 0),
            }
        )

    for plist in players_by_team.values():
        plist.sort(key=lambda p: (p.get("nickname") or "").lower())

    player_totals_params: dict[str, Any] = {"season": season, "division": division_num}
    exclusion_clause = ""
    if excluded:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded)))
        exclusion_clause = f" AND (pst.team_id IS NULL OR pst.team_id NOT IN ({placeholders}))"
        for i, team_id in enumerate(excluded):
            player_totals_params[f"ex{i}"] = team_id

    player_totals_rows = await query_async(
        f"""
        SELECT
            pst.player_id,
            pst.team_id,
            pst.maps_played,
            pst.rounds_played,
            pst.kills,
            pst.deaths,
            pst.assists,
            pst.mvps,
            pst.sniper_kills,
            pst.utility_damage,
            pst.enemies_flashed,
            pst.flash_count,
            pst.flash_successes,
            pst.clutch_kills,
            pst.cl_1v1_attempts,
            pst.cl_1v1_wins,
            pst.cl_1v2_attempts,
            pst.cl_1v2_wins,
            pst.pistol_kills,
            pst.knife_kills,
            pst.zeus_kills,
            pst.adr,
            pst.kr,
            pst.kd,
            pst.hs_pct,
            pst.damage,
            COALESCE(pc.player_name, p.nickname) AS nickname,
            t.name AS team_name
        FROM player_season_totals pst
        LEFT JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN player_championships pc ON pc.player_id = pst.player_id AND pc.championship_id = :champ_id
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE pst.season = :season
          AND pst.division_num = :division
          {exclusion_clause}
        """,
        {**player_totals_params, "champ_id": championship_id},
    )

    player_totals: list[dict[str, Any]] = []
    for row in player_totals_rows:
        player_id = row.get("player_id")
        if player_id:
            unique_player_ids.add(str(player_id))
        player_totals.append(
            {
                "player_id": row.get("player_id"),
                "team_id": row.get("team_id"),
                "team_name": row.get("team_name"),
                "nickname": row.get("nickname"),
                "avatar": DEFAULT_AVATAR,
                "maps_played": int(row.get("maps_played") or 0),
                "rounds_played": int(row.get("rounds_played") or 0),
                "kills": int(row.get("kills") or 0),
                "deaths": int(row.get("deaths") or 0),
                "assists": int(row.get("assists") or 0),
                "mvps": int(row.get("mvps") or 0),
                "sniper_kills": int(row.get("sniper_kills") or 0),
                "knife_kills": int(row.get("knife_kills") or 0),
                "zeus_kills": int(row.get("zeus_kills") or 0),
                "utility_damage": int(row.get("utility_damage") or 0),
                "enemies_flashed": int(row.get("enemies_flashed") or 0),
                "flash_count": int(row.get("flash_count") or 0),
                "flash_successes": int(row.get("flash_successes") or 0),
                "clutch_kills": int(row.get("clutch_kills") or 0),
                "cl_1v1_attempts": int(row.get("cl_1v1_attempts") or 0),
                "cl_1v1_wins": int(row.get("cl_1v1_wins") or 0),
                "cl_1v2_attempts": int(row.get("cl_1v2_attempts") or 0),
                "cl_1v2_wins": int(row.get("cl_1v2_wins") or 0),
                "pistol_kills": int(row.get("pistol_kills") or 0),
                "adr": float(row.get("adr") or 0.0),
                "kr": float(row.get("kr") or 0.0),
                "kd": float(row.get("kd") or 0.0),
                "hs_pct": float(row.get("hs_pct") or 0.0),
                "damage": int(row.get("damage") or 0),
            }
        )

    teams: list[dict[str, Any]] = []
    for t in team_rows:
        matches_played = int(t.get("matches_played") or 0)
        matches_won = int(t.get("matches_won") or 0)
        matches_lost = max(matches_played - matches_won, 0)
        maps_played = int(t.get("maps_played") or 0)
        maps_won = int(t.get("maps_won") or 0)
        maps_lost = max(maps_played - maps_won, 0)
        rounds_won = int(t.get("rounds_won") or 0)
        rounds_lost = int(t.get("rounds_lost") or 0)
        rounds_diff = rounds_won - rounds_lost
        kills = int(t.get("kills") or 0)
        deaths = int(t.get("deaths") or 0)
        damage = int(t.get("damage") or 0)
        total_rounds = rounds_won + rounds_lost

        kd = kills / deaths if deaths else (float(kills) if kills else 0.0)
        adr = damage / total_rounds if total_rounds else 0.0
        match_win_rate = (matches_won / matches_played * 100) if matches_played else 0.0
        map_win_rate = (maps_won / maps_played * 100) if maps_played else 0.0

        teams.append(
            {
                "team_id": t["team_id"],
                "team_name": t["team_name"],
                "display_name": t.get("display_name"),
                "avatar": t.get("avatar") or DEFAULT_AVATAR,
                "matches_played": matches_played,
                "matches_won": matches_won,
                "matches_lost": matches_lost,
                "wins": matches_won,
                "losses": matches_lost,
                "win_rate": round(map_win_rate, 1),
                "match_win_rate": round(match_win_rate, 1),
                "maps_played": maps_played,
                "maps_won": maps_won,
                "maps_lost": maps_lost,
                "rounds_won": rounds_won,
                "rounds_lost": rounds_lost,
                "rounds_diff": rounds_diff,
                "kills": kills,
                "deaths": deaths,
                "kd": round(kd, 2) if isinstance(kd, float) else kd,
                "adr": round(adr, 1),
                "damage": damage,
                "players": players_by_team.get(t["team_id"], []),
            }
        )

    player_counts = await get_player_counts(
        season=season,
        division=division_num,
        include_all_time=True,
    )
    division_player_count = len(unique_player_ids)

    map_stats_task = asyncio.create_task(_get_division_map_stats(championship_id, season, division_num))
    aggregates_task = asyncio.create_task(
        _get_division_aggregates(championship_id, season, division_num, bool(champ["is_playoff"]))
    )
    leaders_task = asyncio.create_task(
        _get_division_leaders(championship_id, season, division_num, bool(champ["is_playoff"]))
    )

    map_stats, aggregates, leaders = await asyncio.gather(
        map_stats_task,
        aggregates_task,
        leaders_task,
    )

    # Ensure aggregate fields the frontend expects are present
    if aggregates is None:
        aggregates = {}
    if map_stats and not aggregates.get("maps_played_total"):
        aggregates["maps_played_total"] = sum(int(item.get("maps_played") or 0) for item in map_stats)
    if aggregates.get("played_matches") is not None and aggregates.get("matches_played") is None:
        aggregates["matches_played"] = aggregates["played_matches"]
    if aggregates.get("total_matches") is None and aggregates.get("matches_played") is not None:
        aggregates["total_matches"] = aggregates["matches_played"]

    return {
        "championship_id": championship_id,
        "slug": champ["slug"],
        "name": champ["name"],
        "season": champ["season"],
        "division_num": champ["division_num"],
        "is_playoff": bool(champ["is_playoff"]),
        "teams": teams,
        "excluded_team_ids": list(excluded),
        "map_stats": map_stats,
        "aggregates": aggregates,
        "leaders": leaders,
        "player_totals": player_totals,
        "player_count": int(division_player_count or 0),
        "season_player_count": int(player_counts.get("season_players") or 0),
        "all_time_player_count": int(player_counts.get("all_time_players") or 0),
    }


async def _get_division_map_stats(championship_id: str, season: int, division_num: int) -> List[Dict[str, Any]]:
    primary_rows = await query_async(
        """
        WITH division_matches AS (
            SELECT match_id
        FROM matches
        WHERE championship_id = :champ_id
        ),
        division_maps AS (
            SELECT
                m.map_id,
                m.map_name,
                COALESCE(m.score_team1, 0) AS score_team1,
                COALESCE(m.score_team2, 0) AS score_team2
            FROM maps m
            JOIN division_matches dm ON dm.match_id = m.match_id
            WHERE m.map_name IS NOT NULL
              AND m.is_forfeit = 0
        ),
        player_totals AS (
            SELECT
                LOWER(dm.map_name) AS map_key,
                dm.map_name,
                COUNT(DISTINCT dm.map_id) AS maps_played,
                SUM(ps.kills) AS kills,
                SUM(ps.deaths) AS deaths,
                SUM(ps.damage) AS damage,
                AVG(ps.adr) AS adr,
                AVG(ps.kr) AS kr,
                SUM(ps.utility_damage) AS utility_damage,
                SUM(ps.enemies_flashed) AS enemies_flashed,
                SUM(ps.flash_count) AS flash_count,
                SUM(ps.sniper_kills) AS sniper_kills,
                SUM(ps.assists) AS assists,
                SUM(ps.mk_2k) AS k2,
                SUM(ps.mk_3k) AS k3,
                SUM(ps.mk_4k) AS k4,
                SUM(ps.mk_5k) AS ace,
                SUM(ps.pistol_kills) AS pistol_kills
            FROM division_maps dm
            LEFT JOIN player_stats ps ON (
                ps.map_id = dm.map_id
                AND ps.is_forfeit_map = 0
            )
            GROUP BY LOWER(dm.map_name), dm.map_name
        ),
        round_totals AS (
            SELECT
                LOWER(dm.map_name) AS map_key,
                SUM(dm.score_team1 + dm.score_team2) AS rounds_played
            FROM division_maps dm
            GROUP BY LOWER(dm.map_name)
        ),
        map_vote_totals AS (
            SELECT
                LOWER(v.map_name) AS map_key,
                COUNT(*) AS banned
            FROM map_votes v
            JOIN division_matches dm ON dm.match_id = v.match_id
            WHERE v.map_name IS NOT NULL
              AND LOWER(v.status) IN ('banned','ban','drop','removed','remove','veto')
            GROUP BY LOWER(v.map_name)
        )
        SELECT
            pt.map_name,
            mc.pretty_name,
            mc.image_sm,
            pt.maps_played,
            COALESCE(mvt.banned, 0) AS banned,
            COALESCE(pt.kills, 0) AS kills,
            COALESCE(pt.deaths, 0) AS deaths,
            COALESCE(pt.damage, 0) AS damage,
            COALESCE(rt.rounds_played, 0) AS rounds_played,
            COALESCE(pt.adr, 0) AS adr,
            COALESCE(pt.kr, 0) AS kr,
            CASE WHEN COALESCE(pt.deaths, 0) = 0 THEN 0 ELSE COALESCE(pt.utility_damage, 0) / NULLIF(pt.deaths, 0) END AS udpr,
            CASE WHEN COALESCE(pt.flash_count, 0) = 0 THEN 0 ELSE COALESCE(pt.enemies_flashed, 0) / NULLIF(pt.flash_count, 0) END AS enemy_flash,
            COALESCE(pt.sniper_kills, 0) AS sniper_kills,
            COALESCE(pt.assists, 0) AS assists,
            COALESCE(pt.k2, 0) AS k2,
            COALESCE(pt.k3, 0) AS k3,
            COALESCE(pt.k4, 0) AS k4,
            COALESCE(pt.ace, 0) AS ace,
            COALESCE(pt.pistol_kills, 0) AS pistol_kills
        FROM player_totals pt
        LEFT JOIN round_totals rt ON rt.map_key = pt.map_key
        LEFT JOIN map_vote_totals mvt ON mvt.map_key = pt.map_key
        LEFT JOIN maps_catalog mc ON mc.map_id = pt.map_name
        ORDER BY pt.maps_played DESC, pt.map_name
        """,
        {"champ_id": championship_id},
    )

    def _build_result(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for r in rows:
            maps_played = int(r.get("maps_played") or 0)
            rounds_played = int(r.get("rounds_played") or 0)
            kills = int(r.get("kills") or 0)
            deaths = int(r.get("deaths") or 0)
            damage = int(r.get("damage") or 0)
            adr = float(r.get("adr") or 0.0)
            kr = float(r.get("kr") or 0.0)
            utility_damage = float(r.get("utility_damage") or 0.0)
            snipers = int(r.get("sniper_kills") or 0)
            assists = int(r.get("assists") or 0)
            bans = int(r.get("banned") or 0)
            result.append(
                {
                    "map_name": r.get("map_name") or r.get("map_id") or "Unknown",
                    "pretty_name": r.get("pretty_name") or r.get("map_name") or r.get("map_id") or "Kartta",
                    "image_sm": r.get("image_sm"),
                    "maps_played": maps_played,
                    "banned": bans,
                    "kills": kills,
                    "deaths": deaths,
                    "damage": damage,
                    "rounds_played": rounds_played,
                    "adr": adr,
                    "kr": kr,
                    "udpr": float(r.get("udpr") or (utility_damage / max(deaths, 1))),
                    "enemy_flash": float(r.get("enemy_flash") or 0.0),
                    "sniper_kills": snipers,
                    "assists": assists,
                    "k2": int(r.get("k2") or 0),
                    "k3": int(r.get("k3") or 0),
                    "k4": int(r.get("k4") or 0),
                    "ace": int(r.get("ace") or 0),
                    "pistol_kills": int(r.get("pistol_kills") or 0),
                    "pick_rate": round((maps_played / max(rounds_played, 1)) * 100, 1) if rounds_played else 0.0,
                }
            )
        return result

    if primary_rows:
        return _build_result(primary_rows)

    return []


async def _get_division_aggregates(
    championship_id: str,
    season: int,
    division_num: int,
    is_playoff: bool,
) -> dict[str, Any]:
    rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT m.match_id) AS total_matches,
            SUM(CASE WHEN m.is_forfeit = 1 THEN 1 ELSE 0 END) AS forfeits
        FROM matches m
        WHERE m.championship_id = :champ_id
        """,
        {"champ_id": championship_id},
    )
    row = rows[0] if rows else {}

    map_totals = await query_async(
        """
        SELECT
          COUNT(*) AS maps_played_total,
          COALESCE(SUM(CASE WHEN mp.is_forfeit = 0 THEN (COALESCE(mp.score_team1,0) + COALESCE(mp.score_team2,0)) ELSE 0 END),0) AS rounds_played_total
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = :champ_id
          AND mp.map_name IS NOT NULL
          AND COALESCE(mp.is_forfeit, 0) = 0
        """,
        {"champ_id": championship_id},
    )
    map_total_row = map_totals[0] if map_totals else {}
    maps_played_total = int(map_total_row.get("maps_played_total") or 0)
    rounds_played_total = int(map_total_row.get("rounds_played_total") or 0)

    player_totals = {}
    if is_playoff:
        player_rows = await query_async(
            """
            SELECT
                COUNT(DISTINCT ps.player_id) AS player_count,
                COALESCE(SUM(CASE WHEN ps.is_forfeit_map = 0 THEN ps.kills ELSE 0 END), 0) AS total_kills,
                COALESCE(SUM(CASE WHEN ps.is_forfeit_map = 0 THEN ps.deaths ELSE 0 END), 0) AS total_deaths
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            WHERE m.championship_id = :champ_id
              AND m.ignored_due_ban = 0
            """,
            {"champ_id": championship_id},
        )
        player_totals = player_rows[0] if player_rows else {}
    else:
        player_rows = await query_async(
            """
            SELECT
                COUNT(DISTINCT pst.player_id) AS player_count,
                COALESCE(SUM(pst.kills), 0) AS total_kills,
                COALESCE(SUM(pst.deaths), 0) AS total_deaths
            FROM player_season_totals pst
            WHERE pst.season = :season AND pst.division_num = :division
            """,
            {"season": season, "division": division_num},
        )
        player_totals = player_rows[0] if player_rows else {}

    team_rounds_total = 0
    if not is_playoff:
        team_rows = await query_async(
            """
            SELECT COALESCE(SUM(rounds_won + rounds_lost), 0) AS rounds_total
            FROM team_season_totals
            WHERE season = :season AND division_num = :division
            """,
            {"season": season, "division": division_num},
        )
        team_rounds_total = int((team_rows[0] or {}).get("rounds_total") or 0) if team_rows else 0

    if rounds_played_total == 0 and team_rounds_total:
        rounds_played_total = dedupe_team_total(team_rounds_total)

    played = await count_played_matches(
        championship_id=championship_id,
        include_forfeits=True,
        include_ignored=True,
    )
    played = int(played or 0)
    total = int(row.get("total_matches") or 0)
    forfeits = int(row.get("forfeits") or 0)

    return {
        "played_matches": played,
        "matches_played": played,
        "total_matches": total,
        "forfeits": forfeits,
        "maps_played_total": maps_played_total,
        "rounds_played_total": rounds_played_total,
        "total_kills": int(player_totals.get("total_kills") or 0),
        "total_deaths": int(player_totals.get("total_deaths") or 0),
        "player_count": int(player_totals.get("player_count") or 0),
    }


async def _get_division_leaders(
    championship_id: str,
    season: int,
    division_num: int,
    is_playoff: bool,
) -> List[Dict[str, Any]]:
    if not is_playoff:
        rows = await query_async(
            """
            SELECT
                pst.player_id,
                pst.team_id,
                pst.kills,
                pst.deaths,
                pst.adr,
                pst.kr,
                pst.kd,
                pst.mvps,
                pst.utility_damage,
                COALESCE(pc.player_name, p.nickname) AS nickname,
                t.name AS team_name
            FROM player_season_totals pst
            LEFT JOIN players p ON p.player_id = pst.player_id
            LEFT JOIN player_championships pc ON pc.player_id = pst.player_id AND pc.championship_id = :champ_id
            LEFT JOIN teams t ON t.team_id = pst.team_id
            WHERE pst.season = :season AND pst.division_num = :division
            ORDER BY pst.kills DESC, pst.adr DESC
            LIMIT 10
            """,
            {"season": season, "division": division_num, "champ_id": championship_id},
        )
        leaders = []
        for row in rows:
            leaders.append(
                {
                    "player_id": row.get("player_id"),
                    "team_id": row.get("team_id"),
                    "team_name": row.get("team_name"),
                    "nickname": row.get("nickname"),
                    "kills": int(row.get("kills") or 0),
                    "deaths": int(row.get("deaths") or 0),
                    "adr": float(row.get("adr") or 0.0),
                    "kr": float(row.get("kr") or 0.0),
                    "kd": float(row.get("kd") or 0.0),
                    "mvps": int(row.get("mvps") or 0),
                    "utility_damage": int(row.get("utility_damage") or 0),
                }
            )
        return leaders

    rows = await query_async(
        """
        WITH player_totals AS (
            SELECT
                ps.player_id,
                ps.team_id,
                SUM(ps.kills) AS kills,
                SUM(ps.deaths) AS deaths,
                AVG(ps.adr) AS adr,
                AVG(ps.kr) AS kr,
                AVG(ps.kd) AS kd,
                SUM(ps.mvps) AS mvps,
                SUM(ps.utility_damage) AS utility_damage
            FROM player_stats ps
            JOIN matches m ON m.match_id = ps.match_id
            WHERE m.championship_id = :champ_id
              AND COALESCE(ps.is_forfeit_map, 0) = 0
            GROUP BY ps.player_id, ps.team_id
        )
        SELECT
            pt.player_id,
            pt.team_id,
            pt.kills,
            pt.deaths,
            pt.adr,
            pt.kr,
            pt.kd,
            pt.mvps,
            pt.utility_damage,
            COALESCE(pc.player_name, p.nickname) AS nickname,
            t.name AS team_name
        FROM player_totals pt
        LEFT JOIN players p ON p.player_id = pt.player_id
        LEFT JOIN player_championships pc ON pc.player_id = pt.player_id AND pc.championship_id = :champ_id
        LEFT JOIN teams t ON t.team_id = pt.team_id
        ORDER BY pt.kills DESC, pt.adr DESC
        LIMIT 10
        """,
        {"champ_id": championship_id},
    )
    leaders = []
    for row in rows:
        leaders.append(
            {
                "player_id": row.get("player_id"),
                "team_id": row.get("team_id"),
                "team_name": row.get("team_name"),
                "nickname": row.get("nickname"),
                "kills": int(row.get("kills") or 0),
                "deaths": int(row.get("deaths") or 0),
                "adr": float(row.get("adr") or 0.0),
                "kr": float(row.get("kr") or 0.0),
                "kd": float(row.get("kd") or 0.0),
                "mvps": int(row.get("mvps") or 0),
                "utility_damage": int(row.get("utility_damage") or 0),
            }
        )
    return leaders


async def fetch_division_winners(championship_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not championship_ids:
        return {}

    placeholders = ", ".join(f":champ{idx}" for idx in range(len(championship_ids)))
    params = {f"champ{idx}": str(cid) for idx, cid in enumerate(championship_ids)}

    rows = await query_async(
        f"""
        SELECT
            c.championship_id,
            tst.team_id,
            t.name AS team_name,
            COALESCE(tst.matches_played, 0) AS matches_played,
            tst.matches_won,
            COALESCE(tst.rounds_won, 0) AS rounds_won,
            COALESCE(tst.rounds_lost, 0) AS rounds_lost
        FROM championships c
        JOIN team_season_totals tst
          ON tst.season = c.season
         AND tst.division_num = c.division_num
        LEFT JOIN teams t ON t.team_id = tst.team_id
        WHERE c.championship_id IN ({placeholders})
        """,
        params,
    )

    teams_by_champ: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        champ_id = str(row.get("championship_id"))
        team = {
            "team_id": str(row.get("team_id")),
            "team_name": row.get("team_name") or row.get("team_id"),
            "matches_played": int(row.get("matches_played") or 0),
            "matches_won": int(row.get("matches_won") or 0),
            "rounds_won": int(row.get("rounds_won") or 0),
            "rounds_lost": int(row.get("rounds_lost") or 0),
        }
        teams_by_champ.setdefault(champ_id, []).append(team)

    winners: dict[str, dict[str, Any]] = {}
    for champ_id, teams in teams_by_champ.items():
        ordered = await _rank_division_teams(champ_id, teams)
        if ordered:
            top = ordered[0]
            winners[champ_id] = {
                "team_id": top["team_id"],
                "team_name": top.get("team_name") or top["team_id"],
            }
    return winners


async def _rank_division_teams(championship_id: str, teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rank teams according to league rules (wins → round diff → head-to-head maps → head-to-head round diff)."""
    if not teams:
        return []

    for team in teams:
        team["round_diff"] = int(team.get("rounds_won", 0) - team.get("rounds_lost", 0))

    teams.sort(
        key=lambda row: (
            -row.get("matches_won", 0),
            -row.get("round_diff", 0),
            -row.get("rounds_won", 0),
            (row.get("team_name") or "").lower(),
            row.get("team_id")
        )
    )

    index = 0
    while index < len(teams):
        j = index + 1
        while (
            j < len(teams)
            and teams[j].get("matches_won") == teams[index].get("matches_won")
            and teams[j].get("round_diff") == teams[index].get("round_diff")
        ):
            j += 1

        if j - index > 1:
            tie_group = teams[index:j]
            tie_ids = [team["team_id"] for team in tie_group if team.get("team_id")]
            head_to_head = await _compute_head_to_head_stats(championship_id, tie_ids)

            ranked_group = sorted(
                tie_group,
                key=lambda row: (
                    -(head_to_head.get(row["team_id"], {}).get("map_diff", 0)),
                    -(head_to_head.get(row["team_id"], {}).get("round_diff", 0)),
                    -row.get("rounds_won", 0),
                    (row.get("team_name") or "").lower(),
                    row.get("team_id")
                )
            )
            teams[index:j] = ranked_group

        index = j

    return teams


async def _compute_head_to_head_stats(championship_id: str, team_ids: list[str]) -> dict[str, dict[str, int]]:
    """Return head-to-head map and round differentials between teams."""
    stats: dict[str, dict[str, int]] = {tid: {"map_wins": 0, "map_losses": 0, "map_diff": 0, "round_diff": 0} for tid in team_ids}
    if len(team_ids) < 2:
        return stats

    placeholders = ", ".join(f":tid{idx}" for idx in range(len(team_ids)))
    params = {"champ": championship_id}
    for idx, tid in enumerate(team_ids):
        params[f"tid{idx}"] = tid

    rows = await query_async(
        f"""
        SELECT
            m.team1_id,
            m.team2_id,
            mp.winner_team_id,
            mp.score_team1,
            mp.score_team2
        FROM matches m
        JOIN maps mp ON mp.match_id = m.match_id
        WHERE m.championship_id = :champ
          AND m.team1_id IN ({placeholders})
          AND m.team2_id IN ({placeholders})
        """,
        params,
    )

    for row in rows:
        team1 = str(row.get("team1_id"))
        team2 = str(row.get("team2_id"))
        if team1 not in stats or team2 not in stats:
            continue

        score1 = int(row.get("score_team1") or 0)
        score2 = int(row.get("score_team2") or 0)
        winner = row.get("winner_team_id")

        if winner == team1:
            stats[team1]["map_wins"] += 1
            stats[team2]["map_losses"] += 1
        elif winner == team2:
            stats[team2]["map_wins"] += 1
            stats[team1]["map_losses"] += 1

        stats[team1]["round_diff"] += score1 - score2
        stats[team2]["round_diff"] += score2 - score1

    for tid, record in stats.items():
        record["map_diff"] = record.get("map_wins", 0) - record.get("map_losses", 0)

    return stats
