from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional

from async_db import query_async
from division_overrides import combined_status_teams

from api.exceptions import NotFoundError
from api.services.player_counts import get_player_counts

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


def get_excluded_team_ids(championship_id: str) -> set[str]:
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}


async def fetch_seasons() -> List[dict[str, Any]]:
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


async def list_divisions(limit: int, offset: int) -> List[dict[str, Any]]:
    rows = await query_async(
        """
        SELECT championship_id, slug, name, season, division_num,
               is_playoffs AS is_playoff
        FROM championships
        ORDER BY season DESC, division_num, is_playoffs
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )
    return rows


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
    rows = await query_async(
        """
        SELECT
            c.championship_id,
            c.slug,
            c.name,
            c.season,
            c.division_num,
            CASE WHEN c.slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff,
            0 AS teams_count,
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
            COUNT(DISTINCT m.match_id) AS total_matches,
            MAX(m.updated_at) AS last_updated
        FROM championships c
        LEFT JOIN matches m ON c.championship_id = m.championship_id
        WHERE c.season = :season
        GROUP BY c.championship_id, c.slug, c.name, c.season, c.division_num, is_playoff
        ORDER BY c.division_num ASC
        LIMIT :limit OFFSET :offset
        """,
        {"season": season, "limit": limit, "offset": offset},
    )
    return rows


async def _fetch_champ_row(where_clause: str, params: dict[str, Any]) -> dict[str, Any]:
    rows = await query_async(
        f"""
        SELECT championship_id, slug, name, season, division_num,
            CASE WHEN slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff
        FROM championships
        WHERE {where_clause}
        LIMIT 1
        """,
        params,
    )
    if not rows:
        raise NotFoundError("Division not found")
    return rows[0]


async def fetch_division_by_slug(slug: str) -> dict[str, Any]:
    return await _fetch_champ_row("slug = :slug", {"slug": slug})


async def fetch_division_by_id(championship_id: str) -> dict[str, Any]:
    return await _fetch_champ_row("championship_id = :champ_id", {"champ_id": championship_id})


async def get_division_details(champ: dict[str, Any]) -> dict[str, Any]:
    championship_id = champ["championship_id"]
    season = int(champ["season"])
    division_num = int(champ["division_num"])

    team_rows = await query_async(
        """
        SELECT DISTINCT t.team_id, t.name AS team_name, t.name AS display_name, t.avatar,
               COALESCE(tst.matches_played, 0) AS matches_played,
               COALESCE(tst.matches_won, 0) AS matches_won,
               COALESCE(tst.maps_played, 0) AS maps_played,
               COALESCE(tst.maps_won, 0) AS maps_won,
               COALESCE(tst.rounds_won, 0) AS rounds_won,
               COALESCE(tst.rounds_lost, 0) AS rounds_lost,
               COALESCE(agg.kills, 0) AS kills,
               COALESCE(agg.deaths, 0) AS deaths,
               COALESCE(agg.damage, 0) AS damage
        FROM teams t
        JOIN matches m ON (m.team1_id = t.team_id OR m.team2_id = t.team_id)
        LEFT JOIN team_season_totals tst ON tst.team_id = t.team_id AND tst.season = :season AND tst.division_num = :division
        LEFT JOIN (
            SELECT team_id, SUM(kills) AS kills, SUM(deaths) AS deaths, SUM(damage) AS damage
            FROM team_map_season_totals
            WHERE season = :season AND division_num = :division
            GROUP BY team_id
        ) agg ON agg.team_id = t.team_id
        WHERE m.championship_id = :champ_id
        ORDER BY team_name, team_id
        """,
        {"champ_id": championship_id, "season": season, "division": division_num},
    )

    excluded = get_excluded_team_ids(championship_id)

    player_rows = await query_async(
        """
        SELECT
            pst.team_id,
            pst.player_id,
            pst.maps_played,
            pst.rounds_played,
            pst.kills,
            pst.deaths,
            p.nickname
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season
          AND pst.division_num = :division
        """,
        {"season": season, "division": division_num},
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
    division_player_count = player_counts.get("division_players")
    if (division_player_count is None or division_player_count == 0) and unique_player_ids:
        division_player_count = len(unique_player_ids)

    map_stats_task = asyncio.create_task(_get_division_map_stats(championship_id, season, division_num))
    aggregates_task = asyncio.create_task(_get_division_aggregates(championship_id, season, division_num))
    leaders_task = asyncio.create_task(_get_division_leaders(championship_id, season, division_num))

    map_stats, aggregates, leaders = await asyncio.gather(
        map_stats_task,
        aggregates_task,
        leaders_task,
    )

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
        "player_count": int(division_player_count or 0),
        "season_player_count": int(player_counts.get("season_players") or 0),
        "all_time_player_count": int(player_counts.get("all_time_players") or 0),
    }


async def _get_division_map_stats(championship_id: str, season: int, division_num: int) -> List[Dict[str, Any]]:
    rows = await query_async(
        """
        WITH division_matches AS (
            SELECT match_id
            FROM matches
            WHERE championship_id = :champ_id
              AND season = :season
              AND division_num = :division
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
              AND m.season = :season
              AND m.division_num = :division
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
                AND ps.season = :season
                AND ps.division_num = :division
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
              AND v.season = :season
              AND v.division_num = :division
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
        {"champ_id": championship_id, "season": season, "division": division_num},
    )

    result = []
    for r in rows:
        maps_played = int(r.get("maps_played") or 0)
        rounds_played = int(r.get("rounds_played") or 0)

        result.append(
            {
                "map_name": r.get("map_name"),
                "pretty_name": r.get("pretty_name") or r.get("map_name"),
                "image_sm": r.get("image_sm"),
                "maps_played": maps_played,
                "banned": int(r.get("banned") or 0),
                "kills": int(r.get("kills") or 0),
                "deaths": int(r.get("deaths") or 0),
                "damage": int(r.get("damage") or 0),
                "rounds_played": rounds_played,
                "adr": float(r.get("adr") or 0.0),
                "kr": float(r.get("kr") or 0.0),
                "udpr": float(r.get("udpr") or 0.0),
                "enemy_flash": float(r.get("enemy_flash") or 0.0),
                "sniper_kills": int(r.get("sniper_kills") or 0),
                "assists": int(r.get("assists") or 0),
                "k2": int(r.get("k2") or 0),
                "k3": int(r.get("k3") or 0),
                "k4": int(r.get("k4") or 0),
                "ace": int(r.get("ace") or 0),
                "pistol_kills": int(r.get("pistol_kills") or 0),
                "pick_rate": round((maps_played / max(rounds_played, 1)) * 100, 1) if rounds_played else 0.0,
            }
        )
    return result


async def _get_division_aggregates(championship_id: str, season: int, division_num: int) -> dict[str, Any]:
    rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
            COUNT(DISTINCT m.match_id) AS total_matches,
            SUM(CASE WHEN m.is_forfeit = 1 THEN 1 ELSE 0 END) AS forfeits
        FROM matches m
        WHERE m.championship_id = :champ_id
          AND m.season = :season
          AND m.division_num = :division
        """,
        {"champ_id": championship_id, "season": season, "division": division_num},
    )
    row = rows[0] if rows else {}
    return {
        "played_matches": int(row.get("played_matches") or 0),
        "total_matches": int(row.get("total_matches") or 0),
        "forfeits": int(row.get("forfeits") or 0),
    }


async def _get_division_leaders(championship_id: str, season: int, division_num: int) -> List[Dict[str, Any]]:
    rows = await query_async(
        """
        SELECT
            pst.player_id,
            pst.team_id,
            pst.kills,
            pst.deaths,
            pst.adr,
            pst.kr,
            pst.rating,
            pst.mvps,
            pst.utility_damage,
            p.nickname,
            t.name AS team_name
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE pst.season = :season
          AND pst.division_num = :division
        ORDER BY pst.rating DESC, pst.kills DESC
        LIMIT 10
        """,
        {"season": season, "division": division_num},
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
                "rating": float(row.get("rating") or 0.0),
                "mvps": int(row.get("mvps") or 0),
                "utility_damage": int(row.get("utility_damage") or 0),
            }
        )
    return leaders
