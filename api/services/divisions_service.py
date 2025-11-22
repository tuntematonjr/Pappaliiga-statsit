from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional, Sequence

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
               is_playoffs AS is_playoff, parent_championship_id
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
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
            COUNT(DISTINCT m.match_id) AS total_matches,
            SUM(CASE WHEN m.finished_at IS NOT NULL THEN 1 ELSE 0 END) AS finished_matches,
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
    return rows


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

    # Fallback: aggregate from team_map_season_totals when detailed map rows are missing
    fallback_rows = await query_async(
        """
        SELECT
            tm.map_name AS map_name,
            mc.pretty_name,
            mc.image_sm,
            SUM(tm.games) AS maps_played,
            SUM(tm.wins) AS wins,
            SUM(tm.total_own_ban + tm.opp_ban + tm.ban1 + tm.ban2) AS banned,
            SUM(tm.kills) AS kills,
            SUM(tm.deaths) AS deaths,
            SUM(tm.damage) AS damage,
            SUM(tm.utility_damage) AS utility_damage,
            SUM(tm.adr * tm.games) AS adr_weighted,
            SUM(tm.games) AS adr_weight,
            SUM(tm.rd) AS rounds_played
        FROM team_map_season_totals tm
        LEFT JOIN maps_catalog mc ON mc.map_id = tm.map_name
        WHERE tm.season = :season
          AND tm.division_num = :division
        GROUP BY tm.map_name, mc.pretty_name, mc.image_sm
        ORDER BY maps_played DESC, tm.map_name
        """,
        {"season": season, "division": division_num},
    )

    normalized_rows: list[dict[str, Any]] = []
    for r in fallback_rows or []:
        maps_played = int(r.get("maps_played") or 0)
        wins = int(r.get("wins") or 0)
        deaths = int(r.get("deaths") or 0)
        weight = float(r.get("adr_weight") or 0.0)
        adr_weighted = float(r.get("adr_weighted") or 0.0)
        normalized_rows.append(
            {
                "map_name": r.get("map_name"),
                "pretty_name": r.get("pretty_name"),
                "image_sm": r.get("image_sm"),
                "maps_played": maps_played,
                "wins": wins,
                "losses": max(maps_played - wins, 0),
                "banned": int(r.get("banned") or 0),
                "kills": int(r.get("kills") or 0),
                "deaths": deaths,
                "damage": int(r.get("damage") or 0),
                "rounds_played": int(r.get("rounds_played") or 0),
                "adr": (adr_weighted / weight) if weight else 0.0,
                "kr": 0.0,
                "udpr": float(r.get("utility_damage") or 0.0) / max(deaths, 1),
                "enemy_flash": 0.0,
                "sniper_kills": 0,
                "assists": 0,
                "k2": 0,
                "k3": 0,
                "k4": 0,
                "ace": 0,
                "pistol_kills": 0,
                "pick_rate": 0.0,
            }
        )

    return _build_result(normalized_rows)


async def _get_division_aggregates(championship_id: str, season: int, division_num: int) -> dict[str, Any]:
    rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
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
        SELECT SUM(tm.games) AS maps_played_total
        FROM team_map_season_totals tm
        WHERE tm.season = :season
          AND tm.division_num = :division
        """,
        {"season": season, "division": division_num},
    )
    maps_played_total = int((map_totals[0] or {}).get("maps_played_total") or 0) if map_totals else 0

    played = int(row.get("played_matches") or 0)
    total = int(row.get("total_matches") or 0)
    forfeits = int(row.get("forfeits") or 0)

    return {
        "played_matches": played,
        "matches_played": played,
        "total_matches": total,
        "forfeits": forfeits,
        "maps_played_total": maps_played_total,
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
