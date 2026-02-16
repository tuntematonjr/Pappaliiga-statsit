from __future__ import annotations

from typing import Any, Optional
from datetime import datetime, timezone

from db_async import (
    compute_team_map_deltas_async,
    get_team_matches_mirror_async,
    query_async,
)

from api.exceptions import NotFoundError
from division_naming import build_division_name
from api.services.player_stats_payload import build_player_stats_payload
from api.services.cache_helpers import (
    GLOBAL_CACHE,
    get_global_revision,
)

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


async def fetch_team(team_id: str) -> dict[str, Any]:
    revision = await get_global_revision()
    cache_key = ("fetch_team", team_id, revision)

    async def _compute():
        rows = await query_async(
            """
            SELECT team_id, name AS team_name, name AS display_name, avatar
            FROM teams
            WHERE team_id = :team_id
            """,
            {"team_id": team_id},
        )
        if not rows:
            raise NotFoundError(f"Team '{team_id}' not found")
        team = rows[0]
        team.setdefault("avatar", DEFAULT_AVATAR)
        team["faceit_url"] = None
        return team

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def fetch_team_season_stats(team_id: str) -> list[dict[str, Any]]:
    revision = await get_global_revision()
    cache_key = ("fetch_team_season_stats", team_id, revision)

    async def _compute():
        rows = await query_async(
            """
            WITH team_champs AS (
                SELECT championship_id
                FROM team_championships
                WHERE team_id = :team_id
            ),
            base_champs AS (
                SELECT c.season, c.division_num, c.championship_id, c.name, c.is_playoffs
                FROM championships c
                JOIN team_champs tc ON tc.championship_id = c.championship_id
                UNION ALL
                SELECT c.season, c.division_num, c.championship_id, c.name, c.is_playoffs
                FROM team_season_totals tst
                JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
                WHERE tst.team_id = :team_id
                  AND c.championship_id NOT IN (SELECT championship_id FROM team_champs)
            )
            SELECT bc.season,
                   bc.division_num,
                   bc.championship_id,
                   bc.name,
                   bc.is_playoffs,
                   COALESCE(tst.maps_played, 0) AS maps_played,
                   COALESCE(tst.matches_played, 0) AS matches_played,
                   COALESCE(tst.matches_won, 0) AS wins,
                   GREATEST(
                       CAST(COALESCE(tst.matches_played, 0) AS SIGNED) - CAST(COALESCE(tst.matches_won, 0) AS SIGNED),
                       0
                   ) AS losses,
                   CASE WHEN COALESCE(tst.matches_played, 0) > 0
                        THEN (tst.matches_won / tst.matches_played)
                        ELSE 0.0 END AS win_rate,
                   COALESCE(tst.rounds_won, 0) AS rounds_won,
                   COALESCE(tst.rounds_lost, 0) AS rounds_lost,
                   COALESCE(tst.maps_won, 0) AS maps_won
            FROM base_champs bc
            LEFT JOIN team_season_totals tst
                ON tst.team_id = :team_id
               AND tst.season = bc.season
               AND tst.division_num = bc.division_num
            ORDER BY bc.season DESC, bc.division_num
            """,
            {"team_id": team_id},
        )
        if not rows:
            raise NotFoundError(f"No stats found for team '{team_id}'")
        processed = []
        for row in rows:
            data = dict(row)
            data["name"] = build_division_name(
                data.get("season"),
                data.get("division_num"),
                data.get("is_playoffs"),
            )
            processed.append(data)
        return processed

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def fetch_team_season_progression(
    team_id: str,
    season: int,
    division_num: int,
) -> list[dict[str, Any]]:
    rows = await query_async(
        """
        SELECT
            tst.snapshot_ts,
            ds.created_at AS snapshot_time,
            tst.matches_played,
            tst.matches_won,
            GREATEST(
                CAST(tst.matches_played AS SIGNED) - CAST(tst.matches_won AS SIGNED),
                0
            ) AS losses,
            CASE WHEN tst.matches_played > 0
                 THEN (tst.matches_won / tst.matches_played)
                 ELSE 0.0 END AS win_rate,
            tst.maps_played,
            tst.maps_won,
            tst.rounds_won,
            tst.rounds_lost
        FROM team_season_totals_prev tst
        LEFT JOIN division_snapshots ds ON ds.snapshot_ts = tst.snapshot_ts
        WHERE tst.team_id = :team_id
          AND tst.season = :season
          AND tst.division_num = :division_num
        ORDER BY tst.snapshot_ts ASC
        """,
        {"team_id": team_id, "season": season, "division_num": division_num},
    )
    if not rows:
        raise NotFoundError(
            f"No progression snapshots found for team '{team_id}' in season {season} division {division_num}"
        )
    return rows


async def list_teams(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    limit: int,
) -> list[dict[str, Any]]:
    if season is not None:
        season_filter = ["c.season = :season"]
        season_name_filter = ["c2.season = :season"]
        params: dict[str, Any] = {"season": season, "limit": limit}

        if division is not None:
            season_filter.append("c.division_num = :division")
            season_name_filter.append("c2.division_num = :division")
            params["division"] = division

        season_where = " AND ".join(season_filter)
        season_name_where = " AND ".join(season_name_filter)
        rows = await query_async(
            f"""
            SELECT
                t.team_id,
                COALESCE(
                    NULLIF((
                        SELECT tc2.team_name
                        FROM team_championships tc2
                        JOIN championships c2 ON c2.championship_id = tc2.championship_id
                        WHERE tc2.team_id = t.team_id
                          AND {season_name_where}
                          AND NULLIF(TRIM(tc2.team_name), '') IS NOT NULL
                        ORDER BY tc2.updated_at DESC, tc2.created_at DESC
                        LIMIT 1
                    ), ''),
                    t.name
                ) AS team_name,
                COALESCE(
                    NULLIF((
                        SELECT tc2.team_name
                        FROM team_championships tc2
                        JOIN championships c2 ON c2.championship_id = tc2.championship_id
                        WHERE tc2.team_id = t.team_id
                          AND {season_name_where}
                          AND NULLIF(TRIM(tc2.team_name), '') IS NOT NULL
                        ORDER BY tc2.updated_at DESC, tc2.created_at DESC
                        LIMIT 1
                    ), ''),
                    t.name
                ) AS display_name,
                t.avatar
            FROM teams t
            JOIN (
                SELECT DISTINCT tc.team_id
                FROM team_championships tc
                JOIN championships c ON c.championship_id = tc.championship_id
                WHERE {season_where}
                UNION
                SELECT DISTINCT tst.team_id
                FROM team_season_totals tst
                WHERE tst.season = :season
                  {"AND tst.division_num = :division" if division is not None else ""}
            ) season_teams ON season_teams.team_id = t.team_id
            ORDER BY display_name, t.team_id
            LIMIT :limit
            """,
            params,
        )
    else:
        rows = await query_async(
            """
            SELECT team_id, name AS team_name, name AS display_name, avatar
            FROM teams
            ORDER BY name, team_id
            LIMIT :limit
            """,
            {"limit": limit},
        )
    for row in rows:
        row.setdefault("avatar", DEFAULT_AVATAR)
        row["faceit_url"] = None
    return rows


async def fetch_team_matches(team_id: str, championship_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch team's matches. If championship_id provided, filter to that championship."""
    # Verify team exists
    team_check = await query_async(
        "SELECT team_id FROM teams WHERE team_id = :team_id",
        {"team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found")
    
    # Get championship_id if not provided - use the one with actual matches
    if championship_id is None:
        # Get the latest championship with actual matches for this team
        champ_rows = await query_async(
            """
            SELECT DISTINCT c.championship_id
            FROM team_season_totals tst
            JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
            WHERE tst.team_id = :team_id
            AND EXISTS (
                SELECT 1 FROM matches m
                WHERE m.championship_id = c.championship_id
                AND (m.team1_id = :team_id OR m.team2_id = :team_id)
            )
            ORDER BY tst.season DESC
            LIMIT 1
            """,
            {"team_id": team_id}
        )
        if not champ_rows:
            raise NotFoundError(f"No matches found for team '{team_id}'")
        championship_id = champ_rows[0]["championship_id"]
    else:
        # Verify championship exists
        champ_rows = await query_async(
            "SELECT championship_id FROM championships WHERE championship_id = :champ_id",
            {"champ_id": championship_id}
        )
        if not champ_rows:
            raise NotFoundError(f"Championship {championship_id} not found")
    
    matches = await get_team_matches_mirror_async(championship_id, team_id)
    if not matches:
        raise NotFoundError(f"No matches found for team '{team_id}' in championship {championship_id}")
    
    # Transform to flat list format for API response
    result = []
    for match in matches:
        left = match.get("left", {})
        right = match.get("right", {})
        result.append({
            "match_id": match["match_id"],
            "ts": match["ts"],
            "scheduled_at": match.get("scheduled_at"),
            "status": match["status"],
            "best_of": match["best_of"],
            "played": match["played"],
            "team1_id": left.get("team_id"),
            "team2_id": right.get("team_id"),
            "team1_name": left.get("team_name"),
            "team2_name": right.get("team_name"),
            "t1_avatar": left.get("avatar"),
            "t2_avatar": right.get("avatar"),
            "faceit_url": match.get("faceit_url"),
            "is_forfeit": match.get("is_forfeit"),
            "winner_team_id": match.get("winner_team_id"),
            "maps": match.get("maps", [])
        })
    
    return result


async def fetch_team_match_player_stats(team_id: str, championship_id: str) -> list[dict[str, Any]]:
    """Fetch player map stats for every match the team played in a championship."""
    team_check = await query_async(
        "SELECT team_id FROM teams WHERE team_id = :team_id",
        {"team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found")

    champ_rows = await query_async(
        "SELECT championship_id FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id}
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    rows = await query_async(
        """
        SELECT
            ps.match_id,
            ps.round_index,
            ps.map_id,
            mp.map_name,
            mc.image_sm,
            mc.image_lg,
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
        LEFT JOIN player_championships pc ON pc.player_id = ps.player_id AND pc.championship_id = :champ_id
        LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        LEFT JOIN maps_catalog mc ON LOWER(mc.map_id) = LOWER(mp.map_name)
        WHERE m.championship_id = :champ_id
          AND (m.team1_id = :team_id OR m.team2_id = :team_id)
        ORDER BY ps.match_id, ps.round_index, ps.player_id
        """,
        {"champ_id": championship_id, "team_id": team_id}
    )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        stats_raw = build_player_stats_payload(row)
        normalized.append(
            {
                "match_id": row.get("match_id"),
                "round_index": int(row.get("round_index") or 0),
                "map_id": row.get("map_id"),
                "map_name": row.get("map_name"),
                "image_sm": row.get("image_sm"),
                "image_lg": row.get("image_lg"),
                "player_id": row.get("player_id"),
                "nickname": row.get("nickname"),
                "team_id": row.get("team_id"),
                "opponent_team_id": row.get("opponent_team_id"),
                "is_forfeit_map": bool(row.get("is_forfeit_map")),
                "stats": stats_raw or {},
            }
        )

    return normalized


def _normalize_matches_for_page(matches: list[dict[str, Any]], team_id: str) -> list[dict[str, Any]]:
    """Ensure matches expose a readable datetime and opponent info for the frontend."""
    normalized: list[dict[str, Any]] = []
    for match in matches:
        item = dict(match)
        ts = match.get("ts") or match.get("played")
        played_at = None
        if ts:
            try:
                played_at = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            except Exception:
                played_at = None
        item["played_at"] = played_at
        # Add opponent convenience fields
        t1 = match.get("team1_id")
        t2 = match.get("team2_id")
        if team_id and (t1 or t2):
            if str(team_id) == str(t1):
                item["opponent_name"] = match.get("team2_name")
            elif str(team_id) == str(t2):
                item["opponent_name"] = match.get("team1_name")
        # Normalize nested maps list to plain dicts
        maps = item.get("maps")
        if isinstance(maps, list):
            cleaned_maps = []
            for m in maps:
                md = dict(m)
                # normalize player side stat blobs if present
                for key in ("left", "right"):
                    if key in md and isinstance(md[key], dict):
                        md[key] = dict(md[key])
                cleaned_maps.append(md)
            item["maps"] = cleaned_maps
        normalized.append(item)
    return normalized


async def fetch_team_map_stats_comprehensive(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    """Fetch comprehensive map statistics for a team in a championship with player metrics aggregated by map."""
    champ_rows = await query_async(
        "SELECT season, division_num, is_playoffs FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    # Base map stats derived from matches in this championship
    map_deltas = await compute_team_map_deltas_async(championship_id, team_id)
    if not map_deltas:
        raise NotFoundError(f"No map stats found for team '{team_id}' in championship {championship_id}")
    team_map_rows = []
    for map_name, payload in map_deltas.items():
        curr = payload.get("curr") or {}
        played = int(curr.get("played") or 0)
        wins = int(curr.get("wins") or 0)
        picks = int(curr.get("picks") or 0)
        pick_wins = int(curr.get("wins_own") or 0)
        opp_pick_wins = int(curr.get("wins_opp") or 0)
        team_map_rows.append(
            {
                "map_name": map_name,
                "played": played,
                "picks": picks,
                "opp_picks": int(curr.get("opp_picks") or 0),
                "pick_wins": pick_wins,
                "opp_pick_wins": opp_pick_wins,
                "wins": wins,
                "games": int(curr.get("games") or 0),
                "ban1": int(curr.get("ban1") or 0),
                "ban2": int(curr.get("ban2") or 0),
                "opp_ban": int(curr.get("opp_ban") or 0),
                "total_own_ban": int(curr.get("total_own_ban") or 0),
                "decov": int(curr.get("decov") or 0),
                "kills": 0,
                "deaths": 0,
                "mvps": 0,
                "rd": int(curr.get("rd") or 0),
                "kd": float(curr.get("kd") or 0),
                "adr": float(curr.get("adr") or 0),
                "damage": 0,
                "utility_damage": 0,
                "winrate": (wins / played * 100) if played > 0 else 0.0,
                "pick_rate": (picks / played * 100) if played > 0 else 0.0,
            }
        )

    # Attach map images from catalog for UI rendering
    catalog_rows = await query_async(
        "SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog"
    )
    catalog_by_id = {}
    catalog_by_pretty = {}
    for row in catalog_rows:
        map_id = (row.get("map_id") or "").lower()
        pretty = (row.get("pretty_name") or "").lower()
        if map_id:
            catalog_by_id[map_id] = row
        if pretty:
            catalog_by_pretty[pretty] = row

    for row in team_map_rows:
        key = (row.get("map_name") or "").lower()
        match = catalog_by_id.get(key) or catalog_by_pretty.get(key)
        if match:
            row["image_sm"] = match.get("image_sm")
            row["image_lg"] = match.get("image_lg")
    
    # Enhance with per-player stats aggregated by map
    # Always use direct query from player_stats joined with championship matches
    # to ensure stats are only from matches actually played in this championship
    player_stats_by_map = await query_async(
        """
        SELECT
            m.map_name,
            COUNT(DISTINCT ps.player_stat_id) as stat_count,
            SUM(ps.kills) as kills,
            SUM(ps.deaths) as deaths,
            SUM(ps.assists) as assists,
            SUM(ps.mvps) as mvps,
            SUM(ps.damage) as damage,
            SUM(ps.utility_damage) as utility_damage,
            AVG(ps.kr) as kr,
            AVG(ps.hs_pct) as hs_pct,
            SUM(ps.sniper_kills) as sniper_kills,
            SUM(ps.pistol_kills) as pistol_kills,
            SUM(ps.mk_2k) as multi_2k,
            SUM(ps.mk_3k) as multi_3k,
            SUM(ps.mk_4k) as multi_4k,
            SUM(ps.mk_5k) as multi_5k,
            SUM(ps.clutch_kills) as clutch_kills,
            SUM(ps.enemies_flashed) as enemies_flashed,
            SUM(ps.flash_count) as flash_count,
            SUM(ps.flash_successes) as flash_successes
        FROM maps m
        INNER JOIN matches mt ON m.match_id = mt.match_id 
        LEFT JOIN player_stats ps ON m.map_id = ps.map_id AND ps.team_id = :team_id AND ps.is_forfeit_map = 0
        WHERE mt.championship_id = :champ_id
            AND (mt.team1_id = :team_id OR mt.team2_id = :team_id)
            AND m.is_forfeit = 0
        GROUP BY m.map_name
        """,
        {"champ_id": championship_id, "team_id": team_id}
    )
    
    # Create lookup dict for player stats
    player_stats_map = {row["map_name"]: dict(row) for row in player_stats_by_map}
    
    # Get actual round counts by map (won + lost rounds)
    # Always filter by championship_id to avoid mixing regular season and playoffs
    rounds_by_map_rows = await query_async(
        """
        SELECT
            m.map_name,
            SUM(COALESCE(ts_team.final_score, 0)) as rounds_won,
            SUM(COALESCE(ts_opp.final_score, 0)) as rounds_lost
        FROM maps m
        INNER JOIN matches mt ON m.match_id = mt.match_id
        INNER JOIN team_stats ts_team
            ON m.map_id = ts_team.map_id
            AND ts_team.team_id = :team_id
            AND ts_team.is_forfeit_map = 0
        LEFT JOIN team_stats ts_opp
            ON m.map_id = ts_opp.map_id
            AND ts_opp.team_id <> :team_id
            AND ts_opp.is_forfeit_map = 0
        WHERE mt.championship_id = :champ_id
            AND (mt.team1_id = :team_id OR mt.team2_id = :team_id)
            AND m.is_forfeit = 0
        GROUP BY m.map_name
        """,
        {"champ_id": championship_id, "team_id": team_id}
    )
    
    # Create lookup dict for rounds by map
    rounds_by_map = {
        row["map_name"]: {
            "rounds_won": int(row["rounds_won"] or 0),
            "rounds_lost": int(row["rounds_lost"] or 0)
        }
        for row in rounds_by_map_rows
    }
    
    # Merge data
    result = []
    for row in team_map_rows:
        data = dict(row)
        played = data.get("played") or 0
        picks = data.get("picks") or 0
        wins = data.get("wins") or 0
        
        data["winrate"] = (wins / played * 100) if played > 0 else 0.0
        data["pick_rate"] = (picks / played * 100) if played > 0 else 0.0
        
        # Use actual round count from matches, fall back to estimate if not available
        map_name = data.get("map_name")
        round_bucket = rounds_by_map.get(map_name, {"rounds_won": 0, "rounds_lost": 0})
        rounds_won = round_bucket.get("rounds_won", 0)
        rounds_lost = round_bucket.get("rounds_lost", 0)
        total_rounds = rounds_won + rounds_lost
        
        # Store actual round totals for display
        data["rounds_won"] = rounds_won
        data["rounds_lost"] = rounds_lost
        data["total_rounds_played"] = total_rounds
        
        # Calculate metrics from damage using actual rounds
        if data.get("adr", 0) == 0 and total_rounds > 0:
            data["adr"] = data.get("damage", 0) / total_rounds
        
        # Calculate UDPR
        if total_rounds > 0:
            data["udpr"] = data.get("utility_damage", 0) / total_rounds
        else:
            data["udpr"] = 0
        
        # Merge player stats if available
        if map_name and map_name in player_stats_map:
            player_data = player_stats_map[map_name]
            for key, value in player_data.items():
                if key != "map_name" and key != "stat_count":
                    # Convert Decimal to float/int
                    if value is not None:
                        if key in ("kr", "hs_pct"):  # Float fields
                            data[key] = float(value) if value is not None else 0
                        else:  # Integer fields
                            data[key] = int(float(value)) if value is not None else 0
                    else:
                        data[key] = 0

        # Refresh KD after merging player totals if possible
        kills_total = data.get("kills") or 0
        deaths_total = data.get("deaths") or 0
        if deaths_total:
            data["kd"] = kills_total / deaths_total
        
        result.append(data)
    
    return result


async def fetch_team_players_comprehensive(team_id: str, championship_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch comprehensive player statistics for a team."""
    if championship_id is None:
        champ_rows = await query_async(
            """
            SELECT DISTINCT c.championship_id, c.season, c.division_num
            FROM team_season_totals tst
            JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
            WHERE tst.team_id = :team_id
            ORDER BY tst.season DESC
            LIMIT 1
            """,
            {"team_id": team_id}
        )
        if not champ_rows:
            raise NotFoundError(f"No championship found for team '{team_id}'")
        championship_id = champ_rows[0]["championship_id"]
    
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Get all player season totals for the team
    rows = await query_async(
        """
        SELECT
            pst.player_id,
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
            pst.mk_2k,
            pst.mk_3k,
            pst.mk_4k,
            pst.mk_5k,
            pst.clutch_kills,
            pst.cl_1v1_attempts,
            pst.cl_1v1_wins,
            pst.cl_1v2_attempts,
            pst.cl_1v2_wins,
            pst.entry_count,
            pst.entry_wins,
            pst.pistol_kills,
            pst.adr,
            pst.kr,
            pst.kd,
            pst.hs_pct,
            pst.damage,
            COALESCE(pc.player_name, p.nickname) AS nickname
        FROM player_season_totals pst
        LEFT JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN player_championships pc ON pc.player_id = pst.player_id AND pc.championship_id = :champ_id
        WHERE pst.season = :season AND pst.division_num = :div AND pst.team_id = :team_id
        ORDER BY pst.maps_played DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id, "champ_id": championship_id}
    )
    
    if not rows:
        raise NotFoundError(f"No players found for team '{team_id}' in championship {championship_id}")
    
    return rows


async def fetch_team_veto_history(team_id: str, championship_id: str) -> list[dict[str, Any]]:
    """Fetch team's veto/pick history for a championship."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Get all veto/pick actions for this team in this championship
    rows = await query_async(
        """
        SELECT
            mv.vote_id, mv.match_id, mv.map_name, mv.status, 
            mv.selected_by_team_id, mv.round_num,
            ROW_NUMBER() OVER (PARTITION BY mv.match_id ORDER BY mv.vote_id ASC) as order_in_match,
            t.name as selected_by_team_name
        FROM map_votes mv
        LEFT JOIN teams t ON t.team_id = mv.selected_by_team_id
        WHERE mv.season = :season AND mv.division_num = :div
        AND (mv.selected_by_team_id = :team_id OR 
             EXISTS (SELECT 1 FROM matches m WHERE m.match_id = mv.match_id 
                     AND (m.team1_id = :team_id OR m.team2_id = :team_id)))
        ORDER BY mv.match_id, mv.vote_id
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    
    if not rows:
        raise NotFoundError(f"No veto history found for team '{team_id}' in championship {championship_id}")
    
    # Transform to flat format with match context
    result = []
    for row in rows:
        result.append({
            "match_id": row["match_id"],
            "map_name": row["map_name"],
            "status": row["status"],
            "selected_by_team_id": row["selected_by_team_id"],
            "selected_by_team_name": row["selected_by_team_name"],
            "round_num": row["round_num"],
            "order": row["order_in_match"]
        })
    
    return result


async def fetch_team_veto_aggregates(team_id: str, championship_id: str) -> list[dict[str, Any]]:
    """Fetch aggregated veto/ban statistics for a team."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Get veto/pick aggregates per map
    rows = await query_async(
        """
        SELECT
            mv.map_name,
            SUM(CASE WHEN mv.status = 'banned' AND mv.selected_by_team_id = :team_id THEN 1 ELSE 0 END) as times_banned,
            SUM(CASE WHEN mv.status = 'picked' AND mv.selected_by_team_id = :team_id THEN 1 ELSE 0 END) as times_picked,
            SUM(CASE WHEN mv.status = 'picked' AND mv.selected_by_team_id != :team_id THEN 1 ELSE 0 END) as times_opponent_picked,
            COUNT(DISTINCT mv.match_id) as total_match_occurrences
        FROM map_votes mv
        WHERE mv.season = :season AND mv.division_num = :div
        AND EXISTS (
            SELECT 1 FROM matches m 
            WHERE m.match_id = mv.match_id 
            AND (m.team1_id = :team_id OR m.team2_id = :team_id)
        )
        GROUP BY mv.map_name
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    
    if not rows:
        return []
    
    # Calculate rates and sort by total picks/bans
    result = []
    for row in rows:
        times_banned = row["times_banned"] or 0
        times_picked = row["times_picked"] or 0
        total = times_banned + times_picked
        data = {
            "map_name": row["map_name"],
            "times_banned": times_banned,
            "times_picked": times_picked,
            "times_opponent_picked": row["times_opponent_picked"] or 0,
            "ban_rate": (times_banned / total * 100) if total > 0 else 0.0,
            "pick_rate": (times_picked / total * 100) if total > 0 else 0.0,
        }
        # Calculate pick win rate
        if times_picked > 0:
            data["pick_win_rate"] = None  # Would need match win data
        result.append(data)
    
    # Sort by total (picks + bans) descending
    result.sort(key=lambda x: (x["times_picked"] + x["times_banned"]), reverse=True)
    
    return result


async def get_division_averages(championship_id: str) -> dict[str, float]:
    """Get division-wide averages for comparison."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        return {}
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    rows = await query_async(
        """
        SELECT
            AVG(CASE WHEN tst.matches_played > 0 THEN (tst.matches_won / tst.matches_played) * 100 ELSE 0 END) AS avg_win_rate,
            AVG(CAST(tst.rounds_won AS SIGNED) - CAST(tst.rounds_lost AS SIGNED)) AS avg_round_diff,
            AVG(CASE WHEN tst.maps_played > 0 THEN (tst.maps_won / tst.maps_played) * 100 ELSE 0 END) AS avg_map_win_rate
        FROM team_season_totals tst
        WHERE tst.season = :season AND tst.division_num = :div
        """,
        {"season": season, "div": division_num}
    )
    
    if not rows:
        return {}
    
    row = rows[0]
    return {
        "avg_win_rate": float(row.get("avg_win_rate") or 0.0),
        "avg_round_diff": float(row.get("avg_round_diff") or 0.0),
        "avg_map_win_rate": float(row.get("avg_map_win_rate") or 0.0)
    }


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        return (ordered[mid - 1] + ordered[mid]) / 2.0
    return ordered[mid]


def _percentile_rank(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    lower = sum(1 for x in values if x < value)
    equal = sum(1 for x in values if x == value)
    return (lower + (equal * 0.5)) / len(values)


async def detect_player_roles(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    """Detect player roles with team-relative scoring and sample-size guards."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        return []
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    rows = await query_async(
        """
        SELECT
            pst.player_id,
            COALESCE(pc.player_name, p.nickname) AS nickname,
            pst.maps_played,
            pst.sniper_kills,
            pst.entry_count,
            pst.entry_wins,
            pst.assists,
            pst.utility_damage,
            pst.enemies_flashed,
            pst.clutch_kills,
            pst.cl_1v1_attempts,
            pst.cl_1v1_wins,
            pst.kills,
            pst.rounds_played,
            CASE WHEN pst.rounds_played > 0 THEN pst.sniper_kills / pst.rounds_played ELSE 0 END AS awp_rate,
            CASE WHEN pst.entry_count > 0 THEN pst.entry_wins / pst.entry_count ELSE 0 END AS entry_success,
            CASE WHEN pst.rounds_played > 0 THEN pst.assists / pst.rounds_played ELSE 0 END AS assist_rate,
            CASE WHEN pst.cl_1v1_attempts > 0 THEN pst.cl_1v1_wins / pst.cl_1v1_attempts ELSE 0 END AS clutch_success
        FROM player_season_totals pst
        LEFT JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN player_championships pc ON pc.player_id = pst.player_id AND pc.championship_id = :champ_id
        WHERE pst.season = :season AND pst.division_num = :div AND pst.team_id = :team_id
        ORDER BY pst.maps_played DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id, "champ_id": championship_id}
    )
    
    if not rows:
        return []

    MIN_MAPS = 3
    MIN_ROUNDS = 60
    MIN_ENTRY_ATTEMPTS = 24
    MIN_CLUTCH_ATTEMPTS = 6

    players = []
    for row in rows:
        maps_played = int(row.get("maps_played") or 0)
        rounds_played = int(row.get("rounds_played") or 0)
        sniper_kills = int(row.get("sniper_kills") or 0)
        entry_count = int(row.get("entry_count") or 0)
        entry_wins = int(row.get("entry_wins") or 0)
        assists = int(row.get("assists") or 0)
        utility_damage = int(row.get("utility_damage") or 0)
        enemies_flashed = int(row.get("enemies_flashed") or 0)
        cl_1v1_attempts = int(row.get("cl_1v1_attempts") or 0)
        cl_1v1_wins = int(row.get("cl_1v1_wins") or 0)
        kills = int(row.get("kills") or 0)
        clutch_kills = int(row.get("clutch_kills") or 0)

        round_den = rounds_played if rounds_played > 0 else 1
        awp_rate = sniper_kills / round_den
        entry_success = (entry_wins / entry_count) if entry_count > 0 else 0.0
        assist_rate = assists / round_den
        clutch_success = (cl_1v1_wins / cl_1v1_attempts) if cl_1v1_attempts > 0 else 0.0
        entry_rate = entry_count / round_den
        utility_per_round = utility_damage / round_den
        flashed_per_round = enemies_flashed / round_den
        clutch_attempt_rate = cl_1v1_attempts / round_den
        frag_rate = kills / round_den
        clutch_kill_rate = clutch_kills / round_den

        players.append({
            "player_id": row["player_id"],
            "nickname": row["nickname"],
            "maps_played": maps_played,
            "rounds_played": rounds_played,
            "entry_count": entry_count,
            "cl_1v1_attempts": cl_1v1_attempts,
            "awp_rate": awp_rate,
            "entry_success": entry_success,
            "assist_rate": assist_rate,
            "clutch_success": clutch_success,
            "entry_rate": entry_rate,
            "utility_per_round": utility_per_round,
            "flashed_per_round": flashed_per_round,
            "clutch_attempt_rate": clutch_attempt_rate,
            "frag_rate": frag_rate,
            "clutch_kill_rate": clutch_kill_rate,
        })

    stable_players = [
        p for p in players
        if p["maps_played"] >= MIN_MAPS and p["rounds_played"] >= MIN_ROUNDS
    ]
    sample_players = stable_players if len(stable_players) >= 3 else players

    def sample_values(metric: str, predicate=None) -> list[float]:
        values = []
        for p in sample_players:
            if predicate and not predicate(p):
                continue
            values.append(float(p[metric]))
        return values

    awp_values = sample_values("awp_rate")
    entry_rate_values = sample_values("entry_rate")
    assist_values = sample_values("assist_rate")
    utility_values = sample_values("utility_per_round")
    flashed_values = sample_values("flashed_per_round")
    clutch_attempt_values = sample_values("clutch_attempt_rate")
    frag_values = sample_values("frag_rate")
    clutch_kill_values = sample_values("clutch_kill_rate")

    entry_success_values = sample_values("entry_success", lambda p: p["entry_count"] >= MIN_ENTRY_ATTEMPTS)
    clutch_success_values = sample_values("clutch_success", lambda p: p["cl_1v1_attempts"] >= MIN_CLUTCH_ATTEMPTS)

    entry_success_median = _median(entry_success_values) if entry_success_values else 0.45
    clutch_success_median = _median(clutch_success_values) if clutch_success_values else 0.35
    assist_median = _median(assist_values) if assist_values else 0.12
    utility_median = _median(utility_values) if utility_values else 0.0
    flashed_median = _median(flashed_values) if flashed_values else 0.0
    frag_median = _median(frag_values) if frag_values else 0.0
    clutch_kill_median = _median(clutch_kill_values) if clutch_kill_values else 0.0

    players_with_roles = []
    for p in players:
        awp_rate = p["awp_rate"]
        entry_success = p["entry_success"]
        assist_rate = p["assist_rate"]
        clutch_success = p["clutch_success"]
        entry_count = p["entry_count"]
        cl_1v1_attempts = p["cl_1v1_attempts"]

        awp_pct = _percentile_rank(awp_values, awp_rate)
        entry_rate_pct = _percentile_rank(entry_rate_values, p["entry_rate"])
        assist_pct = _percentile_rank(assist_values, assist_rate)
        utility_pct = _percentile_rank(utility_values, p["utility_per_round"])
        flashed_pct = _percentile_rank(flashed_values, p["flashed_per_round"])
        clutch_attempt_pct = _percentile_rank(clutch_attempt_values, p["clutch_attempt_rate"])
        frag_pct = _percentile_rank(frag_values, p["frag_rate"])
        clutch_kill_pct = _percentile_rank(clutch_kill_values, p["clutch_kill_rate"])
        entry_success_pct = _percentile_rank(entry_success_values, entry_success) if entry_success_values else 0.0
        clutch_success_pct = _percentile_rank(clutch_success_values, clutch_success) if clutch_success_values else 0.0

        awp_score = (0.6 * awp_pct) + (0.4 * _clamp01((awp_rate - 0.04) / 0.16))
        entry_score = (0.55 * entry_rate_pct) + (0.45 * entry_success_pct)
        support_score = (0.45 * assist_pct) + (0.35 * utility_pct) + (0.20 * flashed_pct)
        clutch_score = (0.55 * clutch_success_pct) + (0.45 * clutch_attempt_pct)
        utility_expert_score = (0.6 * utility_pct) + (0.4 * flashed_pct)
        playmaker_score = (0.5 * frag_pct) + (0.3 * entry_rate_pct) + (0.2 * clutch_attempt_pct)
        closer_score = (0.55 * clutch_success_pct) + (0.30 * clutch_kill_pct) + (0.15 * frag_pct)

        candidates: list[tuple[str, float]] = []
        if awp_rate >= 0.04 and awp_score >= 0.56:
            candidates.append(("AWPer", awp_score))
        if entry_count >= MIN_ENTRY_ATTEMPTS and entry_success >= max(0.42, entry_success_median) and entry_score >= 0.55:
            candidates.append(("Entry Fragger", entry_score))
        if (assist_rate >= max(0.10, assist_median * 0.9) or p["utility_per_round"] >= 3.0) and support_score >= 0.53:
            candidates.append(("Support", support_score))
        if cl_1v1_attempts >= MIN_CLUTCH_ATTEMPTS and clutch_success >= max(0.30, clutch_success_median) and clutch_score >= 0.57:
            candidates.append(("Clutcher", clutch_score))
        if (
            p["utility_per_round"] >= max(2.2, utility_median * 0.9)
            and p["flashed_per_round"] >= max(0.08, flashed_median * 0.85)
            and utility_expert_score >= 0.58
        ):
            candidates.append(("Utility Expert", utility_expert_score))
        if p["frag_rate"] >= max(0.60, frag_median * 0.95) and playmaker_score >= 0.58:
            candidates.append(("Playmaker", playmaker_score))
        if (
            cl_1v1_attempts >= MIN_CLUTCH_ATTEMPTS
            and clutch_success >= max(0.30, clutch_success_median)
            and p["clutch_kill_rate"] >= max(0.012, clutch_kill_median * 0.9)
            and closer_score >= 0.60
        ):
            candidates.append(("Closer", closer_score))

        # Identity badges with lighter gates so every player gets a meaningful style label.
        initiator_score = (0.65 * entry_rate_pct) + (0.35 * entry_success_pct)
        sharpshooter_score = (0.70 * awp_pct) + (0.30 * awp_score)
        anchor_score = (0.50 * (1.0 - entry_rate_pct)) + (0.30 * assist_pct) + (0.20 * utility_pct)
        utility_core_score = (0.45 * utility_pct) + (0.35 * flashed_pct) + (0.20 * assist_pct)
        team_player_score = (0.40 * assist_pct) + (0.30 * utility_pct) + (0.30 * clutch_attempt_pct)

        if entry_count >= 12 and initiator_score >= 0.50:
            candidates.append(("Initiator", initiator_score))
        if awp_rate >= 0.02 and sharpshooter_score >= 0.50:
            candidates.append(("Sharpshooter", sharpshooter_score))
        if p["entry_rate"] <= max(0.12, _median(entry_rate_values) if entry_rate_values else 0.12) and anchor_score >= 0.50:
            candidates.append(("Anchor", anchor_score))
        if utility_core_score >= 0.50:
            candidates.append(("Utility Core", utility_core_score))
        if team_player_score >= 0.48:
            candidates.append(("Team Player", team_player_score))

        best_by_name: dict[str, float] = {}
        for name, score in candidates:
            previous = best_by_name.get(name)
            if previous is None or score > previous:
                best_by_name[name] = score
        ranked = sorted(best_by_name.items(), key=lambda item: item[1], reverse=True)
        roles = ["Rifler"]
        fallback_pool = [
            ("Sharpshooter", sharpshooter_score),
            ("Initiator", initiator_score),
            ("Utility Core", utility_core_score),
            ("Anchor", anchor_score),
            ("Team Player", team_player_score),
        ]
        fallback_ranked = sorted(fallback_pool, key=lambda item: item[1], reverse=True)

        secondary_ranked = [(name, score) for name, score in ranked if score >= 0.58]
        if not secondary_ranked:
            secondary_ranked = fallback_ranked

        for name, _score in secondary_ranked:
            if len(roles) >= 3:
                break
            if name not in roles:
                roles.append(name)

        players_with_roles.append({
            "player_id": p["player_id"],
            "nickname": p["nickname"],
            "maps_played": p["maps_played"],
            "roles": roles,
            "primary_role": "Rifler",
            "role_stats": {
                "awp_rate": round(awp_rate * 100, 1),
                "entry_success": round(entry_success * 100, 1),
                "assist_rate": round(assist_rate * 100, 1),
                "clutch_success": round(clutch_success * 100, 1)
            }
        })

    return players_with_roles


async def fetch_comprehensive_team_season(team_id: str, championship_id: str) -> dict[str, Any]:
    """Fetch all comprehensive team season data in one call."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Verify team exists in this championship
    team_check = await query_async(
        """
        SELECT tst.* FROM team_season_totals tst
        WHERE tst.season = :season AND tst.division_num = :div AND tst.team_id = :team_id
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found in championship {championship_id}")
    
    # Fetch all components
    try:
        team_stats = (await fetch_team_season_stats(team_id))
        team_stats = next((s for s in team_stats if s.get("championship_id") == championship_id), None)
    except NotFoundError:
        team_stats = None
    if team_stats:
        team_stats = dict(team_stats)
    
    try:
        map_stats_raw = await fetch_team_map_stats_comprehensive(championship_id, team_id)
        map_stats = [dict(row) for row in map_stats_raw]
    except NotFoundError:
        map_stats = []
    
    try:
        matches_raw = await fetch_team_matches(team_id, championship_id)
        matches = _normalize_matches_for_page(matches_raw, team_id)
    except NotFoundError:
        matches = []
    
    try:
        players_raw = await fetch_team_players_comprehensive(team_id, championship_id)
        players = [dict(row) for row in players_raw]
    except NotFoundError:
        players = []
    
    try:
        veto_history_raw = await fetch_team_veto_history(team_id, championship_id)
        veto_history = [dict(row) for row in veto_history_raw]
    except NotFoundError:
        veto_history = []
    
    try:
        veto_aggregates_raw = await fetch_team_veto_aggregates(team_id, championship_id)
        veto_aggregates = [dict(row) for row in veto_aggregates_raw]
    except NotFoundError:
        veto_aggregates = []
    
    # Fetch Phase 1 enhancements (division averages and player roles)
    try:
        division_averages = await get_division_averages(championship_id)
    except Exception as e:
        print(f"Error fetching division averages: {e}")
        division_averages = {}
    
    try:
        player_roles = await detect_player_roles(championship_id, team_id)
    except Exception as e:
        print(f"Error fetching player roles: {e}")
        player_roles = []
    
    return {
        "championship_id": championship_id,
        "season": season,
        "division_num": division_num,
        "team_stats": team_stats,
        "map_stats": map_stats,
        "match_history": matches,
        "player_stats": players,
        "veto_history": veto_history,
        "veto_aggregates": veto_aggregates,
        # Phase 1 enhancements
        "division_averages": division_averages,
        "player_roles": player_roles,
    }

