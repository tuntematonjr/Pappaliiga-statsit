from __future__ import annotations

from typing import Any, Collection, Dict, Optional
import json
from datetime import datetime, timezone

from db_async import compute_team_map_deltas_async, get_team_matches_mirror_async, query_async
from standings_utils import calculate_standings, get_team_position

from api.exceptions import NotFoundError

DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


async def fetch_team(team_id: str) -> dict[str, Any]:
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


async def fetch_team_season_stats(team_id: str) -> list[dict[str, Any]]:
    rows = await query_async(
        """
         SELECT tst.season, tst.division_num, c.championship_id,
             c.name,
             c.is_playoffs,
             tst.maps_played, tst.matches_played, tst.matches_won AS wins,
               GREATEST(
                   CAST(tst.matches_played AS SIGNED) - CAST(tst.matches_won AS SIGNED),
                   0
               ) AS losses,
               CASE WHEN tst.matches_played > 0
                    THEN (tst.matches_won / tst.matches_played)
                    ELSE 0.0 END AS win_rate,
               tst.rounds_won, tst.rounds_lost, tst.maps_won
        FROM team_season_totals tst
        JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
        WHERE tst.team_id = :team_id
        ORDER BY tst.season DESC, tst.division_num
        """,
        {"team_id": team_id},
    )
    if not rows:
        raise NotFoundError(f"No stats found for team '{team_id}'")
    return rows


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


async def fetch_team_map_stats(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    map_deltas = await compute_team_map_deltas_async(championship_id, team_id)
    if not map_deltas:
        raise NotFoundError(
            f"No map stats found for team '{team_id}' in championship {championship_id}"
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


async def list_teams(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    limit: int,
) -> list[dict[str, Any]]:
    if season is not None and division is not None:
        rows = await query_async(
            """
            SELECT DISTINCT t.team_id, t.name AS team_name, t.name AS display_name, t.avatar
            FROM teams t
            JOIN team_season_totals tst ON tst.team_id = t.team_id
            WHERE tst.season = :season AND tst.division_num = :division
            ORDER BY t.name, t.team_id
            LIMIT :limit
            """,
            {"season": season, "division": division, "limit": limit},
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
        JOIN matches m ON m.match_id = ps.match_id
        LEFT JOIN players p ON p.player_id = ps.player_id
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
        stats_raw = {
            "Kills": row.get("kills") or 0,
            "Deaths": row.get("deaths") or 0,
            "Assists": row.get("assists") or 0,
            "MVPs": row.get("mvps") or 0,
            "Headshots": row.get("headshots") or 0,
            "Damage": row.get("damage") or 0,
            "Sniper Kills": row.get("sniper_kills") or 0,
            "Pistol Kills": row.get("pistol_kills") or 0,
            "Knife Kills": row.get("knife_kills") or 0,
            "Zeus Kills": row.get("zeus_kills") or 0,
            "First Kills": row.get("first_kills") or 0,
            "Enemies Flashed": row.get("enemies_flashed") or 0,
            "Flash Count": row.get("flash_count") or 0,
            "Flash Successes": row.get("flash_successes") or 0,
            "Utility Damage": row.get("utility_damage") or 0,
            "Utility Count": row.get("utility_count") or 0,
            "Utility Successes": row.get("utility_successes") or 0,
            "Utility Enemies": row.get("utility_enemies") or 0,
            "Double Kills": row.get("mk_2k") or 0,
            "Triple Kills": row.get("mk_3k") or 0,
            "Quadro Kills": row.get("mk_4k") or 0,
            "Penta Kills": row.get("mk_5k") or 0,
            "Clutch Kills": row.get("clutch_kills") or 0,
            "1v1Count": row.get("cl_1v1_attempts") or 0,
            "1v1Wins": row.get("cl_1v1_wins") or 0,
            "1v2Count": row.get("cl_1v2_attempts") or 0,
            "1v2Wins": row.get("cl_1v2_wins") or 0,
            "Entry Count": row.get("entry_count") or 0,
            "Entry Wins": row.get("entry_wins") or 0,
            "K/D Ratio": row.get("kd") or 0.0,
            "K/R Ratio": row.get("kr") or 0.0,
            "ADR": row.get("adr") or 0.0,
            "Headshots %": row.get("hs_pct") or 0.0,
            "Result": row.get("result") or 0,
        }
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


async def fetch_team_players(team_id: str, championship_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Fetch team's players. If championship_id provided, filter to that championship."""
    # Verify team exists
    team_check = await query_async(
        "SELECT team_id FROM teams WHERE team_id = :team_id",
        {"team_id": team_id}
    )
    if not team_check:
        raise NotFoundError(f"Team '{team_id}' not found")
    
    # Get championship for filtering - use one with actual player data
    if championship_id is None:
        # Get the latest championship with actual player data for this team
        champ_rows = await query_async(
            """
            SELECT DISTINCT c.championship_id
            FROM team_season_totals tst
            JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num
            WHERE tst.team_id = :team_id
            AND EXISTS (
                SELECT 1 FROM player_stats ps
                JOIN matches m ON m.match_id = ps.match_id
                WHERE m.championship_id = c.championship_id AND ps.team_id = :team_id
            )
            ORDER BY tst.season DESC
            LIMIT 1
            """,
            {"team_id": team_id}
        )
        if not champ_rows:
            raise NotFoundError(f"No championship found for team '{team_id}'")
        championship_id = champ_rows[0]["championship_id"]
    
    champ_info = await query_async(
        "SELECT season, division_num, is_playoffs FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_info:
        raise NotFoundError(f"Championship {championship_id} not found")
    season = champ_info[0]["season"]
    division_num = champ_info[0]["division_num"]
    is_playoffs = bool(champ_info[0].get("is_playoffs"))

    if not is_playoffs:
        rows = await query_async(
            """
            SELECT
                pp.player_id,
                pp.nickname,
                pst.maps_played AS matches_played,
                pst.kills,
                pst.deaths,
                pst.damage,
                pst.adr,
                pst.headshots
            FROM player_season_totals pst
            JOIN players pp ON pp.player_id = pst.player_id
            WHERE pst.season = :season
              AND pst.division_num = :division
              AND pst.team_id = :team_id
            ORDER BY pst.maps_played DESC, pst.kills DESC
            """,
            {"season": season, "division": division_num, "team_id": team_id},
        )
    else:
        rows = await query_async(
            """
            SELECT
                pp.player_id,
                pp.nickname,
                COUNT(DISTINCT ps.match_id) AS matches_played,
                SUM(COALESCE(ps.kills, 0)) AS kills,
                SUM(COALESCE(ps.deaths, 0)) AS deaths,
                SUM(COALESCE(ps.damage, 0)) AS damage,
                AVG(NULLIF(ps.adr, 0)) AS adr,
                SUM(COALESCE(ps.headshots, 0)) AS headshots
            FROM player_stats ps
            JOIN players pp ON pp.player_id = ps.player_id
            JOIN matches m ON m.match_id = ps.match_id
            WHERE m.championship_id = :champ_id AND ps.team_id = :team_id
            GROUP BY pp.player_id, pp.nickname
            ORDER BY matches_played DESC, kills DESC
            """,
            {"champ_id": championship_id, "team_id": team_id}
        )
    
    if not rows:
        raise NotFoundError(f"No players found for team '{team_id}' in championship {championship_id}")
    
    return rows


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


async def fetch_team_page(team_id: str, championship_id: Optional[str] = None) -> dict[str, Any]:
    """Return consolidated payload for the team page (profile, seasons, selected season data)."""
    team = await fetch_team(team_id)

    try:
        seasons = await fetch_team_season_stats(team_id)
    except NotFoundError:
        seasons = []

    available_champs = {row.get("championship_id") for row in seasons if row.get("championship_id")}
    selected_champ = championship_id or None
    if selected_champ:
        if selected_champ not in available_champs and available_champs:
            raise NotFoundError(f"Championship {selected_champ} not found for team '{team_id}'")
    elif available_champs:
        # Default to most recent season (already ordered desc in query)
        selected_champ = seasons[0]["championship_id"]

    season_payload: dict[str, Any] | None = None
    if selected_champ:
        stats = next((s for s in seasons if s.get("championship_id") == selected_champ), None)
        try:
            map_stats = await fetch_team_map_stats(selected_champ, team_id)
        except NotFoundError:
            map_stats = []
        try:
            matches = await fetch_team_matches(team_id, selected_champ)
        except NotFoundError:
            matches = []
        try:
            players = await fetch_team_players(team_id, selected_champ)
        except NotFoundError:
            players = []

        season_payload = {
            "championship_id": selected_champ,
            "stats": stats,
            "map_stats": map_stats,
            "matches": _normalize_matches_for_page(matches, team_id),
            "players": players,
        }

    return {
        "team": team,
        "seasons": seasons,
        "current_championship_id": selected_champ,
        "season_data": season_payload,
    }


async def fetch_team_map_stats_comprehensive(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    """Fetch comprehensive map statistics for a team in a championship with player metrics aggregated by map."""
    champ_rows = await query_async(
        "SELECT season, division_num, is_playoffs FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    is_playoffs = bool(champ.get("is_playoffs"))
    
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
    
    # Enhance with per-player stats aggregated by map
    if not is_playoffs:
        player_stats_by_map = await query_async(
            """
            SELECT
                pm.map_name,
                SUM(pm.maps_played) AS stat_count,
                SUM(pm.kills) AS kills,
                SUM(pm.deaths) AS deaths,
                SUM(pm.assists) AS assists,
                SUM(pm.mvps) AS mvps,
                SUM(pm.damage) AS damage,
                SUM(pm.utility_damage) AS utility_damage,
                AVG(NULLIF(pm.kr, 0)) AS kr,
                AVG(NULLIF(pm.hs_pct, 0)) AS hs_pct,
                SUM(pm.sniper_kills) AS sniper_kills,
                SUM(pm.pistol_kills) AS pistol_kills,
                SUM(pm.mk_2k) AS multi_2k,
                SUM(pm.mk_3k) AS multi_3k,
                SUM(pm.mk_4k) AS multi_4k,
                SUM(pm.mk_5k) AS multi_5k,
                SUM(pm.clutch_kills) AS clutch_kills,
                SUM(pm.enemies_flashed) AS enemies_flashed,
                SUM(pm.flash_count) AS flash_count,
                SUM(pm.flash_successes) AS flash_successes
            FROM player_map_season_totals pm
            WHERE pm.season = :season
              AND pm.division_num = :division
              AND pm.team_id = :team_id
            GROUP BY pm.map_name
            """,
            {"season": season, "division": division_num, "team_id": team_id},
        )
    else:
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
    if not is_playoffs:
        rounds_by_map_rows = await query_async(
            """
            SELECT
                map_name,
                SUM(COALESCE(kills, 0)) AS rounds_won,
                SUM(COALESCE(deaths, 0)) AS rounds_lost
            FROM team_map_season_totals
            WHERE season = :season AND division_num = :division AND team_id = :team_id
            GROUP BY map_name
            """,
            {"season": season, "division": division_num, "team_id": team_id},
        )
    else:
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
        estimated_rounds = played * 30
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
            p.nickname
        FROM player_season_totals pst
        LEFT JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season AND pst.division_num = :div AND pst.team_id = :team_id
        ORDER BY pst.maps_played DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id}
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


async def _calculate_h2h_stats(
    championship_id: str,
    team_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """
    Calculate head-to-head stats between specified teams.
    
    Returns dict mapping team_id to {h2h_maps_won, h2h_round_diff}
    based only on matches between the specified teams.
    """
    if len(team_ids) < 2:
        return {}
    
    # Get all matches between these teams
    placeholders = ', '.join([f':team{i}' for i in range(len(team_ids))])
    params = {"champ_id": championship_id}
    for i, tid in enumerate(team_ids):
        params[f'team{i}'] = tid
    
    matches = await query_async(
        f"""
        SELECT
            match_id,
            team1_id,
            team2_id,
            winner_team_id,
            status,
            best_of
        FROM matches
        WHERE championship_id = :champ_id
          AND status = 'FINISHED'
          AND team1_id IN ({placeholders})
          AND team2_id IN ({placeholders})
        """,
        params
    )
    
    # Get map results for these matches
    match_ids = [m['match_id'] for m in matches]
    if not match_ids:
        return {tid: {'h2h_maps_won': 0, 'h2h_round_diff': 0} for tid in team_ids}
    
    map_placeholders = ', '.join([f':mid{i}' for i in range(len(match_ids))])
    map_params = {}
    for i, mid in enumerate(match_ids):
        map_params[f'mid{i}'] = mid
    
    maps = await query_async(
        f"""
        SELECT
            match_id,
            winner_team_id,
            is_forfeit,
            score_team1,
            score_team2
        FROM maps
        WHERE match_id IN ({map_placeholders})
        """,
        map_params
    )
    
    # Build match lookup
    match_lookup = {m['match_id']: m for m in matches}
    
    # Calculate h2h stats per team
    h2h_stats = {tid: {'h2h_maps_won': 0, 'h2h_round_diff': 0} for tid in team_ids}
    
    for map_row in maps:
        match = match_lookup.get(map_row['match_id'])
        if not match:
            continue
        
        team1_id = match['team1_id']
        team2_id = match['team2_id']
        winner_id = map_row['winner_team_id']
        
        # Count map win
        if winner_id and winner_id in h2h_stats:
            h2h_stats[winner_id]['h2h_maps_won'] += 1
        
        # Count rounds
        if map_row['is_forfeit']:
            # Forfeit: 13-0
            if winner_id == team1_id:
                h2h_stats[team1_id]['h2h_round_diff'] += 13
                h2h_stats[team2_id]['h2h_round_diff'] -= 13
            elif winner_id == team2_id:
                h2h_stats[team2_id]['h2h_round_diff'] += 13
                h2h_stats[team1_id]['h2h_round_diff'] -= 13
        else:
            # Actual score
            t1_score = int(map_row['score_team1'] or 0)
            t2_score = int(map_row['score_team2'] or 0)
            h2h_stats[team1_id]['h2h_round_diff'] += (t1_score - t2_score)
            h2h_stats[team2_id]['h2h_round_diff'] += (t2_score - t1_score)
    
    return h2h_stats


async def get_division_standings(championship_id: str) -> list[dict[str, Any]]:
    """
    Get division standings following official Pappaliiga rules:
    1. Matches won
    2. Round differential  
    3. Head-to-head maps (between tied teams)
    4. Head-to-head round differential (between tied teams)
    5. Alphabetical
    """
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    rows = await query_async(
        """
        SELECT
            tst.team_id,
            t.name AS team_name,
            tst.matches_played,
            tst.matches_won,
            GREATEST(CAST(tst.matches_played AS SIGNED) - CAST(tst.matches_won AS SIGNED), 0) AS matches_lost,
            tst.maps_played,
            tst.maps_won,
            GREATEST(CAST(tst.maps_played AS SIGNED) - CAST(tst.maps_won AS SIGNED), 0) AS maps_lost,
            tst.rounds_won,
            tst.rounds_lost,
            (CAST(tst.rounds_won AS SIGNED) - CAST(tst.rounds_lost AS SIGNED)) AS round_diff,
            CASE WHEN tst.matches_played > 0 THEN (tst.matches_won / tst.matches_played) * 100 ELSE 0 END AS win_rate
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE tst.season = :season AND tst.division_num = :div
        """,
        {"season": season, "div": division_num}
    )
    
    if not rows:
        return []
    
    # Convert to list of dicts
    teams = [dict(row) for row in rows]
    
    # Calculate h2h stats for potential tiebreakers
    # Only calculated between teams - used when tied on wins and round_diff
    team_ids = [t['team_id'] for t in teams]
    h2h_data = await _calculate_h2h_stats(championship_id, team_ids)
    
    # Use standings utility with official Pappaliiga tiebreakers
    sorted_teams = calculate_standings(
        teams,
        primary_key='matches_won',
        tiebreakers=['round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name'],
        h2h_data=h2h_data
    )
    
    return sorted_teams


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


async def detect_player_roles(championship_id: str, team_id: str) -> list[dict[str, Any]]:
    """Detect player roles based on statistical patterns."""
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
            p.nickname,
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
        WHERE pst.season = :season AND pst.division_num = :div AND pst.team_id = :team_id
        ORDER BY pst.maps_played DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    
    players_with_roles = []
    for row in rows:
        awp_rate = float(row.get("awp_rate") or 0.0)
        entry_success = float(row.get("entry_success") or 0.0)
        assist_rate = float(row.get("assist_rate") or 0.0)
        clutch_success = float(row.get("clutch_success") or 0.0)
        entry_count = int(row.get("entry_count") or 0)
        utility_damage = int(row.get("utility_damage") or 0)
        enemies_flashed = int(row.get("enemies_flashed") or 0)
        
        # Role detection logic
        roles = []
        if awp_rate > 0.15:  # More than 15% of rounds using AWP
            roles.append("AWPer")
        if entry_count > 50 and entry_success > 0.45:  # High entry attempts with decent success
            roles.append("Entry Fragger")
        if assist_rate > 0.15 or (utility_damage > 1000 and enemies_flashed > 100):
            roles.append("Support")
        if clutch_success > 0.35 and row.get("cl_1v1_attempts", 0) > 10:
            roles.append("Clutcher")
        
        # Default to Rifler if no specific role identified
        if not roles:
            roles.append("Rifler")
        
        players_with_roles.append({
            "player_id": row["player_id"],
            "nickname": row["nickname"],
            "maps_played": int(row["maps_played"] or 0),
            "roles": roles,
            "primary_role": roles[0] if roles else "Rifler",
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

