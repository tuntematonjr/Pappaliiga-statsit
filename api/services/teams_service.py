from __future__ import annotations

from typing import Any, Collection, Dict, Optional
from datetime import datetime, timezone

from async_db import compute_team_map_deltas_async, get_team_matches_mirror_async, query_async

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
               tst.maps_played, tst.matches_played, tst.matches_won AS wins,
               (tst.matches_played - tst.matches_won) AS losses,
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
            "maps": match.get("maps", [])
        })
    
    return result


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
    
    # Query player stats for the team in this championship
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
            SUM(COALESCE(ps.mvps, 0)) AS headshots
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
    """Fetch comprehensive map statistics for a team in a championship."""
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")
    
    champ = champ_rows[0]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Get team map totals
    rows = await query_async(
        """
        SELECT
            map_name, played, picks, opp_picks, wins, games, ban1, ban2, opp_ban, 
            total_own_ban, decov, kills, deaths, mvps, rd, kd, adr, damage, utility_damage
        FROM team_map_season_totals
        WHERE season = :season AND division_num = :div AND team_id = :team_id
        ORDER BY played DESC, wins DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    
    if not rows:
        raise NotFoundError(f"No map stats found for team '{team_id}' in championship {championship_id}")
    
    # Add calculated fields
    result = []
    for row in rows:
        data = dict(row)
        played = data.get("played") or 0
        picks = data.get("picks") or 0
        wins = data.get("wins") or 0
        
        data["winrate"] = (wins / played * 100) if played > 0 else 0.0
        data["pick_rate"] = (picks / played * 100) if played > 0 else 0.0
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
            player_id, maps_played, rounds_played, kills, deaths, assists, mvps, 
            sniper_kills, utility_damage, enemies_flashed, flash_count, flash_successes,
            mk_2k, mk_3k, mk_4k, mk_5k, clutch_kills, cl_1v1_attempts, cl_1v1_wins,
            cl_1v2_attempts, cl_1v2_wins, entry_count, entry_wins, pistol_kills,
            adr, kr, kd, rating, hs_pct, damage
        FROM player_season_totals
        WHERE season = :season AND division_num = :div AND team_id = :team_id
        ORDER BY maps_played DESC, rating DESC
        """,
        {"season": season, "div": division_num, "team_id": team_id}
    )
    
    if not rows:
        raise NotFoundError(f"No players found for team '{team_id}' in championship {championship_id}")
    
    # Get player names
    player_ids = [r["player_id"] for r in rows]
    if player_ids:
        player_names = await query_async(
            f"""
            SELECT player_id, nickname
            FROM players
            WHERE player_id IN ({','.join([':p' + str(i) for i in range(len(player_ids))])})
            """,
            {f"p{i}": pid for i, pid in enumerate(player_ids)}
        )
        name_map = {p["player_id"]: p["nickname"] for p in player_names}
        
        for row in rows:
            row["nickname"] = name_map.get(row["player_id"], "Unknown")
    else:
        for row in rows:
            row["nickname"] = "Unknown"
    
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
    
    return {
        "championship_id": championship_id,
        "season": season,
        "division_num": division_num,
        "team_stats": team_stats,
        "map_stats": map_stats,
        "match_history": matches,
        "player_stats": players,
        "veto_history": veto_history,
        "veto_aggregates": veto_aggregates
    }

