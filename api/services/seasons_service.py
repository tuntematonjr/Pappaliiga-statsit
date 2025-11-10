"""Season-specific service functions for aggregated statistics."""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from async_db import query_async
from division_overrides import combined_status_teams

logger = logging.getLogger("pappaliiga.api.seasons")

async def get_seasons_list() -> List[Dict[str, Any]]:
    """Return list of all seasons with metadata."""
    rows = await query_async(
        """
        SELECT 
            c.season,
            MIN(m.started_at) AS start_date,
            MAX(m.finished_at) AS end_date,
            COUNT(DISTINCT c.championship_id) AS divisions_count,
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS finished_matches,
            COUNT(DISTINCT m.match_id) AS total_matches
        FROM championships c
        LEFT JOIN matches m ON m.championship_id = c.championship_id
        GROUP BY c.season
        ORDER BY c.season DESC
        """
    )
    
    seasons = []
    for row in rows:
        season_num = int(row["season"])
        start_ts = row.get("start_date")
        end_ts = row.get("end_date")
        finished = int(row.get("finished_matches") or 0)
        total = int(row.get("total_matches") or 0)
        
        # Determine status based on current time and match completion
        status = "finished"
        if end_ts is None or end_ts > int(time.time()):
            if finished > 0:
                status = "active"
            else:
                status = "upcoming"
        
        seasons.append({
            "id": season_num,
            "name": f"Season {season_num}",
            "status": status,
            "start_date": _format_timestamp(start_ts) if start_ts else None,
            "end_date": _format_timestamp(end_ts) if end_ts else None,
            "divisions_count": int(row.get("divisions_count") or 0),
        })
    
    return seasons


async def get_season_summary(season: int) -> Dict[str, Any]:
    """Return comprehensive aggregated statistics for a season."""
    
    # Get team counts and match data
    team_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT tst.team_id) AS total_teams,
            COALESCE(SUM(tst.matches_played), 0) AS matches_played,
            COALESCE(SUM(tst.matches_won), 0) AS matches_won,
            COALESCE(SUM(tst.maps_played), 0) AS maps_played,
            COALESCE(SUM(tst.rounds_won), 0) AS rounds_won,
            COALESCE(SUM(tst.rounds_lost), 0) AS rounds_lost
        FROM team_season_totals tst
        WHERE tst.season = :season
        """,
        {"season": season},
    )
    team_data = team_rows[0] if team_rows else {}
    
    # Get player counts and stats
    player_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT pst.player_id) AS total_players,
            COALESCE(SUM(pst.kills), 0) AS total_kills,
            COALESCE(SUM(pst.deaths), 0) AS total_deaths,
            COALESCE(AVG(NULLIF(pst.adr, 0)), 0) AS avg_adr,
            COALESCE(SUM(pst.utility_damage), 0) AS utility_damage,
            COALESCE(SUM(pst.cl_1v1_wins + pst.cl_1v2_wins), 0) AS clutch_wins,
            COALESCE(SUM(pst.entry_wins - (pst.entry_count - pst.entry_wins)), 0) AS entry_diff
        FROM player_season_totals pst
        WHERE pst.season = :season
        """,
        {"season": season},
    )
    player_data = player_rows[0] if player_rows else {}
    
    # Get division progress
    progress_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT c.championship_id) AS total_divisions,
            COUNT(DISTINCT CASE 
                WHEN (
                    SELECT COUNT(*) 
                    FROM matches m2 
                    WHERE m2.championship_id = c.championship_id 
                    AND m2.finished_at IS NULL
                ) = 0 
                THEN c.championship_id 
            END) AS finished_divisions
        FROM championships c
        WHERE c.season = :season
        """,
        {"season": season},
    )
    progress_data = progress_rows[0] if progress_rows else {}
    
    teams = int(team_data.get("total_teams") or 0)
    players = int(player_data.get("total_players") or 0)
    matches = int(team_data.get("matches_played") or 0)
    matches_won = int(team_data.get("matches_won") or 0)
    rounds = int(team_data.get("rounds_won") or 0) + int(team_data.get("rounds_lost") or 0)
    kills = int(player_data.get("total_kills") or 0)
    deaths = int(player_data.get("total_deaths") or 0)
    
    total_divisions = int(progress_data.get("total_divisions") or 0)
    finished_divisions = int(progress_data.get("finished_divisions") or 0)
    
    return {
        "season_id": season,
        "teams": teams,
        "players": players,
        "matches": matches,
        "rounds": rounds,
        "kills": kills,
        "deaths": deaths,
        "win_rate": float(matches_won) / matches if matches > 0 else 0.0,
        "kd_ratio": float(kills) / deaths if deaths > 0 else 0.0,
        "adr_avg": float(player_data.get("avg_adr") or 0.0),
        "clutch_wins": int(player_data.get("clutch_wins") or 0),
        "entry_diff": int(player_data.get("entry_diff") or 0),
        "utility_damage": int(player_data.get("utility_damage") or 0),
        "finished_percent": (float(finished_divisions) / total_divisions * 100) if total_divisions > 0 else 0.0,
        "progress": {
            "divisions_finished": finished_divisions,
            "divisions_total": total_divisions,
        },
    }


async def get_season_divisions(season: int) -> List[Dict[str, Any]]:
    """Return list of divisions for a season with embedded stats.
    
    Note: Only returns regular season divisions. Playoff data is embedded within each division.
    """
    
    # Get all divisions for the season (including playoffs for data lookup)
    divisions_rows = await query_async(
        """
        SELECT 
            c.championship_id,
            c.season,
            c.division_num,
            c.name,
            c.slug,
            c.is_playoffs,
            c.parent_championship_id,
            COUNT(DISTINCT CASE WHEN m.is_forfeit = 0 THEN ct.team_id END) AS teams_count,
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
            COUNT(DISTINCT m.match_id) AS total_matches,
            MIN(m.started_at) AS first_started,
            MAX(m.finished_at) AS last_finished
        FROM championships c
        LEFT JOIN matches m ON m.championship_id = c.championship_id
        LEFT JOIN (
            SELECT DISTINCT match_id, team1_id AS team_id FROM matches
            UNION
            SELECT DISTINCT match_id, team2_id AS team_id FROM matches
        ) ct ON ct.match_id = m.match_id
        WHERE c.season = :season
        GROUP BY c.championship_id, c.season, c.division_num, c.name, c.slug, c.is_playoffs, c.parent_championship_id
        ORDER BY c.is_playoffs ASC, c.division_num ASC
        """,
        {"season": season},
    )
    
    # Separate regular divisions from playoffs
    regular_divisions = [row for row in divisions_rows if not row["is_playoffs"]]
    playoff_divisions = {str(row["parent_championship_id"]): row for row in divisions_rows if row["is_playoffs"] and row["parent_championship_id"]}
    
    # Get winners for all divisions
    champ_ids = [str(row["championship_id"]) for row in divisions_rows]
    winners_map = await _fetch_division_winners_map(champ_ids) if champ_ids else {}
    
    # Get best players per division
    best_players_map = await _fetch_best_players_map(season)
    
    # Get MVP teams per division
    mvp_teams_map = await _fetch_mvp_teams_map(season)
    
    divisions = []
    
    # Only process regular divisions for the main list
    for row in regular_divisions:
        champ_id = str(row["championship_id"])
        division_num = int(row["division_num"])
        teams_count = int(row.get("teams_count") or 0)
        played = int(row.get("played_matches") or 0)
        total = int(row.get("total_matches") or 0)
        
        # Determine status
        status = "waiting"
        if played > 0 and played < total:
            status = "active"
        elif played == total and total > 0:
            status = "finished"
        
        finished_percent = (float(played) / total * 100) if total > 0 else 0.0
        
        # Get tier (division_num is typically the tier)
        tier = division_num
        
        winners_data = winners_map.get(champ_id, [])
        best_player = best_players_map.get((season, division_num))
        mvp_team = mvp_teams_map.get((season, division_num))
        
        division_data = {
            "division_id": champ_id,
            "tier": tier,
            "name": row["name"],
            "status": status,
            "is_playoff": False,
            "season": {
                "teams": teams_count,
                "matches_played": played,
                "matches_total": total,
                "finished_percent": finished_percent,
            },
            "winners": winners_data,
            "best_player": best_player,
            "mvp_team": mvp_team,
        }
        
        # Log warnings for missing critical data
        if teams_count == 0:
            logger.warning(f"Division {champ_id} (tier {tier}) has 0 teams")
        if not best_player:
            logger.debug(f"Division {champ_id} (tier {tier}) missing best_player")
        if not mvp_team:
            logger.debug(f"Division {champ_id} (tier {tier}) missing mvp_team")
        
        # Check if this division has an associated playoff
        if champ_id in playoff_divisions:
            playoff_row = playoff_divisions[champ_id]
            playoff_champ_id = str(playoff_row["championship_id"])
            playoff_played = int(playoff_row.get("played_matches") or 0)
            playoff_total = int(playoff_row.get("total_matches") or 0)
            playoff_teams = int(playoff_row.get("teams_count") or 0)
            
            playoff_status = "waiting"
            if playoff_played > 0 and playoff_played < playoff_total:
                playoff_status = "active"
            elif playoff_played == playoff_total and playoff_total > 0:
                playoff_status = "finished"
            
            playoff_winners = winners_map.get(playoff_champ_id, [])
            
            division_data["playoffs"] = {
                "status": playoff_status,
                "teams": playoff_teams,
                "matches_played": playoff_played,
                "matches_total": playoff_total,
                "winner": playoff_winners[0]["team_name"] if playoff_winners else None,
            }
        else:
            # No playoff for this division yet
            division_data["playoffs"] = {
                "status": "waiting",
                "teams": 0,
                "matches_played": 0,
                "matches_total": 0,
                "winner": None,
            }
        
        divisions.append(division_data)
    
    # Log summary of returned divisions
    logger.info(
        f"get_season_divisions(season={season}): Returning {len(divisions)} divisions "
        f"(filtered {len(playoff_divisions)} playoff divisions)"
    )
    
    return divisions


async def _fetch_division_winners_map(championship_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch winners for multiple championships."""
    if not championship_ids:
        return {}
    
    placeholders = ", ".join(f":id{i}" for i in range(len(championship_ids)))
    params = {f"id{i}": cid for i, cid in enumerate(championship_ids)}
    
    rows = await query_async(
        f"""
        SELECT
            m.championship_id,
            t.name AS team_name,
            COUNT(*) AS wins
        FROM matches m
        JOIN teams t ON t.team_id = m.winner_team_id
        WHERE m.championship_id IN ({placeholders})
        AND m.winner_team_id IS NOT NULL
        AND m.is_forfeit = 0
        GROUP BY m.championship_id, m.winner_team_id, t.name
        ORDER BY m.championship_id, wins DESC
        """,
        params,
    )
    
    result: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        champ_id = str(row["championship_id"])
        if champ_id not in result:
            result[champ_id] = []
        
        result[champ_id].append({
            "team_name": row["team_name"],
            "place": len(result[champ_id]) + 1,
        })
    
    return result


async def _fetch_best_players_map(season: int) -> Dict[tuple[int, int], Dict[str, Any]]:
    """Fetch best player (by rating) for each division in a season."""
    rows = await query_async(
        """
        SELECT 
            pst.season,
            pst.division_num,
            pst.player_id,
            p.nickname,
            pst.rating
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season
        AND pst.maps_played >= 3
        AND pst.rating = (
            SELECT MAX(pst2.rating)
            FROM player_season_totals pst2
            WHERE pst2.season = pst.season
            AND pst2.division_num = pst.division_num
            AND pst2.maps_played >= 3
        )
        """,
        {"season": season},
    )
    
    result = {}
    for row in rows:
        key = (int(row["season"]), int(row["division_num"]))
        result[key] = {
            "name": row["nickname"],
            "rating": float(row["rating"]),
        }
    
    return result


async def _fetch_mvp_teams_map(season: int) -> Dict[tuple[int, int], str]:
    """Fetch MVP team (most wins) for each division in a season."""
    rows = await query_async(
        """
        SELECT 
            tst.season,
            tst.division_num,
            t.name AS team_name,
            tst.matches_won
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE tst.season = :season
        AND tst.matches_won = (
            SELECT MAX(tst2.matches_won)
            FROM team_season_totals tst2
            WHERE tst2.season = tst.season
            AND tst2.division_num = tst.division_num
        )
        """,
        {"season": season},
    )
    
    result = {}
    for row in rows:
        key = (int(row["season"]), int(row["division_num"]))
        if key not in result:  # Take first in case of tie
            result[key] = row["team_name"]
    
    return result


def _format_timestamp(ts: Optional[int]) -> Optional[str]:
    """Convert Unix timestamp to ISO date string."""
    if ts is None:
        return None
    
    from datetime import datetime
    try:
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


async def get_division_detailed_stats(season: int, division_id: str) -> Dict[str, Any]:
    """Return detailed breakdown for a specific division including teams, players, and playoff bracket."""
    
    # Get division metadata
    div_rows = await query_async(
        """
        SELECT 
            c.championship_id,
            c.season,
            c.division_num,
            c.name,
            c.is_playoffs,
            c.parent_championship_id
        FROM championships c
        WHERE c.championship_id = :division_id
        """,
        {"division_id": division_id},
    )
    
    if not div_rows:
        raise ValueError(f"Division {division_id} not found")
    
    div_data = div_rows[0]
    is_playoff = bool(div_data["is_playoffs"])
    division_num = int(div_data["division_num"])
    
    # Get excluded teams
    from division_overrides import combined_status_teams
    excluded_teams = {team["team_id"] for team in combined_status_teams(division_id)}
    
    # Build exclusion clause
    exclusion_clause = ""
    params: Dict[str, Any] = {"season": season, "division_num": division_num}
    
    if excluded_teams:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded_teams)))
        exclusion_clause = f" AND tst.team_id NOT IN ({placeholders})"
        for i, tid in enumerate(excluded_teams):
            params[f"ex{i}"] = tid
    
    # Get team stats for season
    team_rows = await query_async(
        f"""
        SELECT 
            t.team_id,
            t.name,
            tst.matches_played,
            tst.matches_won,
            tst.matches_lost,
            tst.rounds_won,
            tst.rounds_lost,
            tst.rounds_won - tst.rounds_lost AS rounds_diff,
            tst.maps_played,
            tst.maps_won,
            tst.maps_lost,
            CASE 
                WHEN tst.matches_played > 0 THEN tst.matches_won / tst.matches_played 
                ELSE 0 
            END AS win_rate
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE tst.season = :season 
        AND tst.division_num = :division_num
        {exclusion_clause}
        ORDER BY tst.matches_won DESC, rounds_diff DESC
        """,
        params,
    )
    
    # Get aggregate stats (kills, deaths, adr) per team
    team_agg_rows = await query_async(
        f"""
        SELECT 
            pst.team_id,
            COALESCE(SUM(pst.kills), 0) AS kills,
            COALESCE(SUM(pst.deaths), 0) AS deaths,
            COALESCE(AVG(NULLIF(pst.adr, 0)), 0) AS adr,
            COALESCE(AVG(NULLIF(pst.rating, 0)), 0) AS rating
        FROM player_season_totals pst
        WHERE pst.season = :season 
        AND pst.division_num = :division_num
        {exclusion_clause.replace("tst.", "pst.")}
        GROUP BY pst.team_id
        """,
        params,
    )
    
    agg_map = {row["team_id"]: row for row in team_agg_rows}
    
    teams = []
    for row in team_rows:
        team_id = row["team_id"]
        agg = agg_map.get(team_id, {})
        
        kills = int(agg.get("kills", 0))
        deaths = int(agg.get("deaths", 0))
        
        teams.append({
            "name": row["name"],
            "matches": int(row.get("matches_played", 0)),
            "wins": int(row.get("matches_won", 0)),
            "losses": int(row.get("matches_lost", 0)),
            "rounds": int(row.get("rounds_won", 0)) + int(row.get("rounds_lost", 0)),
            "kills": kills,
            "deaths": deaths,
            "adr": float(agg.get("adr", 0.0)),
            "rating": float(agg.get("rating", 0.0)),
        })
    
    # Get player leaderboards
    player_leaders_rows = await query_async(
        f"""
        SELECT 
            'top_frags' AS category,
            p.nickname AS player,
            pst.kills AS value
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season 
        AND pst.division_num = :division_num
        {exclusion_clause.replace("tst.", "pst.")}
        ORDER BY pst.kills DESC
        LIMIT 1
        """,
        params,
    )
    
    best_kd_rows = await query_async(
        f"""
        SELECT 
            'best_kd' AS category,
            p.nickname AS player,
            pst.kd AS value
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season 
        AND pst.division_num = :division_num
        AND pst.maps_played >= 3
        {exclusion_clause.replace("tst.", "pst.")}
        ORDER BY pst.kd DESC
        LIMIT 1
        """,
        params,
    )
    
    most_mvps_rows = await query_async(
        f"""
        SELECT 
            'most_mvps' AS category,
            p.nickname AS player,
            pst.mvps AS value
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season 
        AND pst.division_num = :division_num
        {exclusion_clause.replace("tst.", "pst.")}
        ORDER BY pst.mvps DESC
        LIMIT 1
        """,
        params,
    )
    
    player_leaders = {
        "top_frags": {"player": player_leaders_rows[0]["player"], "value": int(player_leaders_rows[0]["value"])} if player_leaders_rows else None,
        "best_kd": {"player": best_kd_rows[0]["player"], "value": float(best_kd_rows[0]["value"])} if best_kd_rows else None,
        "most_mvps": {"player": most_mvps_rows[0]["player"], "value": int(most_mvps_rows[0]["value"])} if most_mvps_rows else None,
    }
    
    # Get playoff bracket if this is a playoff division or has playoffs
    playoff_bracket = []
    playoff_info = {
        "matches_played": 0,
        "matches_total": 0,
        "bracket": []
    }
    
    if is_playoff:
        # This IS the playoff - get bracket
        playoff_info = await _get_playoff_bracket(division_id)
    else:
        # Check if this division HAS a playoff
        playoff_rows = await query_async(
            """
            SELECT championship_id
            FROM championships
            WHERE parent_championship_id = :parent_id AND is_playoffs = 1
            """,
            {"parent_id": division_id},
        )
        
        if playoff_rows:
            playoff_champ_id = playoff_rows[0]["championship_id"]
            playoff_info = await _get_playoff_bracket(playoff_champ_id)
    
    return {
        "division_id": division_id,
        "season": {
            "teams": teams,
            "player_leaders": player_leaders,
        },
        "playoffs": playoff_info,
    }


async def _get_playoff_bracket(championship_id: str) -> Dict[str, Any]:
    """Get playoff bracket details for a championship."""
    
    matches_rows = await query_async(
        """
        SELECT 
            m.match_id,
            m.best_of AS round,
            t1.name AS team1,
            t2.name AS team2,
            tw.name AS winner,
            m.finished_at
        FROM matches m
        LEFT JOIN teams t1 ON t1.team_id = m.team1_id
        LEFT JOIN teams t2 ON t2.team_id = m.team2_id
        LEFT JOIN teams tw ON tw.team_id = m.winner_team_id
        WHERE m.championship_id = :champ_id
        AND m.is_forfeit = 0
        ORDER BY m.started_at ASC
        """,
        {"champ_id": championship_id},
    )
    
    bracket = []
    played = 0
    total = len(matches_rows)
    
    for row in matches_rows:
        if row.get("finished_at"):
            played += 1
        
        bracket.append({
            "round": int(row.get("round", 1)),
            "match_id": row["match_id"],
            "team1": row.get("team1"),
            "team2": row.get("team2"),
            "winner": row.get("winner"),
        })
    
    return {
        "matches_played": played,
        "matches_total": total,
        "bracket": bracket,
    }

