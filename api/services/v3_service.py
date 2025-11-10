"""API v3 service layer."""
from __future__ import annotations
from typing import List, Dict, Any
import db_ops_async as db
from db_async import get_pool

async def get_season_summary_v3(season_id: int) -> Dict[str, Any]:
    """Gets season summary statistics for v3."""
    # This is a placeholder implementation.
    # The actual implementation will require new DB queries.
    summary = {
        "season_id": season_id,
        "divisions_total": 26,
        "divisions_finished": 0,
        "teams": 150,
        "players": 750,
        "matches": 0,
        "rounds": 0,
        "kills": 0,
        "deaths": 0,
        "adr_avg": 0.0,
        "kd_avg": 0.0,
        "win_rate": 0.0,
    }
    return summary

async def get_divisions_v3(season_id: int) -> List[Dict[str, Any]]:
    """Gets all divisions for a season for v3."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Query divisions directly by season
        divisions = await db.get_all_base_divisions_for_season(conn, season_id)
        
        result = []
        for div in divisions:
            stats = await db.get_division_stats_for_v3(conn, div['division_id'])
            
            season_stats = stats.get('season', {})
            playoff_stats = stats.get('playoffs', {})

            def get_status(matches_played, matches_total, db_status):
                if matches_played == 0:
                    return "waiting"
                if matches_played >= matches_total and matches_total > 0:
                    return "finished"
                if db_status == 'finished': # last match finished
                     return "finished"
                return "active"

            season_status = get_status(
                season_stats.get('matches_played', 0),
                season_stats.get('matches_total', 0),
                season_stats.get('division_status')
            )
            
            playoff_matches_played = playoff_stats.get('matches_played', 0)
            playoff_matches_total = 7 # Hardcoded as per spec for now
            
            playoff_status = get_status(playoff_matches_played, playoff_matches_total, None)


            result.append({
                "division_id": div['division_id'],
                "tier": div['tier'],
                "name": div['name'],
                "status": season_status,
                "season": {
                    "teams": season_stats.get('teams', 0),
                    "matches_played": season_stats.get('matches_played', 0),
                    "matches_total": season_stats.get('matches_total', 0)
                },
                "playoffs": {
                    "status": playoff_status,
                    "teams": 8,
                    "matches_played": playoff_matches_played,
                    "matches_total": playoff_matches_total,
                    "winner_team": playoff_stats.get('winner_team')
                },
                "meta": {
                    "winner_team": None, # Placeholder
                    "mvp_player": None
                }
            })
        return result
