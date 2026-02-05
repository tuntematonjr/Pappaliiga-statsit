#!/usr/bin/env python3
"""Test to verify that team map stats are correct and don't pull wrong data."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.services.teams_service import fetch_team_map_stats_comprehensive
from db_async import query_async


async def test_team_map_stats_correctness():
    """Test that team map stats don't include wrong data from other matches."""
    
    # Test with a team that doesn't play all maps
    champ_id = "8d243c3b-336b-4bac-899f-004358e64ee1"  # 4 Divisioona S11
    team_id = "da163e83-7643-489d-9bc3-2ba9bfb4202c"  # ++ crew
    
    print(f"Testing team map stats correctness for {team_id} in {champ_id}\n")
    
    # Get the team map stats from the API service
    map_stats = await fetch_team_map_stats_comprehensive(champ_id, team_id)
    
    # Get the ground truth from database
    actual_maps_played = await query_async("""
        SELECT 
            m.map_name,
            COUNT(DISTINCT m.map_id) as maps_played,
            SUM(ps.kills) as total_kills,
            SUM(ps.deaths) as total_deaths
        FROM maps m
        INNER JOIN matches mt ON m.match_id = mt.match_id
        LEFT JOIN player_stats ps ON m.map_id = ps.map_id AND ps.team_id = :team_id AND ps.is_forfeit_map = 0
        WHERE mt.championship_id = :champ_id
            AND (mt.team1_id = :team_id OR mt.team2_id = :team_id)
            AND m.is_forfeit = 0
        GROUP BY m.map_name
    """, {"champ_id": champ_id, "team_id": team_id})
    
    actual_by_map = {r['map_name']: r for r in actual_maps_played}
    
    print("Comparing API results with actual database values:\n")
    all_correct = True
    
    for m in sorted(map_stats, key=lambda x: x.get("played", 0), reverse=True):
        map_name = m['map_name']
        api_played = m.get('played', 0)
        api_kills = m.get('kills', 0)
        api_deaths = m.get('deaths', 0)
        
        if map_name in actual_by_map:
            actual = actual_by_map[map_name]
            db_played = int(actual['maps_played'] or 0)
            db_kills = int(actual['total_kills'] or 0)
            db_deaths = int(actual['total_deaths'] or 0)
            
            kills_match = api_kills == db_kills
            deaths_match = api_deaths == db_deaths
            
            status = "OK" if kills_match and deaths_match else "MISMATCH"
            
            print(f"  [{status}] {map_name:20}")
            print(f"         API:    played={api_played:2}, kills={api_kills:4}, deaths={api_deaths:4}")
            print(f"         DB:     played={db_played:2}, kills={db_kills:4}, deaths={db_deaths:4}")
            
            if not kills_match or not deaths_match:
                all_correct = False
        else:
            # Map not in database - should have zero stats
            if api_played == 0 and api_kills == 0 and api_deaths == 0:
                print(f"  [OK] {map_name:20} - not played, correctly shows zero stats")
            else:
                print(f"  [BUG] {map_name:20} - not in DB but has non-zero stats!")
                print(f"         API: played={api_played}, kills={api_kills}, deaths={api_deaths}")
                all_correct = False
        print()
    
    if all_correct:
        print("\nPASS: All map stats match database ground truth!")
        return True
    else:
        print("\nFAIL: Some map stats don't match!")
        return False


async def main():
    """Main entry point."""
    success = await test_team_map_stats_correctness()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
