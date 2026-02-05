#!/usr/bin/env python3
"""Debug script to find where Mirage and Dust2 stats are coming from."""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_async import query_async, connection


async def debug_map_stats(championship_id: str):
    """Debug map stats for a championship."""
    
    print(f"\n=== Debugging Map Stats for Championship {championship_id} ===\n")
    
    # Get all maps in division_maps (what SHOULD be returned)
    print("1. Maps actually played in this championship:")
    rows = await query_async("""
        SELECT 
            m.map_name,
            COUNT(DISTINCT m.map_id) as map_count,
            COUNT(DISTINCT m.match_id) as match_count
        FROM maps m
        JOIN matches mt ON m.match_id = mt.match_id
        WHERE mt.championship_id = :champ_id
            AND m.is_forfeit = 0
            AND m.map_name IS NOT NULL
        GROUP BY m.map_name
        ORDER BY map_count DESC
    """, {"champ_id": championship_id})
    
    for r in rows:
        print(f"  {r['map_name']:20} - {r['map_count']} map instances, {r['match_count']} matches")
    
    # Check if Mirage or Dust2 appear in maps table
    print("\n2. Check for Mirage and Dust2 in maps table:")
    rows = await query_async("""
        SELECT 
            m.map_name,
            COUNT(*) as count,
            COUNT(DISTINCT m.match_id) as matches
        FROM maps m
        JOIN matches mt ON m.match_id = mt.match_id
        WHERE mt.championship_id = :champ_id
            AND m.map_name IN ('de_mirage', 'de_dust2', 'de_dust', 'mirage', 'dust2', 'dust')
        GROUP BY m.map_name
    """, {"champ_id": championship_id})
    
    if rows:
        print("  Found these maps:")
        for r in rows:
            print(f"    {r['map_name']} - {r['count']} entries, {r['matches']} matches")
    else:
        print("  NO Mirage or Dust2 in maps table for this championship!")
    
    # Check if there are player_stats for Mirage or Dust2
    print("\n3. Check for player_stats entries with map_id linking to Mirage/Dust2:")
    rows = await query_async("""
        SELECT 
            m.map_name,
            COUNT(ps.player_stat_id) as stat_count,
            COUNT(DISTINCT ps.player_id) as player_count,
            SUM(ps.kills) as total_kills,
            SUM(ps.deaths) as total_deaths
        FROM player_stats ps
        LEFT JOIN maps m ON ps.map_id = m.map_id
        WHERE ps.season = (SELECT season FROM championships WHERE championship_id = :champ_id LIMIT 1)
            AND ps.division_num = (SELECT division_num FROM championships WHERE championship_id = :champ_id LIMIT 1)
            AND m.map_name IN ('de_mirage', 'de_dust2', 'de_dust', 'mirage', 'dust2', 'dust')
        GROUP BY m.map_name
    """, {"champ_id": championship_id})
    
    if rows:
        print("  Found player stats for these maps:")
        for r in rows:
            print(f"    {r['map_name']} - {r['stat_count']} stat entries, {r['player_count']} players, {r['total_kills']} kills, {r['total_deaths']} deaths")
    else:
        print("  NO player stats found for Mirage or Dust2!")
    
    # Check for orphaned player_stats (map_id is NULL or doesn't exist in maps)
    print("\n4. Check for player_stats with NULL or non-existent map_id:")
    rows = await query_async("""
        SELECT 
            COUNT(ps.player_stat_id) as null_map_id_count,
            SUM(CASE WHEN m.map_id IS NULL THEN 1 ELSE 0 END) as orphaned_count
        FROM player_stats ps
        LEFT JOIN maps m ON ps.map_id = m.map_id
        WHERE ps.season = (SELECT season FROM championships WHERE championship_id = :champ_id LIMIT 1)
            AND ps.division_num = (SELECT division_num FROM championships WHERE championship_id = :champ_id LIMIT 1)
    """, {"champ_id": championship_id})
    
    if rows:
        r = rows[0]
        print(f"  Player stats with NULL map_id in match: {r['null_map_id_count']}")
        print(f"  Orphaned player stats (no matching maps entry): {r['orphaned_count']}")
    
    # Check division_snapshots to see if Mirage/Dust2 are being tracked
    print("\n5. Check division_snapshots for Mirage/Dust2:")
    rows = await query_async("""
        SELECT 
            map_name,
            COUNT(*) as snapshot_count
        FROM player_map_season_snapshots
        WHERE season = (SELECT season FROM championships WHERE championship_id = :champ_id LIMIT 1)
            AND map_name IN ('de_mirage', 'de_dust2', 'de_dust', 'mirage', 'dust2', 'dust')
        GROUP BY map_name
    """, {"champ_id": championship_id})
    
    if rows:
        print("  Found snapshots for:")
        for r in rows:
            print(f"    {r['map_name']} - {r['snapshot_count']} snapshots")
    else:
        print("  NO snapshots found for Mirage or Dust2!")
    
    # Run the actual query used by the API
    print("\n6. Run actual division map stats query:")
    rows = await query_async("""
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
        )
        SELECT
            pt.map_name,
            pt.maps_played,
            COALESCE(rt.rounds_played, 0) AS rounds_played,
            COALESCE(pt.kills, 0) AS kills,
            COALESCE(pt.deaths, 0) AS deaths,
            COALESCE(pt.damage, 0) AS damage
        FROM player_totals pt
        LEFT JOIN round_totals rt ON rt.map_key = pt.map_key
        WHERE pt.map_name IN ('de_mirage', 'de_dust2', 'de_dust', 'mirage', 'dust2', 'dust')
        ORDER BY pt.maps_played DESC, pt.map_name
    """, {"champ_id": championship_id})
    
    if rows:
        print("  Query result (should be empty if never played):")
        for r in rows:
            print(f"    {r['map_name']} - {r['maps_played']} played, {r['rounds_played']} rounds, {r['kills']} kills, {r['deaths']} deaths, {r['damage']} damage")
    else:
        print("  Query returned no results for Mirage/Dust2 (CORRECT!)")


async def main():
    """Main entry point."""
    # Test with a known championship ID - adjust if needed
    championship_id = "pappaliiga-11-1"  # Adjust to a real championship
    
    # Or if you want to debug multiple:
    divisions = [
        "pappaliiga-11-1",
        "pappaliiga-11-2", 
        "pappaliiga-11-3",
        "pappaliiga-11-4",
    ]
    
    for champ_id in divisions:
        try:
            await debug_map_stats(champ_id)
        except Exception as e:
            print(f"Error for {champ_id}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
