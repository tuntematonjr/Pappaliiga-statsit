#!/usr/bin/env python3
"""Quick script to fix team_season_totals using direct SQL - excludes playoffs."""
import asyncio
from db_async import connection

async def fix_division_20():
    """Fix team_season_totals for Division 20 Season 11 - EXCLUDING playoffs."""
    season = 11
    division_num = 20
    
    async with connection() as conn:
        # Delete old data first to avoid conflicts
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM team_season_totals
                WHERE season = %s AND division_num = %s
            """, (season, division_num))
            print(f"Deleted {cur.rowcount} old rows")
        
        await conn.commit()
        
        # Insert fresh data (EXCLUDING PLAYOFFS)
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO team_season_totals (
                    season, division_num, team_id,
                    matches_played, matches_won, maps_played, maps_won,
                    rounds_won, rounds_lost
                )
                SELECT
                    %s AS season, %s AS division_num, team_id,
                    SUM(matches_played) AS matches_played,
                    SUM(matches_won) AS matches_won,
                    SUM(maps_played) AS maps_played,
                    SUM(maps_won) AS maps_won,
                    SUM(rounds_won) AS rounds_won,
                    SUM(rounds_lost) AS rounds_lost
                FROM (
                    SELECT
                        m.team1_id AS team_id,
                        COUNT(DISTINCT m.match_id) AS matches_played,
                        COUNT(DISTINCT CASE WHEN m.winner_team_id = m.team1_id THEN m.match_id END) AS matches_won,
                        COUNT(mp.map_id) AS maps_played,
                        SUM(CASE WHEN mp.winner_team_id = m.team1_id THEN 1 ELSE 0 END) AS maps_won,
                        SUM(COALESCE(mp.score_team1, 0)) AS rounds_won,
                        SUM(COALESCE(mp.score_team2, 0)) AS rounds_lost
                    FROM matches m
                    JOIN championships c ON m.championship_id = c.championship_id
                    LEFT JOIN maps mp ON mp.match_id = m.match_id
                    WHERE m.season = %s
                    AND m.division_num = %s
                    AND c.is_playoffs = 0
                    AND m.finished_at IS NOT NULL
                    GROUP BY m.team1_id
                    
                    UNION ALL
                    
                    SELECT
                        m.team2_id AS team_id,
                        COUNT(DISTINCT m.match_id) AS matches_played,
                        COUNT(DISTINCT CASE WHEN m.winner_team_id = m.team2_id THEN m.match_id END) AS matches_won,
                        COUNT(mp.map_id) AS maps_played,
                        SUM(CASE WHEN mp.winner_team_id = m.team2_id THEN 1 ELSE 0 END) AS maps_won,
                        SUM(COALESCE(mp.score_team2, 0)) AS rounds_won,
                        SUM(COALESCE(mp.score_team1, 0)) AS rounds_lost
                    FROM matches m
                    JOIN championships c ON m.championship_id = c.championship_id
                    LEFT JOIN maps mp ON mp.match_id = m.match_id
                    WHERE m.season = %s
                    AND m.division_num = %s
                    AND c.is_playoffs = 0
                    AND m.finished_at IS NOT NULL
                    GROUP BY m.team2_id
                ) AS agg
                GROUP BY team_id
            """, (season, division_num, season, division_num, season, division_num))
            
            print(f"Inserted {cur.rowcount} team total rows")
        
        await conn.commit()
        
        # Verify Servujatkot
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT tst.team_id, tst.matches_played, tst.matches_won, tst.maps_played, tst.maps_won, tst.rounds_won, tst.rounds_lost
                FROM team_season_totals tst
                WHERE tst.season = %s AND tst.division_num = %s
                ORDER BY tst.matches_won DESC
                LIMIT 1
            """, (season, division_num))
            row = await cur.fetchone()
            if row:
                print(f"\nTop team (by matches_won): {row[0]}")
                print(f"  Matches: {row[1]} played, {row[2]} won")
                print(f"  Maps: {row[3]} played, {row[4]} won")
                print(f"  Rounds: {row[5]} won, {row[6]} lost, {row[5]-row[6]} diff")
                print(f"\nExpected from screenshot: 11 GP, 7 W for Servujatkot")

if __name__ == '__main__':
    asyncio.run(fix_division_20())
