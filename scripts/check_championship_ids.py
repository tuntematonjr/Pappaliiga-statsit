#!/usr/bin/env python3
"""Check matches table championship_id values."""
import asyncio
from db_async import connection

async def check_matches():
    async with connection() as conn:
        async with conn.cursor() as cur:
            # Check what championship IDs exist for season 11 div 20
            await cur.execute("""
                SELECT DISTINCT championship_id, COUNT(*) as cnt
                FROM matches
                WHERE season = 11 AND division_num = 20
                GROUP BY championship_id
            """)
            rows = await cur.fetchall()
            print("Championship IDs in matches for S11 D20:")
            for row in rows:
                print(f"  {row[0]}: {row[1]} matches")
            
            # Check one specific match
            await cur.execute("""
                SELECT match_id, championship_id, team1_id, team2_id, winner_team_id, status
                FROM matches
                WHERE season = 11 AND division_num = 20
                AND (team1_id = '753b32ea-1d6e-46ab-8a8b-151de3229325'
                     OR team2_id = '753b32ea-1d6e-46ab-8a8b-151de3229325')
                LIMIT 3
            """)
            rows = await cur.fetchall()
            print("\nSample matches for Servujatkot:")
            for row in rows:
                print(f"  Match: {row[0]}")
                print(f"  Championship: {row[1]}")
                print(f"  Teams: {row[2]} vs {row[3]}")
                print(f"  Winner: {row[4]}, Status: {row[5]}")
                print()

if __name__ == '__main__':
    asyncio.run(check_matches())
