"""Check database for missing team counts"""
import asyncio
import asyncmy
from pathlib import Path
from env_loader import load_env
from db_async import get_pool

# Load environment variables
load_env(Path(__file__).parent)

async def check_team_counts():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(asyncmy.cursors.DictCursor) as cur:
            # Get all base divisions for season 11
            await cur.execute("""
                SELECT championship_id, division_num, name
                FROM championships
                WHERE season = 11 AND is_playoffs = 0
                ORDER BY division_num
            """)
            divisions = await cur.fetchall()
            
            print(f"Found {len(divisions)} base divisions for season 11\n")
            print("=" * 80)
            
            issues = []
            for div in divisions:
                # Check team_season_totals
                await cur.execute("""
                    SELECT COUNT(DISTINCT team_id) as team_count
                    FROM team_season_totals
                    WHERE season = 11 AND division_num = %s
                """, (div['division_num'],))
                result = await cur.fetchone()
                team_count = result['team_count'] if result else 0
                
                # Check matches
                await cur.execute("""
                    SELECT 
                        COUNT(*) as total_matches,
                        COUNT(CASE WHEN status = 'finished' THEN 1 END) as finished_matches
                    FROM matches
                    WHERE championship_id = %s
                """, (div['championship_id'],))
                match_result = await cur.fetchone()
                
                if team_count == 0:
                    issues.append({
                        'div_num': div['division_num'],
                        'name': div['name'],
                        'championship_id': div['championship_id'],
                        'teams': team_count,
                        'matches_total': match_result['total_matches'],
                        'matches_finished': match_result['finished_matches']
                    })
                    print(f"⚠ Tier {div['division_num']}: {div['name']}")
                    print(f"  Teams in team_season_totals: {team_count}")
                    print(f"  Matches: {match_result['finished_matches']}/{match_result['total_matches']}")
                    print()
            
            print("=" * 80)
            if issues:
                print(f"\nFound {len(issues)} divisions with 0 teams in team_season_totals")
                print("\nLet's check if these divisions have teams in other tables...")
                
                for issue in issues[:3]:  # Check first 3
                    print(f"\nChecking {issue['name']}:")
                    
                    # Check if teams exist in matches
                    await cur.execute("""
                        SELECT DISTINCT team1_id, team2_id
                        FROM matches
                        WHERE championship_id = %s
                        LIMIT 1
                    """, (issue['championship_id'],))
                    match_teams = await cur.fetchone()
                    
                    if match_teams:
                        print(f"  ✓ Has teams in matches table: {match_teams['team1_id']}, {match_teams['team2_id']}")
                    else:
                        print(f"  ✗ No teams found in matches either")
            else:
                print("\n✓ All divisions have team data!")

asyncio.run(check_team_counts())
