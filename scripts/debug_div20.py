"""Check for playoff vs regular season data mixing."""
import asyncio
import sys
import traceback
from db_async import query_async

async def main():
    try:
        # Find all Division 20 championships
        champs = await query_async(
            """
            SELECT championship_id, name, season, division_num, is_playoffs, parent_championship_id
            FROM championships 
            WHERE division_num = 20
            ORDER BY season DESC, is_playoffs
            """
        )
        
        print("All Division 20 championships:")
        print(f"{'ID':<40} {'Name':<30} {'Season':<8} {'Playoffs?':<10}")
        print("-" * 95)
        for c in champs:
            playoff_mark = "✓ PLAYOFF" if c['is_playoffs'] else "Regular"
            parent = f" (parent: {c['parent_championship_id'][:8]}...)" if c['parent_championship_id'] else ""
            print(f"{c['championship_id']:<40} {c['name']:<30} S{c['season']:<7} {playoff_mark}{parent}")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "="*95)
    
    # Check the specific championship from earlier
    target_id = "7ff31db2-456c-426d-adaa-7bc640a257eb"
    
    champ_info = await query_async(
        "SELECT * FROM championships WHERE championship_id = :id",
        {"id": target_id}
    )
    
    if champ_info:
        c = champ_info[0]
        print(f"\nChampionship {target_id}:")
        print(f"  Name: {c['name']}")
        print(f"  Season: {c['season']}, Division: {c['division_num']}")
        print(f"  Is Playoffs: {c['is_playoffs']}")
        print(f"  Parent: {c['parent_championship_id']}")
    
    # Now check team_season_totals for Servujatkot in this championship
    print("\n" + "="*95)
    print("Checking team_season_totals for Servujatkot:")
    
    servujatkot = await query_async(
        """
        SELECT tst.*, t.name
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE t.name = 'Servujatkot' AND tst.season = 11 AND tst.division_num = 20
        """
    )
    
    if servujatkot:
        s = servujatkot[0]
        print(f"\n  Team: {s['name']}")
        print(f"  Matches played: {s['matches_played']}")
        print(f"  Matches won: {s['matches_won']}")
        print(f"  Maps won: {s['maps_won']}")
        print(f"  Maps played: {s['maps_played']}")
        print(f"  Rounds won: {s['rounds_won']}")
        print(f"  Rounds lost: {s['rounds_lost']}")
        print(f"  Round diff: {s['rounds_won'] - s['rounds_lost']}")
    
    # Check actual matches for Servujatkot
    print("\n" + "="*95)
    print("Checking actual matches for Servujatkot in this championship:")
    
    matches = await query_async(
        """
        SELECT m.match_id, m.status, m.winner_team_id, m.best_of,
               t1.name as team1_name, t2.name as team2_name,
               m.team1_id, m.team2_id
        FROM matches m
        JOIN teams t1 ON t1.team_id = m.team1_id
        JOIN teams t2 ON t2.team_id = m.team2_id
        WHERE m.championship_id = :champ_id
          AND (t1.name = 'Servujatkot' OR t2.name = 'Servujatkot')
        ORDER BY m.finished_at
        """,
        {"champ_id": target_id}
    )
    
    print(f"\nFound {len(matches)} matches for Servujatkot")
    print(f"{'Status':<12} {'Team 1':<25} {'Team 2':<25} {'Winner':<25}")
    print("-" * 90)
    
    finished = 0
    wins = 0
    for m in matches:
        if m['status'] == 'FINISHED':
            finished += 1
            winner_name = 'Servujatkot' if m['winner_team_id'] == (
                m['team1_id'] if m['team1_name'] == 'Servujatkot' else m['team2_id']
            ) else (m['team1_name'] if m['team1_name'] != 'Servujatkot' else m['team2_name'])
            
            if winner_name == 'Servujatkot':
                wins += 1
            
            print(f"{m['status']:<12} {m['team1_name']:<25} {m['team2_name']:<25} {winner_name:<25}")
    
    print(f"\nFinished matches: {finished}")
    print(f"Servujatkot wins: {wins}")
    print("\nScreenshot shows: 11 GP, 7 W for Servujatkot")

if __name__ == "__main__":
    asyncio.run(main())
