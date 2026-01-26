"""
Test script to verify head-to-head standings calculations.
Fetches a division's standings and displays h2h tiebreaker data.
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.services.teams_service import get_division_standings


async def main():
    # Test with a championship ID - replace with actual ID from your DB
    # Example: Season 5, Division 1 regular season
    championship_id = "7ff31db2-456c-426d-adaa-7bc640a257eb"
    
    print(f"Fetching standings for championship: {championship_id}\n")
    
    try:
        standings = await get_division_standings(championship_id)
        
        print(f"{'Pos':<5} {'Team Name':<30} {'W':<4} {'RD':<6} {'H2H Maps':<10} {'H2H RD':<8}")
        print("-" * 70)
        
        for team in standings:
            pos = team.get('position', '?')
            name = team.get('team_name', 'Unknown')[:28]
            wins = team.get('matches_won', 0)
            rd = team.get('round_diff', 0)
            
            # H2H stats are in the h2h_data structure, not in team dict
            # They're only used for sorting, not stored back
            print(f"{pos:<5} {name:<30} {wins:<4} {rd:<6}")
        
        print(f"\nTotal teams: {len(standings)}")
        
        # Check for potential ties
        print("\n=== Checking for tied teams ===")
        from collections import defaultdict
        by_wins_and_rd = defaultdict(list)
        
        for team in standings:
            key = (team.get('matches_won', 0), team.get('round_diff', 0))
            by_wins_and_rd[key].append(team.get('team_name', 'Unknown'))
        
        ties_found = False
        for (wins, rd), teams in by_wins_and_rd.items():
            if len(teams) > 1:
                ties_found = True
                print(f"\nTied at {wins}W, {rd:+d}RD:")
                for team_name in teams:
                    print(f"  - {team_name}")
        
        if not ties_found:
            print("No ties found in current standings.")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
