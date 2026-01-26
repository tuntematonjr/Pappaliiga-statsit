"""Quick script to find and check division 20 standings."""
import asyncio
from db_async import query_async
from api.services.teams_service import get_division_standings

async def main():
    # Check max division
    max_div = await query_async("SELECT MAX(division_num) as max_div FROM championships")
    print(f"Max division in DB: {max_div[0]['max_div']}")
    
    # Find Division 20 or similar
    champs = await query_async(
        """
        SELECT championship_id, name, season, division_num 
        FROM championships 
        WHERE (division_num >= 18 AND division_num <= 22) AND is_playoffs = 0
        ORDER BY season DESC, division_num 
        LIMIT 10
        """
    )
    
    print(f"\nFound {len(champs)} championships:")
    for c in champs:
        print(f"  S{c['season']}D{c['division_num']}: {c['name']}")
    
    if not champs:
        print("No divisions 18-22 found")
        return
    
    # Look for one matching the screenshot teams
    print("\n" + "="*75)
    print("Checking for Servujatkot (should be #1 in Division 20)...")
    
    for champ in champs:
        if champ['division_num'] != 20:
            continue
            
        champ_id = champ['championship_id']
        print(f"\nTrying: {champ['name']} (S{champ['season']}D{champ['division_num']})")
        
        try:
            standings = await get_division_standings(champ_id)
            
            if standings and standings[0]['team_name'] == 'Servujatkot':
                print(f"✓ FOUND IT! Championship ID: {champ_id}\n")
                
                print(f"{'#':<4} {'Team':<25} {'Matches':<12} {'Maps':<12} {'RD':<6}")
                print(f"{'':4} {'':25} {'P':>3} {'W':>3} {'L':>3}  {'Won':>3} {'Lost':>4}  {''}")
                print("-" * 75)
                
                for team in standings[:12]:
                    print(f"{team['position']:<4} {team['team_name'][:24]:<25} "
                          f"{team['matches_played']:>3} {team['matches_won']:>3} {team['matches_lost']:>3}  "
                          f"{team['maps_won']:>3} {team['maps_lost']:>4}  {team['round_diff']:>+6}")
                
                print("\nScreenshot shows (GP=matches, W=wins, +/-=round diff):")
                print("1. Servujatkot: 11 GP, 7 W, 90 +/-")
                print("2. Dorve: 11 GP, 4 W, 47 +/-")
                print("3. Hold My Beer: 11 GP, 4 W, 64 +/-")
                
                return
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
