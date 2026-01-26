"""Quick API test for Phase 1 features."""
import asyncio
import requests
from db_async import query_async

API_BASE = "http://localhost:8000/api"


async def get_test_ids():
    """Get a championship and team ID for testing."""
    # Get a championship with teams
    rows = await query_async(
        """
        SELECT c.championship_id, c.name, c.slug, c.season, c.division_num
        FROM championships c
        WHERE c.is_playoffs = 0
        AND EXISTS (
            SELECT 1 FROM team_season_totals tst 
            WHERE tst.season = c.season AND tst.division_num = c.division_num
        )
        LIMIT 1
        """
    )
    if not rows:
        print("❌ No championships found")
        return None, None
    
    champ = rows[0]
    champ_id = champ["championship_id"]
    season = champ["season"]
    division_num = champ["division_num"]
    print(f"✅ Using championship: {champ['name']} ({champ['slug']})")
    
    # Get a team from that championship
    team_rows = await query_async(
        """
        SELECT tst.team_id, t.name
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE tst.season = :season AND tst.division_num = :div
        AND tst.matches_played > 3
        LIMIT 1
        """,
        {"season": season, "div": division_num}
    )
    
    if not team_rows:
        print("❌ No teams found")
        return champ_id, None
    
    team = team_rows[0]
    print(f"✅ Using team: {team['name']}")
    
    return champ_id, team["team_id"]


async def main():
    print("=== Testing Phase 1 API Endpoints ===\n")
    
    champ_id, team_id = await get_test_ids()
    if not champ_id or not team_id:
        return
    
    print(f"\nChampionship ID: {champ_id}")
    print(f"Team ID: {team_id}\n")
    
    # Test comprehensive team page endpoint
    print("Testing /api/teams/{team_id}/page?championship_id={champ_id}...")
    try:
        url = f"{API_BASE}/teams/{team_id}/page"
        params = {"championship_id": champ_id}
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code != 200:
            print(f"❌ Status {resp.status_code}: {resp.text[:200]}")
            return
        
        data = resp.json()
        print("✅ Got response!\n")
        
        # Check Phase 1 fields
        print("Phase 1 Fields:")
        
        if data.get("standings"):
            print(f"  ✅ standings: {len(data['standings'])} teams")
            print(f"     Sample: Position #{data['standings'][0]['position']} - {data['standings'][0]['team_name']}")
        else:
            print("  ❌ standings: MISSING")
        
        if data.get("team_position"):
            tp = data["team_position"]
            print(f"  ✅ team_position: #{tp['position']} ({tp['matches_won']}W/{tp['matches_played']}M, RD: {tp['round_diff']})")
        else:
            print("  ❌ team_position: MISSING")
        
        if data.get("recent_form"):
            form = data["recent_form"]
            results = " ".join([m["result"] for m in form])
            print(f"  ✅ recent_form: {results} ({len(form)} matches)")
        else:
            print("  ❌ recent_form: MISSING")
        
        if data.get("division_averages"):
            avgs = data["division_averages"]
            print(f"  ✅ division_averages: WR={avgs['avg_win_rate']:.1f}%, RD={avgs['avg_round_diff']:.0f}, MWR={avgs['avg_map_win_rate']:.1f}%")
        else:
            print("  ❌ division_averages: MISSING")
        
        if data.get("player_roles"):
            roles = data["player_roles"]
            print(f"  ✅ player_roles: {len(roles)} players with roles")
            if roles:
                p = roles[0]
                print(f"     Sample: {p['player_nickname']} - {p.get('primary_role', 'N/A')} ({', '.join(p.get('roles', []))})")
        else:
            print("  ❌ player_roles: MISSING")
        
        print("\n=== Test Complete ===")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
