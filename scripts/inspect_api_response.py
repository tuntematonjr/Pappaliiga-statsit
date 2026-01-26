"""Detailed API response inspection."""
import asyncio
import requests
from db_async import query_async
import json

API_BASE = "http://localhost:8000/api"


async def main():
    print("Getting test championship...\n")
    
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
        print("No championships found")
        return
    
    champ = rows[0]
    champ_id = champ["championship_id"]
    season = champ["season"]
    division_num = champ["division_num"]
    
    # Get team
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
    
    team_id = team_rows[0]["team_id"]
    
    print(f"Championship: {champ['name']} (ID: {champ_id})")
    print(f"Team: {team_rows[0]['name']} (ID: {team_id})\n")
    
    # Make request
    url = f"{API_BASE}/teams/{team_id}/page"
    params = {"championship_id": champ_id}
    resp = requests.get(url, params=params, timeout=10)
    
    if resp.status_code != 200:
        print(f"Error {resp.status_code}:")
        print(resp.text)
        return
    
    data = resp.json()
    
    # Pretty print season_data
    if data.get("season_data"):
        print("season_data keys:", list(data["season_data"].keys()))
        print("\nPhase 1 fields:")
        for field in ["standings", "teamPosition", "recentForm", "divisionAverages", "playerRoles"]:
            value = data["season_data"].get(field)
            if value is None:
                print(f"  {field}: None")
            elif isinstance(value, list):
                print(f"  {field}: list[{len(value)}]")
                if value:
                    print(f"    First item: {json.dumps(value[0], indent=2)[:200]}")
            elif isinstance(value, dict):
                print(f"  {field}: dict")
                print(f"    {json.dumps(value, indent=2)[:200]}")
            else:
                print(f"  {field}: {value}")
    else:
        print("No season_data!")


if __name__ == "__main__":
    asyncio.run(main())
