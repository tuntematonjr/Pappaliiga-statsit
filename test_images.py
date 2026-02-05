import asyncio
from db_async import query_async

result = asyncio.run(query_async("""
SELECT c.championship_id, c.season, c.division_num, t.team_id, t.name 
FROM team_season_totals tst 
JOIN championships c ON c.season = tst.season AND c.division_num = tst.division_num 
JOIN teams t ON t.team_id = tst.team_id 
WHERE c.season < 12 AND tst.maps_played > 0 
LIMIT 1
"""))

if result:
    r = result[0]
    print(f"{r['team_id']}|{r['championship_id']}|{r['name']}")
else:
    print('Not found')
