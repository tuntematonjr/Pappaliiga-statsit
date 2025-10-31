import sys, os, asyncio
sys.path.insert(0, os.getcwd())
from db_async import connection

async def main():
    season = 11
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM player_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_season_totals rows:', r[0] if r else 0)

            await cur.execute("SELECT COALESCE(SUM(rounds_played),0), COALESCE(SUM(kills),0), COALESCE(SUM(deaths),0) FROM player_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('SUM rounds_played,kills,deaths:', r)

            await cur.execute("SELECT COUNT(*) FROM player_stats WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_stats rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM maps WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('maps rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM player_map_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_map_season_totals rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM team_map_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('team_map_season_totals rows:', r[0] if r else 0)

asyncio.run(main())
