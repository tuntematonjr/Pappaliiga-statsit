import asyncio
from db_async import connection


async def main():
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SHOW TABLES LIKE 'team_map_season_totals'")
            t1 = await cur.fetchall()
            await cur.execute("SHOW TABLES LIKE 'team_map_season_totals_prev'")
            t2 = await cur.fetchall()
            print('team_map_season_totals exists:', bool(t1))
            print('team_map_season_totals_prev exists:', bool(t2))

            if t1:
                await cur.execute("SELECT COUNT(*) FROM team_map_season_totals")
                print('team_map_season_totals rows:', (await cur.fetchone())[0])
            if t2:
                await cur.execute("SELECT COUNT(*) FROM team_map_season_totals_prev")
                print('team_map_season_totals_prev rows:', (await cur.fetchone())[0])


if __name__ == '__main__':
    asyncio.run(main())
