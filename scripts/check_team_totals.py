import asyncio
from db_async import connection


async def main():
    season = 11
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) AS cnt FROM team_season_totals WHERE season = %s", (season,))
            row = await cur.fetchone()
            print(f"team_season_totals count for season {season}:", row[0] if row else 0)

            await cur.execute("SELECT season, division_num, team_id, matches_played, maps_played FROM team_season_totals WHERE season = %s LIMIT 10", (season,))
            rows = await cur.fetchall()
            for r in rows:
                print(r)


if __name__ == '__main__':
    asyncio.run(main())
