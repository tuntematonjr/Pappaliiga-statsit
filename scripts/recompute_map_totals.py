import asyncio
from db_async import connection
from db_ops_async import upsert_team_map_season_totals_async


async def recompute(season: int = 11):
    async with connection() as conn:
        async with conn.cursor() as cur:
            # Find distinct team_id and map_name combos from player_stats/maps for the season
            await cur.execute(
                """
                SELECT DISTINCT ps.team_id, mp.map_name, ps.division_num
                FROM player_stats ps
                JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
                WHERE ps.season = %s AND ps.team_id IS NOT NULL
                """,
                (season,)
            )
            rows = await cur.fetchall()
            combos = [(r[0], r[1], r[2]) for r in rows if r and r[0] and r[1]]

        print(f'Found {len(combos)} team+map combos for season {season}')

        updated = 0
        async with connection() as conn:
            async with conn.cursor() as cur:
                for team_id, map_name, division_num in combos:
                    try:
                        await upsert_team_map_season_totals_async(conn, season, division_num, team_id, map_name)
                        updated += 1
                    except Exception as e:
                        print('Failed:', team_id, map_name, e)

        print('Updated combos:', updated)


if __name__ == '__main__':
    asyncio.run(recompute())
