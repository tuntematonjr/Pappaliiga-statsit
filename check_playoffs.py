import asyncio
from db_async import readonly_connection

async def check():
    async with readonly_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                'SELECT season, name, is_playoffs, parent_championship_id '
                'FROM championships WHERE is_playoffs=1 ORDER BY season DESC LIMIT 10'
            )
            rows = await cur.fetchall()
            print('Playoff championships:')
            if rows:
                for r in rows:
                    print(f'Season {r[0]}: {r[1]} (parent: {r[3]})')
            else:
                print('No playoff championships found')
            
            # Also check total count
            await cur.execute('SELECT COUNT(*) FROM championships WHERE is_playoffs=1')
            count = await cur.fetchone()
            print(f'\nTotal playoff championships: {count[0]}')

asyncio.run(check())
