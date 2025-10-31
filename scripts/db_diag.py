import os
import asyncio

print('DATABASE_URL:', os.environ.get('DATABASE_URL'))

async def _run():
    try:
        from db_async import connection
    except Exception as e:
        print('Import error for db_async:', e)
        return

    try:
        async with connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT 1')
                print('DB test query OK:', await cur.fetchone())
    except Exception as e:
        print('DB connection/query error:', e)

if __name__ == '__main__':
    asyncio.run(_run())
