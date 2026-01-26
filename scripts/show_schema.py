#!/usr/bin/env python3
"""Show table schema for team_season_totals."""
import asyncio
from db_async import connection

async def show_schema():
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SHOW CREATE TABLE team_season_totals")
            row = await cur.fetchone()
            print(row[1])

if __name__ == '__main__':
    asyncio.run(show_schema())
