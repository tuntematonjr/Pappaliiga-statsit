"""
Force-drop all tables in the pappaliiga database using asyncmy.
Usage: python drop_all_tables.py
"""

import asyncio
from db_async import connection
from env_loader import load_env

async def drop_all_tables():
    load_env()  # Load .env variables before connecting
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SET FOREIGN_KEY_CHECKS=0")
            await cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
            tables = [row[0] for row in await cur.fetchall()]
            print(f"Found tables: {tables}")
            for table in tables:
                print(f"Dropping table: {table}")
                await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
    print("All tables dropped.")

if __name__ == "__main__":
    asyncio.run(drop_all_tables())
