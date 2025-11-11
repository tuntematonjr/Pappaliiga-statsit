"""
Reset database - drop all tables and recreate schema.
Use with caution - this will delete ALL data!
"""
from __future__ import annotations

import asyncio
import sys

import env_loader  # Load environment variables
from db_async import get_pool


async def reset_database(force: bool = False):
    """Drop all tables and recreate from schema."""
    if not force:
        print("⚠️  WARNING: This will DROP ALL TABLES and recreate the schema!")
        response = input("Type 'YES' to confirm: ")
        if response != "YES":
            print("Aborted.")
            return

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Get all tables
            await cur.execute("SHOW TABLES")
            tables = await cur.fetchall()
            
            if tables:
                print(f"Found {len(tables)} tables to drop...")
                
                # Disable foreign key checks
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                
                # Drop all tables
                for row in tables:
                    table_name = list(row.values())[0]
                    print(f"  Dropping table: {table_name}")
                    await cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                
                # Re-enable foreign key checks
                await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                
                await conn.commit()
                print("✓ All tables dropped")
            else:
                print("No tables found")
    
    # Apply schema
    print("\nApplying schema from mariadb_schema.sql...")
    from tools.apply_schema import apply_schema_async
    await apply_schema_async()
    print("✓ Schema applied successfully")
    print("\n✅ Database reset complete!")


if __name__ == "__main__":
    force = "--force" in sys.argv
    if force:
        print("⚠️  Force mode enabled - skipping confirmation")
    asyncio.run(reset_database(force=force))
