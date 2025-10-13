"""
Clean database reset utility - drops all tables and recreates schema.
Usage: python reset_db_clean.py
"""
import asyncio
from db_async import connection
from env_loader import load_env
from pathlib import Path

async def reset_clean():
    load_env()
    
    async with connection() as conn:
        async with conn.cursor() as cur:
            # Disable foreign key checks
            await cur.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Get all tables
            await cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
            tables = [row[0] for row in await cur.fetchall()]
            print(f"Found {len(tables)} tables to drop: {tables}")
            
            # Drop each table
            for table in tables:
                print(f"Dropping table: {table}")
                await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            
            # Re-enable foreign key checks
            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print("All tables dropped successfully.")
            
            # Read and execute schema
            schema_path = Path(__file__).with_name("mariadb_schema.sql")
            sql = schema_path.read_text(encoding="utf-8")
            
            # Split by semicolon and execute each statement
            statements = [chunk.strip() for chunk in sql.split(";") if chunk.strip()]
            print(f"\nCreating schema with {len(statements)} statements...")
            
            for i, stmt in enumerate(statements, 1):
                try:
                    await cur.execute(stmt)
                    if "CREATE TABLE" in stmt.upper():
                        # Extract table name for logging
                        table_name = stmt.split("CREATE TABLE IF NOT EXISTS")[1].split("(")[0].strip()
                        print(f"  [{i}/{len(statements)}] Created table: {table_name}")
                except Exception as e:
                    print(f"  [{i}/{len(statements)}] ERROR: {e}")
                    print(f"  Statement: {stmt[:100]}...")
                    raise
            
            print("\nSchema created successfully!")

if __name__ == "__main__":
    asyncio.run(reset_clean())
