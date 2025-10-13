#!/usr/bin/env python3
"""Quick script to check player_season_totals schema"""
import asyncio
from async_db import query_async
from env_loader import load_env


async def main():
    load_env()
    rows = await query_async("DESCRIBE player_season_totals")
    print(f"player_season_totals has {len(rows)} columns:")
    for row in rows:
        print(f"  {row}")


if __name__ == "__main__":
    asyncio.run(main())
