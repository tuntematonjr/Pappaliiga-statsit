import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Iterable

# Ensure repo root on sys.path so env_loader/db_async resolve even when run directly
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from env_loader import load_env
except Exception:
    load_env = None

if load_env:
    load_env()

from db_async import connection  # noqa: E402


async def fetch_one(cur, query: str, params: Iterable[Any] | None = None):
    await cur.execute(query, params or ())
    return await cur.fetchone()


async def fetch_count(cur, table: str, season: int | None = None):
    if season is None:
        row = await fetch_one(cur, f"SELECT COUNT(*) FROM {table}")
    else:
        row = await fetch_one(cur, f"SELECT COUNT(*) FROM {table} WHERE season = %s", (season,))
    return row[0] if row else 0


async def run_health_check(season: int | None):
    print("DATABASE_URL:", os.environ.get("DATABASE_URL", "<not set>"))
    try:
        async with connection() as conn:
            async with conn.cursor() as cur:
                pong = await fetch_one(cur, "SELECT 1")
                print("DB connectivity OK:", pong[0] if pong else None)

                if season is not None:
                    print(f"\nSeason diagnostics for season={season}")
                    print("player_season_totals rows:", await fetch_count(cur, "player_season_totals", season))
                    sums = await fetch_one(
                        cur,
                        """
                        SELECT
                            COALESCE(SUM(rounds_played),0),
                            COALESCE(SUM(kills),0),
                            COALESCE(SUM(deaths),0)
                        FROM player_season_totals
                        WHERE season = %s
                        """,
                        (season,),
                    )
                    print("player_season_totals sums (rounds,kills,deaths):", sums)
                    print("player_stats rows:", await fetch_count(cur, "player_stats", season))
                    print("maps rows:", await fetch_count(cur, "maps", season))
                    print("player_map_season_totals rows:", await fetch_count(cur, "player_map_season_totals", season))
                    print("team_map_season_totals rows:", await fetch_count(cur, "team_map_season_totals", season))
                    print("team_season_totals rows:", await fetch_count(cur, "team_season_totals", season))
    except Exception as exc:  # pragma: no cover - diagnostic script
        print("DB connection/query error:", exc)
        raise


def main():
    parser = argparse.ArgumentParser(description="Quick DB diagnostics and optional per-season counts.")
    parser.add_argument("--season", type=int, help="Season number to inspect (counts + basic sums).")
    args = parser.parse_args()
    asyncio.run(run_health_check(args.season))


if __name__ == "__main__":
    main()
