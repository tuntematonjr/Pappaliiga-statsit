import asyncio
import sys
from pathlib import Path

# Ensure the repository root is on sys.path so imports like `env_loader` and `db_async` work
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from env_loader import load_env

# Load .env from the repository root (if present) so DATABASE_URL can be set there.
load_env()

from db_async import connection

async def main():
    season = 11
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM player_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_season_totals rows:', r[0] if r else 0)

            await cur.execute("SELECT COALESCE(SUM(rounds_played),0), COALESCE(SUM(kills),0), COALESCE(SUM(deaths),0) FROM player_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('SUM rounds_played, kills, deaths:', r)

            await cur.execute("SELECT COUNT(*) FROM player_stats WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_stats rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM maps WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('maps rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM player_map_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('player_map_season_totals rows:', r[0] if r else 0)

            await cur.execute("SELECT COUNT(*) FROM team_map_season_totals WHERE season = %s", (season,))
            r = await cur.fetchone()
            print('team_map_season_totals rows:', r[0] if r else 0)

if __name__ == '__main__':
    asyncio.run(main())
