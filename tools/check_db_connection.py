import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from env_loader import load_env  # noqa: E402

load_env(PROJECT_ROOT)

from db_async import readonly_connection


async def main() -> None:
    async with readonly_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT DATABASE()")
            row = await cur.fetchone()
            print("DATABASE()", row[0] if row else None)


if __name__ == "__main__":
    asyncio.run(main())
