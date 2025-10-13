import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from env_loader import load_env  # noqa: E402

load_env(PROJECT_ROOT)

from db_async import create_schema_async  # noqa: E402


async def main() -> None:
    await create_schema_async(force=True)


if __name__ == "__main__":
    asyncio.run(main())
