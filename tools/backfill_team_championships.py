"""Backfill team_championships table with historical team data.

This script populates the team_championships table by copying current team names
for all teams that participated in each championship. Run this after adding the
team_championships table to preserve existing data.

Usage:
    python tools/backfill_team_championships.py [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from db_async import connection, query_async

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOGGER = logging.getLogger(__name__)


async def backfill_team_championships(dry_run: bool = False) -> None:
    """Backfill team_championships with current team data for all championships."""
    
    # Get all team-championship combinations from matches
    LOGGER.info("Fetching team-championship combinations from matches...")
    rows = await query_async(
        """
        SELECT DISTINCT 
            m.championship_id,
            t.team_id,
            t.name AS team_name
        FROM matches m
        CROSS JOIN (
            SELECT team1_id AS team_id FROM matches 
            UNION
            SELECT team2_id AS team_id FROM matches
        ) AS teams
        JOIN teams t ON t.team_id = teams.team_id
        WHERE teams.team_id = m.team1_id OR teams.team_id = m.team2_id
        ORDER BY m.championship_id, t.name
        """
    )
    
    LOGGER.info(f"Found {len(rows)} team-championship combinations")
    
    if dry_run:
        LOGGER.info("DRY RUN - would insert the following records:")
        for row in rows[:10]:  # Show first 10
            LOGGER.info(
                f"  {row['team_id']} / {row['championship_id']}: "
                f"{row['team_name']}"
            )
        if len(rows) > 10:
            LOGGER.info(f"  ... and {len(rows) - 10} more")
        return
    
    # Batch insert with ON DUPLICATE KEY UPDATE
    LOGGER.info("Inserting team-championship records...")
    
    async with connection(label="backfill-team-champs") as conn:
        async with conn.cursor() as cur:
            sql = """
            INSERT INTO team_championships (team_id, championship_id, team_name)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              team_name = CASE WHEN VALUES(team_name) <> '' THEN VALUES(team_name) ELSE team_championships.team_name END
            """
            
            params = [
                (
                    row["team_id"],
                    row["championship_id"],
                    row["team_name"] or "",
                )
                for row in rows
            ]
            
            await cur.executemany(sql, params)
            await conn.commit()
    
    LOGGER.info(f"Successfully inserted/updated {len(rows)} team-championship records")
    
    # Verify
    verify_rows = await query_async("SELECT COUNT(*) AS cnt FROM team_championships")
    count = verify_rows[0]["cnt"] if verify_rows else 0
    LOGGER.info(f"team_championships table now has {count} records")


async def main():
    parser = argparse.ArgumentParser(description="Backfill team_championships table")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to database",
    )
    args = parser.parse_args()
    
    try:
        await backfill_team_championships(dry_run=args.dry_run)
        LOGGER.info("Backfill completed successfully")
    except Exception as exc:
        LOGGER.exception("Backfill failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
