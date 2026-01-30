"""Migrate player_stats from JSON to individual columns.

WARNING: This will drop and recreate the player_stats table, losing all data.
You must re-sync after running this migration.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db_async import connection


async def main():
    print("WARNING: This will drop the player_stats table and all its data!")
    print("You will need to re-sync all matches after this migration.")
    
    response = input("\nType 'YES' to proceed: ")
    if response != "YES":
        print("Migration cancelled.")
        return
    
    async with connection(label="migrate-player-stats") as conn:
        async with conn.cursor() as cur:
            print("\n1. Dropping player_stats table...")
            await cur.execute("DROP TABLE IF EXISTS player_stats")
            
            print("2. Creating new player_stats table with individual columns...")
            await cur.execute("""
                CREATE TABLE player_stats (
                    player_stat_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    season SMALLINT NOT NULL,
                    division_num SMALLINT NOT NULL,
                    match_id VARCHAR(64) NOT NULL,
                    round_index SMALLINT NOT NULL,
                    map_id BIGINT UNSIGNED NULL,
                    player_id VARCHAR(64) NOT NULL,
                    team_id VARCHAR(64) NULL,
                    opponent_team_id VARCHAR(64) NULL,
                    is_forfeit_map TINYINT(1) NOT NULL DEFAULT 0,
                    -- Core stats
                    kills SMALLINT NOT NULL DEFAULT 0,
                    deaths SMALLINT NOT NULL DEFAULT 0,
                    assists SMALLINT NOT NULL DEFAULT 0,
                    mvps SMALLINT NOT NULL DEFAULT 0,
                    headshots SMALLINT NOT NULL DEFAULT 0,
                    damage INT NOT NULL DEFAULT 0,
                    -- Weapon-specific
                    sniper_kills SMALLINT NOT NULL DEFAULT 0,
                    pistol_kills SMALLINT NOT NULL DEFAULT 0,
                    knife_kills SMALLINT NOT NULL DEFAULT 0,
                    zeus_kills SMALLINT NOT NULL DEFAULT 0,
                    first_kills SMALLINT NOT NULL DEFAULT 0,
                    -- Utility
                    enemies_flashed SMALLINT NOT NULL DEFAULT 0,
                    flash_count SMALLINT NOT NULL DEFAULT 0,
                    flash_successes SMALLINT NOT NULL DEFAULT 0,
                    utility_damage INT NOT NULL DEFAULT 0,
                    utility_count SMALLINT NOT NULL DEFAULT 0,
                    utility_successes SMALLINT NOT NULL DEFAULT 0,
                    utility_enemies SMALLINT NOT NULL DEFAULT 0,
                    -- Multikills
                    mk_2k SMALLINT NOT NULL DEFAULT 0,
                    mk_3k SMALLINT NOT NULL DEFAULT 0,
                    mk_4k SMALLINT NOT NULL DEFAULT 0,
                    mk_5k SMALLINT NOT NULL DEFAULT 0,
                    -- Clutch
                    clutch_kills SMALLINT NOT NULL DEFAULT 0,
                    cl_1v1_attempts SMALLINT NOT NULL DEFAULT 0,
                    cl_1v1_wins SMALLINT NOT NULL DEFAULT 0,
                    cl_1v2_attempts SMALLINT NOT NULL DEFAULT 0,
                    cl_1v2_wins SMALLINT NOT NULL DEFAULT 0,
                    -- Entry
                    entry_count SMALLINT NOT NULL DEFAULT 0,
                    entry_wins SMALLINT NOT NULL DEFAULT 0,
                    -- Ratios and percentages
                    kd FLOAT NOT NULL DEFAULT 0,
                    kr FLOAT NOT NULL DEFAULT 0,
                    adr FLOAT NOT NULL DEFAULT 0,
                    hs_pct DECIMAL(6,3) NOT NULL DEFAULT 0,
                    result TINYINT(1) NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (player_stat_id),
                    KEY idx_player_stats_match_round (match_id, round_index),
                    KEY idx_player_stats_season_division (season, division_num),
                    KEY idx_player_stats_player (player_id),
                    KEY idx_player_stats_team (team_id),
                    KEY idx_player_stats_map (map_id),
                    UNIQUE KEY uq_player_stats_match_round_player (match_id, round_index, player_id),
                    CONSTRAINT fk_player_stats_match FOREIGN KEY (match_id) REFERENCES matches (match_id) ON DELETE CASCADE,
                    CONSTRAINT fk_player_stats_map FOREIGN KEY (map_id) REFERENCES maps (map_id) ON DELETE SET NULL,
                    CONSTRAINT fk_player_stats_player FOREIGN KEY (player_id) REFERENCES players (player_id),
                    CONSTRAINT fk_player_stats_team FOREIGN KEY (team_id) REFERENCES teams (team_id),
                    CONSTRAINT fk_player_stats_opponent FOREIGN KEY (opponent_team_id) REFERENCES teams (team_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            print("3. Clearing dependent aggregation tables...")
            await cur.execute("TRUNCATE TABLE player_season_totals")
            await cur.execute("TRUNCATE TABLE player_map_season_totals")
            
        await conn.commit()
    
    print("\n✅ Migration complete!")
    print("\nNext steps:")
    print("1. Run: python sync.py")
    print("   (or python sync.py --all-seasons to resync everything)")


if __name__ == "__main__":
    asyncio.run(main())
