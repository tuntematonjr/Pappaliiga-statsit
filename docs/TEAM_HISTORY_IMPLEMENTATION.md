# Team Names - Historical Data Implementation

## Problem
Team names can change over time. FACEIT only provides current team information, so when we sync historical matches, old team names get overwritten with current ones. This causes historical matches to display incorrect team branding.

Note: **Logos are excluded** because FACEIT only stores current URLs, which would break or change when teams rebrand, defeating the purpose of historical preservation.

## Solution
Created a `team_championships` table to store team name as they appeared in each championship. This preserves historical accuracy while still allowing teams to update their current branding.

## Database Schema

### New Table: `team_championships`
```sql
CREATE TABLE IF NOT EXISTS team_championships (
    team_id VARCHAR(64) NOT NULL,
    championship_id VARCHAR(64) NOT NULL,
    team_name VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (team_id, championship_id),
    KEY idx_team_championships_championship (championship_id),
    CONSTRAINT fk_team_championships_team FOREIGN KEY (team_id)
        REFERENCES teams (team_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_team_championships_championship FOREIGN KEY (championship_id)
        REFERENCES championships (championship_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### Relationship
- **teams**: Stores current team information (latest name)
- **team_championships**: Stores historical team names per championship
- When displaying data, queries use `COALESCE(tc.team_name, t.name)` to prefer historical data if available

## Implementation Changes

### 1. Database Layer (`db_async.py`)
- Added `upsert_team_championships_bulk_async()` function
- Updated `get_team_matches_mirror_async()` to LEFT JOIN with team_championships

### 2. Sync Pipeline (`sync_pipeline.py`)
- Import `upsert_team_championships_bulk_async`
- After upserting teams, now also upserts team_championships records
- Applies to:
  - Match-level team sync (when processing individual matches)
  - Championship-level team sync (when syncing full championships)
  - Single match updates

### 3. API Services
Updated queries to use historical team names:
- **matches_service.py**: Match list and match details queries
- **divisions_service.py**: Division standings query
- **seasons_service.py**: Playoff bracket query
- **db_async.py**: Team matches mirror query

Pattern used:
```sql
LEFT JOIN team_championships tc ON tc.team_id = t.team_id AND tc.championship_id = :champ_id
...
COALESCE(tc.team_name, t.name) AS team_name
```

### 4. Frontend
No changes needed - the frontend automatically receives correct historical team names from the API.

## Migration

### Applying Schema Changes
```bash
# Apply the schema changes
python tools/apply_schema.py
```

### Backfilling Existing Data
```bash
# Preview what will be backfilled
python tools/backfill_team_championships.py --dry-run

# Perform the backfill
python tools/backfill_team_championships.py
```

The backfill script:
1. Finds all team-championship combinations from existing matches
2. Copies current team name for each combination
3. Uses ON DUPLICATE KEY UPDATE to avoid conflicts

### Future Syncs
After applying schema changes, all future syncs will automatically populate `team_championships` with current team names as matches are synced.

## Data Flow

1. **FACEIT API** provides current team name
2. **Sync Pipeline** writes to both:
   - `teams` table (current data, gets overwritten)
   - `team_championships` table (historical snapshot, preserved)
3. **API Queries** prefer `team_championships` data when available
4. **Frontend** displays historical team names correctly

## Benefits

- ✅ Historical matches show correct team names
- ✅ Teams can rename without losing history
- ✅ No manual intervention needed after initial migration
- ✅ Backward compatible (falls back to teams table if no historical data)
- ✅ Future-proof (automatically captures new data)

## About Logos

Logos (avatars) are **not stored** for the following reason:
- FACEIT only provides current image URLs
- These URLs will either break or redirect to new logos when teams rebrand
- Storing a URL that becomes invalid/incorrect defeats the purpose of historical preservation
- To properly preserve logos would require downloading and locally archiving images

Current logos are served from the `teams` table. If historical logo preservation becomes critical, implement proper image archival.

## Testing

After migration:
1. Check an old match - should show team names as they were then
2. Change a team name in FACEIT and resync
3. Verify old matches still show old name
4. Verify new matches show new name

## Notes

- The backfill uses current team names, so if a team already changed names, historical matches will initially show the wrong name until the next actual historical sync captures the data from FACEIT
- Consider running syncs for old seasons to capture actual historical data if critical
- The `teams` table remains the source of truth for current team information
