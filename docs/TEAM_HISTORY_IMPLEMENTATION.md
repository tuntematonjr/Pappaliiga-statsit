# Team Name History

Quick reference for preserving historical team names by championship.

## Why
FACEIT team names can change over time. Without history, old matches show the latest team name.

## Data Model
- Current team identity: `teams`
- Historical per-championship name: `team_championships`
- Key: `(team_id, championship_id)`

Read pattern:

```sql
LEFT JOIN team_championships tc
  ON tc.team_id = t.team_id
 AND tc.championship_id = :champ_id
COALESCE(tc.team_name, t.name) AS team_name
```

## Write Path
- Sync writes current team info to `teams`.
- Sync also upserts `team_championships` via `upsert_team_championships_bulk_async()` in `db_async.py`.
- Used during championship sync and match-level updates in `sync_pipeline.py`.

## Apply / Backfill
Apply schema:

```bash
mysql -u <user> -p <database> < mariadb_schema.sql
```

Backfill existing rows:

```bash
python tools/backfill_team_championships.py --dry-run
python tools/backfill_team_championships.py
```

## Limitations
- Only names are historized.
- Logos are not historized (current logos still come from `teams`).

## Validation Checklist
1. Open an old match and verify team names.
2. Resync after a team rename.
3. Confirm old matches keep old names and new matches show new names.
