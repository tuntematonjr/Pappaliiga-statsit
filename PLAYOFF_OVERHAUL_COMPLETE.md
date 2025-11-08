# Playoff Division Overhaul - Implementation Complete

## Summary

Successfully implemented a full backend-to-frontend solution to fix playoff divisions appearing as separate cards. The root cause was identified as client-side slug-based heuristics failing due to naming variations. The solution establishes an explicit parent-child relationship in the database.

## Changes Made

### 1. Database Schema (`mariadb_schema.sql`)
- ✅ Added `parent_championship_id VARCHAR(64) NULL` column to championships table
- ✅ Created index `idx_championships_parent` on parent_championship_id
- ✅ Added foreign key constraint with CASCADE behavior

### 2. Backend Data Layer (`db_ops_async.py`)
- ✅ Updated `_CHAMPIONSHIP_UPSERT_SQL` to include parent_championship_id column
- ✅ Modified `_prepare_championship_payload` to extract parent_championship_id from row data

### 3. Sync Pipeline (`sync_pipeline.py`)
- ✅ Added `derive_slug_base()` helper function to strip playoff suffixes
- ✅ Created `find_parent_championship_id()` async function to query for parent divisions
- ✅ Updated both `sync_championship_async()` and `update_single_match_async()` to call find_parent_championship_id before upserting
- ✅ Added `import re` for regex-based slug processing

### 4. API Models (`api/routers/divisions.py`)
- ✅ Added `parent_championship_id: Optional[str] = None` to DivisionSummary Pydantic model
- ✅ Added `parent_championship_id: Optional[str] = None` to DivisionDetails Pydantic model
- Note: `_normalize_division_summary` already passes through all fields, so no changes needed there

### 5. API Services (`api/services/divisions_service.py`)
- ✅ Updated `list_divisions()` query to SELECT parent_championship_id
- ✅ Updated `list_divisions_by_season()` query to:
  - SELECT `c.parent_championship_id`
  - Use `c.is_playoffs AS is_playoff` instead of CASE statement
  - Include `c.parent_championship_id` in GROUP BY clause
- ✅ Updated `_fetch_champ_row()` to SELECT parent_championship_id

### 6. Backfill Script (`tools/backfill_playoff_parents.py`)
- ✅ Created new script with --dry-run and --apply flags
- ✅ Implements slug-based inference (strips -po, -playoffs, -pudotuspelit)
- ✅ Matches on season + division_num + slug base
- ✅ Provides detailed logging of parent assignments and orphans

### 7. Frontend Cleanup (`frontend/static/components/DivisionCardList.js`)
- ✅ Removed debug console.log statements from normalizedDivisions computed property
- ✅ Removed orphan warning console.warn statement
- Note: The existing `derivePlayoffParentKey()` function already checks for `parentChampionshipId` (camelCase) first, so frontend will automatically use the new field

## How It Works

### Data Flow:
1. **During Sync** → `sync_pipeline.py` detects playoff division → queries for matching non-playoff parent → stores parent_championship_id
2. **In Database** → championships.parent_championship_id points to parent championship_id
3. **API Response** → parent_championship_id returned as `parentChampionshipId` (camelCase via CamelModel)
4. **Frontend** → `derivePlayoffParentKey()` extracts `parentChampionshipId` → uses it as grouping key → playoff card nested in parent card

### Fallback Logic:
- If `parentChampionshipId` is present in API response → use it (primary)
- Else if slug can be parsed → use slug base (fallback)
- Else if season + division_num available → use `{season}-{divisionNum}` (fallback)
- Else use canonical key (last resort)

## Testing Steps

### 1. Apply Schema Changes
```powershell
python tools\apply_schema.py
```

### 2. Backfill Existing Data
```powershell
# Preview changes first
python tools\backfill_playoff_parents.py --dry-run

# Review output, then apply if looks correct
python tools\backfill_playoff_parents.py --apply
```

### 3. Run Sync Pipeline
```powershell
# This will test that new syncs automatically set parent_championship_id
python sync.py
```

### 4. Start Development Servers
```powershell
.\scripts\dev_start_simple.ps1 -FrontendLiveReload
```

### 5. Verify Frontend
- Open http://localhost:8001
- Check that PO divisions appear **inside** their parent division cards
- No separate top-level playoff cards should exist
- Check browser console - should be **no debug logs** about divisions/keys/playoffs

### 6. Verify API Response
```powershell
# Check that API returns parentChampionshipId
curl http://localhost:8000/divisions/by-season?season=10 | ConvertFrom-Json | Select-Object -ExpandProperty divisions | Where-Object { $_.isPlayoff } | Format-Table name, championshipId, parentChampionshipId
```

## Expected Behavior

### Before Fix:
- ❌ "Divisioona 1 Pudotuspelit" appears as separate top-level card
- ❌ "Divisioona 1" appears without playoff section
- ❌ Duplicate/confusing layout

### After Fix:
- ✅ "Divisioona 1" main card contains nested playoff section
- ✅ "Divisioona 1 Pudotuspelit" appears **inside** parent card
- ✅ Single, clean card per division with optional playoff section

## Rollback Plan (If Needed)

If issues occur:
1. The new column is nullable, so no data integrity issues
2. Frontend has fallbacks to slug-based matching
3. To completely rollback:
   ```sql
   ALTER TABLE championships DROP FOREIGN KEY fk_championships_parent;
   ALTER TABLE championships DROP INDEX idx_championships_parent;
   ALTER TABLE championships DROP COLUMN parent_championship_id;
   ```

## Files Modified

- `mariadb_schema.sql`
- `db_ops_async.py`
- `sync_pipeline.py`
- `api/routers/divisions.py`
- `api/services/divisions_service.py`
- `frontend/static/components/DivisionCardList.js`

## Files Created

- `tools/backfill_playoff_parents.py`
- `PLAYOFF_OVERHAUL_COMPLETE.md` (this file)

## Notes

- The frontend code already had logic to handle `parentChampionshipId` - the previous attempts just lacked the backend data
- All changes are backward-compatible (nullable column, fallback logic preserved)
- Foreign key constraint ensures referential integrity
- Backfill script is idempotent (safe to run multiple times)

---

**Status**: ✅ Implementation Complete - Ready for Testing
**Date**: 2024
**Branch**: new-sync
