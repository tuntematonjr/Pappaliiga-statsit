# Pappaliiga Stats - AI Agent Instructions

## Architecture Overview

**Three-tier system**: Async data sync pipeline → FastAPI REST API → Vue 3 SPA frontend

- **Data pipeline** (`sync.py`, `sync_pipeline.py`): Fetches Faceit API data with adaptive rate limiting, normalizes stats, persists to MariaDB
- **API layer** (`api/`): FastAPI server with service-based architecture, TTL caching, and comprehensive async DB operations
- **Frontend** (`frontend/`): Vue 3 SPA with Pinia stores, component-based UI, client-side routing via Vue Router

## Critical Workflows

### Running the application locally
```powershell
# Start both backend and frontend (opens two terminal windows)
.\scripts\dev_start.ps1

# Or manually:
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
python frontend\spa_server.py 8080
```

### Syncing data from Faceit
```powershell
# Full sync of current season
python sync.py

# Sync specific season
python sync.py --season 11

# Reset database and resync
python sync.py --reset

# Update single match
python sync.py --match <match_id>
```

### Database operations
```powershell
# Check database connection
python tools\check_db_connection.py

# Apply schema changes
python tools\apply_schema.py

# Recompute aggregated totals
python tools\recompute_totals.py
```

## Project Conventions

### Async everywhere
- All I/O operations use `async`/`await`
- Use `db_async.py` connection helpers: `connection()`, `readonly_connection()`, `transaction()`
- Database pool sizing: `DEFAULT_POOL_MAX_SIZE = 30` (sized for 10 concurrent championships × 3 connections each)
- Never block the event loop - use `asyncio.create_task()` for concurrent operations

### Database patterns
```python
# Transactional writes
async with connection(label="descriptive-label") as conn:
    await upsert_match_async(conn, match_data)
    # conn.commit() called automatically on exit

# Readonly queries (uses separate pool)
async with readonly_connection(label="query-teams") as conn:
    teams = await fetch_all("SELECT * FROM teams WHERE ...", params)

# Retry on deadlock (built into db_ops_async)
await _retry_on_deadlock(
    lambda: upsert_operation(data),
    label="operation-name",
    max_attempts=3
)
```

### Rate limiting strategy
- Faceit API: 10,000 requests/hour hard limit
- Adaptive throttling via `AdaptiveLimiter` in `faceit_client_async.py`
- Backs off exponentially on 429 errors, recovers gradually on success
- Track usage: `get_rate_limit_stats()` shows calls/hour and current delay

### Service layer architecture
- Services in `api/services/` contain business logic, no direct DB queries in routers
- Use `AsyncTTLCache` for expensive aggregations: default 30s TTL, 128 entries
- Example: `stats_service.get_season_stats()` caches division standings computation

### Division overrides system
- `division_overrides.json`: Define banned/quit teams per championship
- Effects: matches with banned teams set `ignored_due_ban=1`, excluded from stats
- Load via `load_division_overrides()` which returns dict[championship_id, {banned_teams, quit_teams}]

### Frontend API integration
- API base URL: runtime injection via `window.__API_BASE__` in `api/main.py`
- API client: `frontend/static/api-client.js` wraps fetch with base URL resolution
- Fallback: hostname heuristic (`localhost:8080` → `http://localhost:8000/api`)

### Match data normalization
- Parse Faceit API responses in `sync_pipeline.py:_build_normalised_match()`
- Handles multiple API formats: `rounds` array from stats, `detailed_results` from match details
- Forfeit detection: `score_team1 + score_team2 == 0` or `map_name == 'forfeit'`
- Map votes: Democracy API provides veto/pick sequence, round 7 special handling (BO3 decider vs BO2 overflow)

### Diagnostic logging
- `runtime_diagnostics.py`: Emits JSONL snapshots to `logs/runtime_diagnostics.jsonl` every 15s during sync
- Track progress: `diagnostics.mark_progress("match", match_id)`
- Pool monitoring: includes MariaDB connection pool saturation metrics when `DB_POOL_DEBUG=1`

## Common Pitfalls

1. **Deadlocks in DB writes**: Use `_retry_on_deadlock()` wrapper for all upsert operations in `db_ops_async.py`
2. **Forgetting `await`**: All async functions must be awaited; use `asyncio.create_task()` for fire-and-forget
3. **Rate limit exhaustion**: Sync pipeline throttles aggressively - don't add naive parallel loops over Faceit API calls
4. **Schema FK constraints**: Championships must exist before matches; use `upsert_championships_async()` before syncing matches
5. **Frontend routing**: SPA uses history mode - backend must serve `index.html` for unknown paths (already handled in `api/main.py`)

## Key Files Reference

- `sync_pipeline.py`: Match normalization, stat aggregation logic (~1200 lines)
- `db_async.py`: Connection pooling, transaction helpers, schema management
- `db_ops_async.py`: All database write operations with deadlock retry logic (~2000 lines)
- `faceit_client_async.py`: Rate-limited HTTP client with adaptive throttling
- `api/main.py`: FastAPI app entrypoint, CORS config, SPA fallback routing
- `mariadb_schema.sql`: Complete schema definition, materialized via `create_schema_async()`

## Environment Setup

Required `.env` file in repo root:
```env
FACEIT_API_KEY=your_faceit_api_key_here
DATABASE_URL=mariadb://user:pass@host:3306/pappaliiga
```

Optional tuning:
```env
DB_POOL_MAX_SIZE=30          # Connection pool size
MAX_DB_WRITER_CONCURRENCY=6  # Max concurrent DB writes
SYNC_DIAGNOSTICS=1           # Enable diagnostic logging
```
