# Pappaliiga Stats - AI Agent Instructions

## Project Overview
CS2 tournament statistics platform for Finnish Pappaliiga league. Two-part system: **sync pipeline** (Python async) pulls data from Faceit API into MariaDB, **REST API + SPA** (FastAPI + Vue) serves statistics to users.

## Architecture

### Core Components
- **sync.py / sync_pipeline.py**: Async ETL pipeline syncing Faceit championship/match/player data to MariaDB with concurrent workers (max 10 championships parallel)
- **db_async.py**: Async MariaDB connection pool (asyncmy), explicit transactions via `connection()` context manager, no ORM
- **db_ops_async.py**: Database write operations with automatic deadlock retry logic (3 attempts, exponential backoff)
- **api/main.py**: FastAPI backend serving REST endpoints and SPA
- **frontend/**: Vanilla Vue3 SPA with component-based architecture, no build step

### Data Flow
1. Sync pipeline: Faceit API → `faceit_client_async.py` (rate-limited) → `sync_pipeline.py` (transform) → `db_ops_async.py` (upsert) → MariaDB
2. API requests: Client → FastAPI router → service layer (`api/services/`) → `db_async.py` (read-only) → response

## Critical Patterns

### Database Transactions
**Always use explicit commits:**
```python
async with connection(label="my_operation") as conn:
    # Your queries here
    await conn.commit()  # REQUIRED - autocommit is disabled
```

**For operations that might deadlock (writes in concurrent context):**
```python
await _retry_on_deadlock(
    lambda: upsert_teams_bulk_async(teams, conn=conn),
    label="upsert_teams",
    max_attempts=3
)
```

### Pool Size vs Concurrency
- Pool max = 30 connections (3x max concurrency)
- Each worker may hold multiple connections during retries
- Never set pool < 3x your concurrent workers or deadlocks occur

### Rate Limiting (Faceit API)
- `AdaptiveLimiter` in `faceit_client_async.py` backs off on 429s
- 10k requests/hour limit tracked internally
- All Faceit calls use `httpx.AsyncClient` with retry logic via `tenacity`

### Division Overrides
- `division_overrides.json` defines banned/quit teams per championship
- Matches with banned teams flagged `ignored_due_ban=1` in DB
- Load overrides via `load_division_overrides()` before sync operations

## Development Workflows

### Running Locally
```powershell
# Start backend + frontend (dev helper, opens separate terminals)
.\scripts\dev_start.ps1

# OR manually:
# Backend: python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
# Frontend: python frontend/spa_server.py 8001
```

### Sync Operations
```powershell
# Full sync (current season only)
python sync.py --all-seasons

# Specific season
python sync.py --season 11

# Reset DB and resync (DEV ONLY - destructive)
python sync.py --reset-db --force-reset --all-seasons

# Single match resync
python sync.py --match-id <match_id>

# Refresh divisions.json from Faceit
python sync.py --refresh-divisions
```

### Environment Setup
- `.env` file in repo root (use `env_loader.py`, no external deps)
- Required: `FACEIT_API_KEY`, `DATABASE_URL=mariadb://user:pass@host:3306/database`
- Optional: `DB_POOL_MAX_SIZE`, `MAX_DB_WRITER_CONCURRENCY`

## Code Conventions

### Naming & Style
- **Python**: snake_case for functions/variables, PascalCase for classes, type hints everywhere (`from __future__ import annotations`)
- **Database**: snake_case table/column names, explicit foreign keys with cascade rules
- **API models**: Use `CamelModel` base class (auto snake_case → camelCase serialization via Pydantic)

### Error Handling
- Custom exceptions: `NotFoundError`, `BadRequestError` (inherit from `ServiceError` in `api/exceptions.py`)
- FastAPI exception handlers convert service errors to proper HTTP status codes
- Faceit client raises `RateLimitError` internally for retry logic only

### Async Consistency
- ALL database operations are async (asyncmy + asyncio)
- Use `async with connection()` for read queries, never acquire raw pool connections
- Service layer functions always async: `async def get_team_stats(...) -> dict`

## Testing & Verification
```powershell
# Post-sync verification queries (checks row counts, forfeit consistency)
python sync.py --verify

# Check specific championship sync
python sync.py --championship-id <champ_id> --full
```

## Gotchas

1. **Frontend API base**: Runtime-injected via `window.__API_BASE__` by backend when serving SPA (see `api/main.py` lifespan). Dev fallback uses hostname heuristic.

2. **Schema migrations**: No automated migrations. Apply changes manually via `mariadb_schema.sql`, then run `python sync.py --create-schema` (idempotent, creates missing tables only).

3. **Concurrent writes**: Always wrap bulk upserts in `_retry_on_deadlock()` when syncing multiple championships in parallel. MariaDB row-level locks + high concurrency = deadlocks.

4. **Division registry**: `divisions.json` is source of truth for championships. Update via `division_registry.py::refresh_divisions()` or CLI flag `--refresh-divisions`.

5. **Season numbering**: Default current season in `faceit_config.py::DEFAULT_CURRENT_SEASON`. Update when new season starts.

## File Reference

| File | Purpose |
|------|---------|
| `sync_pipeline.py` | Championship sync orchestration, match/player/team aggregation |
| `db_async.py` | Connection pool, transaction helpers, schema management |
| `db_ops_async.py` | All database writes (upserts, deletes), retry logic |
| `faceit_client_async.py` | Faceit API client with rate limiting & circuit breaker |
| `faceit_config.py` | API keys, division list, environment config |
| `api/services/*.py` | Business logic for API endpoints (thin layer over SQL) |
| `api/routers/*.py` | FastAPI route handlers (validation, pagination) |
| `mariadb_schema.sql` | Complete database schema (MariaDB 10.6+) |

## When Adding Features

1. **New API endpoint**: Add route in `api/routers/`, service function in `api/services/`, response model as `CamelModel` in `api/models.py`
2. **New sync data**: Extend `sync_pipeline.py` fetch logic, add DB columns/tables in `mariadb_schema.sql`, create upsert helper in `db_ops_async.py`
3. **Performance issues**: Check pool diagnostics via `db_async._POOL_DIAGNOSTICS.snapshot()`, verify concurrent worker count vs pool size
