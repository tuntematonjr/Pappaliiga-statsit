# Pappaliiga Stats – AI Agent Playbook

## Stack at a Glance
- Data sync: async pipeline in `sync.py`/`sync_pipeline.py` pulls Faceit Open + Democracy APIs via `faceit_client_async.py`, writes to MariaDB through `db_async.py` helpers; `division_registry.py` refreshes `divisions.json`.
- API: FastAPI app in `api/main.py` (uvicorn serves SPA + API); routers in `api/routers/*` with thin controllers delegating to services; async DB ops end-to-end.
- Frontend: Vue 3 SPA in `frontend/static`, Pinia stores, Vue Router history mode, no build step; served directly by uvicorn.

## Runbook
- Dev start (backend + SPA): `./scripts/dev_start_simple.ps1 -Port 8000 -VenvPath "./venv/Scripts/Activate.ps1"` (Windows) or `./scripts/dev_start_simple.sh 8000 .venv/bin/activate` (WSL/macOS/Linux).
- Manual dev: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000` (serves static assets); legacy `python frontend/spa_server.py 8080` only if you need frontend-only hosting.
- Sync data: `python sync.py` (current season) | `python sync.py --season <n>` | `python sync.py --championship-id <cid>` | `python sync.py --match-id <match>` | `python sync.py --all-seasons`; add `--refresh-divisions [--refresh-min-season N --refresh-dry-run --refresh-allow-empty]` to update divisions.json before syncing; tune concurrency with `--max-concurrency` (fetch) and `--max-db-concurrency` (writers).
- Diagnostics: sync logs rotate in `logs/`; runtime snapshots controlled by `SYNC_DIAGNOSTICS` land in `logs/runtime_diagnostics.jsonl`; rate-limit stats printed at end of sync; `scripts/db_diag.py --season N` gives quick counts.
- DB utilities: `python tools/check_db_connection.py` | `python tools/apply_schema.py` | `python tools/recompute_totals.py` (supports `--season`).

## Backend Conventions
- Async-only; wrap DB work with `connection()` or `readonly_connection()` from `db_async.py` (writes auto-commit/rollback).
- For write batches use `db_async.py` bulk helpers (`upsert_*`, `replace_map_votes_async`, `clear_obsolete_maps_async`) and guard custom writes with `_retry_on_deadlock(..., max_attempts=3)`.
- Keep routers thin; business logic in `api/services/*`; cache expensive aggregations with `AsyncTTLCache` when results are reused.
- Faceit access: `faceit_client_async.py` enforces 10k/hr + adaptive backoff (max 8 concurrent HTTP requests); avoid extra loops that skip the limiter.
- Pool sizing: asyncmy pool defaults min 2 / max 30; sync workers expect `DB_CONNECTIONS_PER_WORKER` (default 3) and `MAX_DB_WRITER_CONCURRENCY` (default 6) to fit inside that pool; `DB_POOL_DEBUG` controls pool tracing.

## Data & Schema Notes
- Championships include `is_playoffs`, `slug`, `parent_championship_id`, `winner_team_id`; playoffs auto-link to regular-season parents by slug match in `sync_pipeline.py`.
- Division registry: `division_registry.py` discovers championships for the organizer and writes `divisions.json`; `faceit_config` reloads it to expose `DIVISIONS` and `CURRENT_SEASON`.
- Division overrides: `division_overrides.json` marks banned/quit teams; matches they touch set `ignored_due_ban=1` and are excluded from stats.
- Match normalization in `sync_pipeline.py` merges Faceit `rounds`/`detailed_results`, detects forfeits, stores map votes, and writes map/team/player totals; pending matches are re-checked every ~15 minutes.
- Schema: computed columns in `player_stats`/`team_stats` derive metrics from JSON; `maps_catalog` stores display metadata; `maps` rows flag `is_forfeit` per map.

## Frontend Guidance (Vue SPA)
- Routes (history mode): `/`, `/seasons`, `/division/:championshipId`, `/division/:championshipId/playoffs`, `/team/:teamId`, `/team/:championshipId/:teamId`, `/player/:playerId`.
- Data flow: Pinia stores in `frontend/static/stores/*` (notably `useTeamStore`, `useSeasonsStore`, `useDivisionStore`); `TeamDetail` uses `apiClient.getTeamPage(teamId, championshipId)` and mirrors `championship` query for sharable links.
- Season selector defaults to latest; use championship names to differentiate playoffs vs regular.
- API base resolved at runtime (`window.__API_BASE__` or origin + `/api`); uvicorn already serves the SPA so no build step.

## Common Pitfalls
1) Exceeding DB pool budget when raising `--max-concurrency`/`--max-db-concurrency` without bumping `DB_POOL_MAX_SIZE`.
2) Skipping `_retry_on_deadlock` or bypassing `db_async` helpers for writes.
3) Losing playoff linkage or `winner_team_id`; use nulls instead of bad IDs to satisfy FK constraints.
4) Forgetting banned-team handling—set `ignored_due_ban` so stats exclude those matches.
5) Bypassing `faceit_client_async` for HTTP loops (ignores limiter) or running sync without diagnostics when stuck (`SYNC_DIAGNOSTICS`).
6) Breaking SPA deep links—keep backend fallback in place and include `championship` query on team links inside a division.

## Key Files
- Data sync: `sync.py`, `sync_pipeline.py`, `division_registry.py`, `faceit_client_async.py`, `division_overrides.json`.
- DB layer: `db_async.py`, `mariadb_schema.sql`.
- API: `api/main.py`, routers `api/routers/{seasons,season_view,divisions,championships,teams,players,matches,stats,maps_catalog}.py`, services `api/services/{seasons_service,season_view_service,stats_service,teams_service}.py`.
- Frontend: `frontend/static/app-main.js`, `frontend/static/api-client.js`, views `frontend/static/views/{HomeView.js,SeasonsView.js,DivisionView.js,TeamDetailView.js,PlayerView.js}`, components under `frontend/static/components/`.

## Environment
Required (`.env` in repo root):
```
FACEIT_API_KEY=your_faceit_api_key_here
DATABASE_URL=mariadb://user:pass@host:3306/pappaliiga
```
Useful overrides:
```
DB_POOL_MIN_SIZE=2
DB_POOL_MAX_SIZE=30
DB_CONNECTIONS_PER_WORKER=3
MAX_DB_WRITER_CONCURRENCY=6
DB_POOL_DEBUG=1
SYNC_DIAGNOSTICS=1
SYNC_DIAGNOSTICS_INTERVAL=15
SYNC_DIAGNOSTICS_PATH=logs/runtime_diagnostics.jsonl
SYNC_LOG_DIR=logs
SYNC_LOG_MAX_FILES=10
```
