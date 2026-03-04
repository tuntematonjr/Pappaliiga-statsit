# Pappaliiga Stats – AI Agent Playbook

## Overview
- Data sync: async pipeline in `sync.py`/`sync_pipeline.py` pulls Faceit Open + Democracy APIs via `faceit_client_async.py`, writes to MariaDB through `db_async.py`, and refreshes divisions via `division_registry.py` → `divisions.json`.
- API: FastAPI app in `api/main.py` (uvicorn serves SPA + API); routers in `api/routers/*` delegate to `api/services/*`; async DB ops end-to-end.
- Frontend: Vue 3 SPA in `frontend/static`, Pinia stores, Vue Router history mode, no build step; served by uvicorn.

## Runbook
### Start dev server
- Windows: `./scripts/dev_start_simple.ps1 -Port 8000 -VenvPath "./venv/Scripts/Activate.ps1"`
- WSL/macOS/Linux: `./scripts/dev_start_simple.sh 8000 .venv/bin/activate`
- Manual: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`

### Sync data
- Current season: `python sync.py`
- Specific season: `python sync.py --season <n>`
- Specific championship: `python sync.py --championship-id <cid>`
- Specific match: `python sync.py --match-id <match>`
- All seasons: `python sync.py --all-seasons`
- Refresh divisions first: `--refresh-divisions [--refresh-min-season N --refresh-dry-run --refresh-allow-empty]`
- Concurrency: `--max-concurrency` (fetch) and `--max-db-concurrency` (writers)

### Diagnostics and DB utilities
- Sync logs: `logs/` (rotation enabled)
- Runtime diagnostics: `SYNC_DIAGNOSTICS` → `logs/runtime_diagnostics.jsonl`
- Quick counts: `scripts/db_diag.py --season N`
- DB utilities: `python tools/check_db_connection.py` | `python tools/apply_schema.py` | `python tools/recompute_totals.py --championship-id <cid>`

## Backend Conventions
- Async-only; wrap DB work with `connection()` or `readonly_connection()` in `db_async.py`.
- Use `db_async.py` bulk helpers (`upsert_*`, `replace_map_votes_async`, `clear_obsolete_maps_async`); guard custom writes with `_retry_on_deadlock(..., max_attempts=3)`.
- Keep routers thin; business logic in `api/services/*`.
- Cache expensive aggregates with `AsyncTTLCache` when reused.
- Faceit access: `faceit_client_async.py` enforces 10k/hr + adaptive backoff (max 8 concurrent HTTP requests); avoid bypassing it.
- Pool sizing: asyncmy defaults min 2 / max 30; sync workers expect `DB_CONNECTIONS_PER_WORKER` (default 3) and `MAX_DB_WRITER_CONCURRENCY` (default 6); `DB_POOL_DEBUG` enables pool tracing.
- Standings: use `standings_utils.calculate_standings()` (official tiebreakers). `_calculate_h2h_stats()` in `teams_service.py` computes head-to-head stats.

## Data & Schema Notes
- Championships include `is_playoffs`, `slug`, `parent_championship_id`, `winner_team_id`. Playoffs auto-link to regular-season parents by slug match in `sync_pipeline.py`.
- `division_registry.py` discovers championships and writes `divisions.json`; `faceit_config` exposes `DIVISIONS` and `CURRENT_SEASON`.
- `division_overrides.json` marks banned/quit teams; matches they touch set `ignored_due_ban=1` and are excluded from stats.
- Match normalization in `sync_pipeline.py` merges Faceit `rounds`/`detailed_results`, detects forfeits, stores map votes, and writes map/team/player totals; pending matches are re-checked about every 15 minutes.
- Schema: computed columns in `player_stats`/`team_stats` derive metrics; `maps_catalog` stores display metadata; `maps` rows flag `is_forfeit` per map.

### Totals tables (performance-critical)
- Regular season (`is_playoffs=0`): use `player_season_totals`, `team_season_totals`, `player_map_season_totals`, `team_map_season_totals` for summary/leaderboard/table data.
- Playoffs (`is_playoffs=1`): totals tables are not reliable; use `matches`, `maps`, `player_stats`, `team_stats` scoped by `championship_id`.
- Progression charts: use `*_prev` snapshot tables ordered by `snapshot_ts`.

### Snapshot policy
- On finished match for a division: create a `division_snapshots` row (with `match_id`) and insert snapshot rows into `*_prev` tables for affected teams/players/maps.

## Frontend (Vue SPA)
- Routes (history mode): `/`, `/seasons`, `/division/:championshipId`, `/division/:championshipId/playoffs`, `/team/:teamId`, `/team/:championshipId/:teamId`, `/player/:playerId`.
- Data flow: Pinia stores in `frontend/static/stores/*` (notably `useTeamStore`, `useSeasonsStore`, `useDivisionStore`).
- `TeamDetail` component in `components/TeamDetail.js` uses `apiClient.getTeamPage(teamId, championshipId)` and mirrors the `championship` query for sharable links.
- Season selector defaults to latest; use championship names to differentiate playoffs vs regular.
- API base resolves at runtime (`window.PL_API_URL` or `window.__API_BASE__` or origin + `/api`).

### SPA navigation & caching guardrails (important)
- Never `return` early from route watchers right after route normalization (`router.replace`) if data fetch should still run.
- When route params change quickly, guard async loaders with request tokens so stale responses cannot overwrite the latest view state.
- Prefer remount safety for detail pages on param changes:
	- app-level `router-view` keyed by route name + params (`frontend/static/app-main.js`)
	- local `:key` on detail wrappers when needed (e.g. `TeamDetailView`).
- In-flight keys must include route context (team/player + championship) so SPA transitions do not reuse wrong pending work.
- Team store fetches should await active in-flight promise before deciding freshness; avoid returning stale `entry.page.data` while another championship is loading.
- Demo links:
	- probe results should be strict (do not treat 429 as “exists”)
	- avoid aggressive duplicate probe retries that spike rate limits
	- 2D replay links should open in a new tab without redirecting the current tab.
- Hosted Linux/proxy deployments: persistent browser cache must be build-versioned (`PL_BUILD_ID`), and static responses should use no-store headers to avoid stale JS after deploy.

## Common Pitfalls
1) Exceeding DB pool budget when raising `--max-concurrency`/`--max-db-concurrency` without bumping `DB_POOL_MAX_SIZE`.
2) Skipping `_retry_on_deadlock` or bypassing `db_async.py` helpers for writes.
3) Losing playoff linkage or `winner_team_id`; use nulls instead of bad IDs to satisfy FK constraints.
4) Forgetting banned-team handling—set `ignored_due_ban` so stats exclude those matches.
5) Bypassing `faceit_client_async.py` (ignores limiter) or running sync without diagnostics (`SYNC_DIAGNOSTICS`).
6) Breaking SPA deep links—keep backend fallback in place and include `championship` query on team links inside a division.
7) SPA stale-state regressions after internal navigation: route watcher early-returns, non-contextual in-flight keys, or missing request-token guards can make views require F5.
8) Hosted env mismatch vs local: stale localStorage/API cache or proxy/static caching can hide fresh frontend fixes until hard refresh.

## Key Files
- Data sync: `sync.py`, `sync_pipeline.py`, `division_registry.py`, `faceit_client_async.py`, `division_overrides.json`.
- DB layer: `db_async.py`, `mariadb_schema.sql`.
- Standings: `standings_utils.py` (official tiebreaker logic with h2h support).
- API: `api/main.py`, routers `api/routers/{seasons,season_view,divisions,championships,teams,players,matches,stats,maps_catalog,image_proxy}.py`, services `api/services/{seasons_service,season_view_service,season_aggregates,stats_service,teams_service,players_service,matches_service,divisions_service,player_counts}.py`.
- Frontend: `frontend/static/app-main.js`, `frontend/static/api-client.js`, views `frontend/static/views/{HomeView.js,SeasonsView.js,DivisionView.js,TeamDetailView.js,PlayerView.js}`, stores `frontend/static/stores/{useHomeStore,useSeasonsStore,useDivisionStore,useTeamStore,usePlayerStore}.js`, components under `frontend/static/components/`.

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
