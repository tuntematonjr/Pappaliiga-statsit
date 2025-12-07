# Pappaliiga Stats – AI Agent Playbook

## Stack at a Glance
- Data sync: `sync.py`, `sync_pipeline.py` → Faceit API → MariaDB with adaptive rate limiting.
- API: FastAPI (`api/`) with service layer, async DB ops, TTL caching.
- Frontend: Vue 3 SPA (`frontend/static`), Pinia stores, Vue Router (history mode), custom components (no build step).

## Runbook
- Dev start (backend + SPA server): `./scripts/dev_start_simple.ps1` (Windows) or `./scripts/dev_start_simple.sh` (WSL/macOS/Linux)
- Manual dev: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000` and `python frontend/spa_server.py 8080`
- Sync data: `python sync.py` (all) | `python sync.py --season <n>` | `python sync.py --match <match_id>` | `python sync.py --reset`
- DB utilities: `python tools/check_db_connection.py` | `python tools/apply_schema.py` | `python tools/recompute_totals.py`

## Backend Conventions
- Async everywhere; use `connection()`, `readonly_connection()`, `transaction()` from `db_async.py`.
- Deadlock safety: wrap writes with `_retry_on_deadlock(..., max_attempts=3)` in `db_ops_async.py`.
- Rate limits: Faceit 10k/hr, adaptive limiter in `faceit_client_async.py`; backs off on 429.
- Service layer only talks to DB helpers; routers stay thin.
- Caching: `AsyncTTLCache` (30s, 128 entries) for heavy aggregations (e.g., `stats_service.get_season_stats`).

## Data & Schema Notes
- Championships table has `is_playoffs` and `name`; use it to distinguish regular vs playoffs.
- Championships must exist before matches; keep FK order in migrations and sync.
- Division overrides: `division_overrides.json` marks banned/quit teams; matches with banned teams set `ignored_due_ban=1` and are excluded from stats.
- Match normalization lives in `sync_pipeline.py::_build_normalised_match` (handles rounds vs detailed_results, forfeits, veto vote quirks).
- Diagnostics: `runtime_diagnostics.py` writes JSONL snapshots to `logs/runtime_diagnostics.jsonl`; enable pool debug via `DB_POOL_DEBUG=1`.

## Frontend Guidance (Vue SPA)
- Routing (history mode):
  - Team base: `/team/:teamId`
  - Team with division context: `/team/:championshipId/:teamId`
  - Division: `/division/:championshipId` (and `/division/:championshipId/playoffs`)
- Team page state: `TeamDetail` uses Pinia `useTeamStore` and `apiClient.getTeamPage(teamId, championshipId)`; `championship` query param is mirrored to keep links shareable.
- Season selector: dropdown defaults to newest season; labels use championship name to differentiate regular vs playoffs.
- When adding links to a team within a division, include `query: { championship: <championshipId> }` so copy-paste preserves the division.
- API base URL injected at runtime via `window.__API_BASE__`; fallback heuristic in `api-client.js` for localhost.

## Common Pitfalls
1) Blocking the event loop—never run sync DB/HTTP work without `await` or proper tasks.
2) Missing deadlock retries on writes—use `_retry_on_deadlock` wrappers.
3) Forgetting playoffs flag/name—championship names differentiate PO vs regular; surface them in UI.
4) SPA deep links 404—ensure backend serves `index.html` for unknown paths (handled in `api/main.py`).
5) Over-parallel Faceit calls—respect the adaptive limiter; avoid naive loops.

## Key Files
- `sync_pipeline.py`, `db_async.py`, `db_ops_async.py`, `faceit_client_async.py`
- `api/main.py`, `api/services/*`, `api/routers/teams.py`
- `frontend/static/components/TeamDetail.js`, `frontend/static/api-client.js`, `frontend/static/views/DivisionView.js`

## Environment
Required `.env` in repo root:
```
FACEIT_API_KEY=your_faceit_api_key_here
DATABASE_URL=mariadb://user:pass@host:3306/pappaliiga
```
Optional:
```
DB_POOL_MAX_SIZE=30
MAX_DB_WRITER_CONCURRENCY=6
SYNC_DIAGNOSTICS=1
```
