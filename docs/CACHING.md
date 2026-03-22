# In-Process Caching

Quick reference for the current API cache setup.

## Implementation
- Core config: `api/services/cache_helpers.py`
- Startup reheat: `api/services/cache_reheat.py`
- Cache type: async TTL + LRU + per-key single-flight (`AsyncTTLCache`)

## Tiers
- Active season: `ACTIVE_CACHE`
- Recent seasons: `RECENT_CACHE`
- Older seasons: `OLD_CACHE`
- Global/all-season queries: `GLOBAL_CACHE`
- Very old seasons are skipped (`PL_CACHE_SKIP_OLD_AFTER`)

Default TTL is 24h for active/recent/old/global. Revision cache TTL is 10s.

## Invalidation
- Keys include revision tokens derived from `MAX(updated_at)`.
- Revisions exist for season, championship, and global scopes.
- Any DB update that changes a relevant `updated_at` value naturally rotates cache keys.
- TTL expiry still applies.

## Key Env Vars
- `PL_CACHE_ACTIVE_TTL`
- `PL_CACHE_RECENT_TTL`
- `PL_CACHE_OLD_TTL`
- `PL_CACHE_GLOBAL_TTL`
- `PL_CACHE_REVISION_TTL`
- `PL_CACHE_RECENT_WINDOW`
- `PL_CACHE_SKIP_OLD_AFTER`
- `PL_CACHE_ACTIVE_MAXSIZE`
- `PL_CACHE_RECENT_MAXSIZE`
- `PL_CACHE_OLD_MAXSIZE`
- `PL_CACHE_GLOBAL_MAXSIZE`
- `PL_CACHE_REVISION_MAXSIZE`

## Reheat
On startup, `reheat_main_page()` preloads high-traffic endpoints for the current season.

Controls:
- `PL_CACHE_REHEAT` (default: true)
- `PL_CACHE_REHEAT_TIMEOUT` (default: 30 seconds)

## Notes
- Cache is process-local and resets on restart.
- Designed for single-process behavior (no distributed invalidation).
