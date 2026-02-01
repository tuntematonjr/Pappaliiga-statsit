# In-process caching

## Goals
- Cache expensive computations in RAM.
- Invalidate on DB changes via revision checks and TTL expiry.
- Avoid caching rarely read old seasons.
- Prevent cache stampede with single-flight.

## Summary of approach
The API uses an in-process async cache with TTL + LRU eviction and per-key single-flight protection. Cache tiers are split by season recency: current season (active), recent seasons, and old seasons. Old seasons beyond a configurable threshold are not cached to avoid wasting RAM.

## Cache tiers and TTLs
Defined in api/services/cache_helpers.py and configurable via environment variables.

Default tier behavior:
- Current season (active): 24-hour TTL, larger cache.
- Recent seasons: 24-hour TTL.
- Old seasons: 24-hour TTL, smaller cache.
- Very old seasons: no caching.
- Global (all-season) aggregates: 24-hour TTL with global revision check.

Environment variables:
- PL_CACHE_ACTIVE_TTL (seconds, default 86400 / 24 hours)
- PL_CACHE_RECENT_TTL (seconds, default 86400 / 24 hours)
- PL_CACHE_OLD_TTL (seconds, default 86400 / 24 hours)
- PL_CACHE_GLOBAL_TTL (seconds, default 86400 / 24 hours)
- PL_CACHE_REVISION_TTL (seconds, default 10)
- PL_CACHE_RECENT_WINDOW (seasons, default 2)
- PL_CACHE_SKIP_OLD_AFTER (seasons, default 5)
- PL_CACHE_ACTIVE_MAXSIZE (default 512)
- PL_CACHE_RECENT_MAXSIZE (default 384)
- PL_CACHE_OLD_MAXSIZE (default 128)
- PL_CACHE_GLOBAL_MAXSIZE (default 128)
- PL_CACHE_REVISION_MAXSIZE (default 256)

## Cache key strategy
Keys include a stable operation name plus parameters and a revision token:
- season summaries: ("season-summary", season, season_revision)
- season divisions: ("season-divisions", season, season_revision)
- season stats: ("season-stats", season, season_revision)
- stats summary: ("stats-summary", "season", season, season_revision)
- lifetime/global: ("overview-stats", global_revision)
- playoff brackets: ("playoff-bracket", championship_id, champ_revision)

## Invalidation strategy
No external cache service is used. Invalidation is handled by revision tokens that are derived from MAX(updated_at) across relevant tables:
- Season revision: matches, maps, team/player totals, map totals, championships for that season.
- Championship revision: matches and maps scoped to a championship.
- Global revision: matches, maps, totals, teams, players, championships.

When any of these tables update, the revision changes and the cache key changes, naturally invalidating old entries. TTL expiry also clears old entries.

## Concurrency protection
AsyncTTLCache enforces single-flight per key. The first request computes and other concurrent requests await the same future.

## Cache reheating
On startup, the API warms caches for the main page and current season data. This precomputes:
- Stats overview and summary (all + current season)
- Current season stats, summary, and divisions
- Season overview (SPA) summary + divisions

Controls:
- PL_CACHE_REHEAT (default true)
- PL_CACHE_REHEAT_TIMEOUT (seconds, default 30)

## Where it is used
- api/services/season_aggregates.py
- api/services/stats_service.py
- api/services/seasons_service.py
- api/services/season_view_service.py
- api/services/matches_service.py

## Notes
- Cache is in-memory only and is lost on restart.
- This is a single-process design and does not assume multiple instances.
- Old seasons beyond PL_CACHE_SKIP_OLD_AFTER are not cached by default.
