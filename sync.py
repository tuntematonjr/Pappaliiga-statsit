

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from typing import Sequence

from db_async import create_schema_async, fetch_val, reset_db_async, connection
from division_overrides import load_division_overrides
from faceit_client_async import get_rate_limit_stats, reset_rate_limit_stats, shutdown_clients
from faceit_config import DIVISIONS, CURRENT_SEASON
from sync_pipeline import ChampionshipSyncResult, sync_championship_async, update_single_match_async
from db_ops_async import upsert_championship_async

LOGGER = logging.getLogger("pappaliiga.sync")

def _format_hms(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds:.2f}s"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h:02}:{m:02}:{s:02}"
    else:
        return f"{m:02}:{s:02}"

def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pappaliiga Faceit sync (async MariaDB pipeline)")
    parser.add_argument("--create-schema", action="store_true", help="Create database schema if missing")
    parser.add_argument("--reset-db", action="store_true", help="Drop and recreate schema (dev only)")
    parser.add_argument("--force-reset", action="store_true", help="Confirm destructive --reset-db")
    parser.add_argument("--championship-id", default=None, help="Sync only the provided championship")
    parser.add_argument("--match-id", default=None, help="Resync a single match")
    parser.add_argument("--full", action="store_true", help="Force full resync for the selected championship")
    parser.add_argument("--all-seasons", action="store_true", help="Sync all seasons (default: current season only)")
    parser.add_argument("--verify", action="store_true", help="Run post-sync verification queries")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=10,
        help="Maximum number of divisions to sync concurrently (default: 10)",
    )
    return parser


async def _verify_counts() -> None:
    tables = [
        "matches",
        "maps",
        "player_stats",
        "team_stats",
        "player_season_totals",
        "team_season_totals",
    ]
    for table in tables:
        count = await fetch_val(f"SELECT COUNT(*) AS c FROM {table}", default=0)
        LOGGER.info("%s rows: %s", table, count)

    inconsistent_forfeits = await fetch_val(
        """
        SELECT COUNT(*)
        FROM matches m
        WHERE m.is_forfeit = 1
          AND EXISTS (
            SELECT 1 FROM maps mp
            WHERE mp.match_id = m.match_id AND mp.is_forfeit = 0
          )
        """,
        default=0,
    )
    if inconsistent_forfeits:
        LOGGER.warning("%s forfeited matches contain non-forfeit maps", inconsistent_forfeits)

    ignored_matches = await fetch_val(
        "SELECT COUNT(*) FROM matches WHERE ignored_due_ban = 1",
        default=0,
    )
    if ignored_matches:
        LOGGER.info("%s matches flagged ignored_due_ban", ignored_matches)


def _resolve_targets(championship_id: str | None, all_seasons: bool = False) -> Sequence[str]:
    if championship_id:
        return [championship_id]
    
    if all_seasons:
        # Return all championships from all seasons
        LOGGER.info("All seasons requested - syncing all divisions from all seasons (%d total)", len(DIVISIONS))
        return [item["championship_id"] for item in DIVISIONS]
    
    # Default to current season only
    current_season_divisions = [
        item["championship_id"] for item in DIVISIONS 
        if item.get("season") == CURRENT_SEASON
    ]
    
    LOGGER.info("No specific championship provided - syncing all Season %d divisions (%d total)", 
                CURRENT_SEASON, len(current_season_divisions))
    return current_season_divisions


async def main_async(args: argparse.Namespace) -> int:
    if args.force_reset and not args.reset_db:
        LOGGER.error("--force-reset must be used together with --reset-db")
        return 2

    await reset_rate_limit_stats()

    if args.create_schema:
        await create_schema_async(force=True)
    else:
        await create_schema_async(force=False)

    if args.reset_db:
        if not args.force_reset:
            LOGGER.error("--reset-db requires --force-reset to avoid accidental data loss")
            return 2
        await reset_db_async(confirm=True)
        await create_schema_async(force=False)
        # Do not proceed to syncing when performing a reset. Optionally run verify, then exit.
        if args.verify:
            await _verify_counts()
        await shutdown_clients()
        return 0

    overrides = load_division_overrides()
    max_concurrency = max(1, args.max_concurrency)

    if args.match_id:
        LOGGER.info("Refreshing single match %s", args.match_id)
        try:
            championship_id = await update_single_match_async(args.match_id)
            if championship_id:
                LOGGER.info("Match %s refreshed (championship %s)", args.match_id, championship_id)
        except Exception as exc:
            LOGGER.exception("Failed to refresh match %s: %s", args.match_id, exc)
            return 1
    else:
        targets = _resolve_targets(args.championship_id, args.all_seasons)
        total_start_time = time.time()
        total_synced_matches = 0
        total_skipped_matches = 0
        
        # Group championships by season for better organization
        championships_by_season = {}
        for championship_id in targets:
            division = next((d for d in DIVISIONS if d["championship_id"] == championship_id), None)
            if division:
                season = division["season"]
                if season not in championships_by_season:
                    championships_by_season[season] = []
                championships_by_season[season].append((championship_id, division))
        
        LOGGER.info("Starting sync for %d championship(s) across %d season(s)", 
                   len(targets), len(championships_by_season))

        # Ensure all championships exist in DB before syncing matches. This avoids
        # foreign-key failures when match rows reference championships that haven't
        # been upserted yet. We use a single DB connection and upsert sequentially.
        try:
            LOGGER.info("Upserting %d championship rows into DB", len(targets))
            async with connection() as conn:
                for season_key in sorted(championships_by_season.keys()):
                    for championship_id, division in championships_by_season[season_key]:
                        row = {
                            "championship_id": championship_id,
                            "season": division.get("season"),
                            "division_num": division.get("division_num"),
                            "name": division.get("name") or division.get("slug") or f"div{division.get('division_num')}-s{division.get('season')}",
                            "is_playoffs": 1 if division.get("is_playoffs") else 0,
                            "slug": division.get("slug") or f"div{division.get('division_num')}-s{division.get('season')}",
                        }
                        try:
                            await upsert_championship_async(conn, row)
                        except Exception:
                            # If upsert fails for a single championship, log and continue so other
                            # divisions can still be processed. The failure will be obvious in logs.
                            LOGGER.exception("Failed to upsert championship %s before sync", championship_id)
        except Exception:
            LOGGER.exception("Unexpected error while upserting championships - continuing with sync")
        
        # Process each season
        for season in sorted(championships_by_season.keys()):
            season_start_time = time.time()
            season_synced_matches = 0
            season_skipped_matches = 0
            season_championships = championships_by_season[season]
            
            LOGGER.info("=== SEASON %d SYNC START ===", season)
            LOGGER.info("Processing %d divisions for Season %d", len(season_championships), season)
            
            sem = asyncio.Semaphore(max_concurrency)
            async def sync_division(
                championship_id: str,
                division: dict,
            ) -> ChampionshipSyncResult | None:
                async with sem:
                    division_name = division.get("name", f"Division {division.get('division_num', '?')}")
                    LOGGER.info("Syncing championship %s (%s)", championship_id, division_name)
                    try:
                        result = await sync_championship_async(
                            championship_id,
                            full=args.full,
                            overrides=overrides,
                        )
                        LOGGER.info(
                            "Synced %s matches for %s (skipped %s)",
                            len(result.synced_match_ids),
                            division_name,
                            result.skipped_matches,
                        )
                        return result
                    except Exception as exc:
                        LOGGER.exception("Championship sync failed for %s: %s", championship_id, exc)
                        return None

            season_results = await asyncio.gather(
                *(sync_division(championship_id, division) for championship_id, division in season_championships)
            )

            processed_championships = 0
            for result in season_results:
                if not result:
                    continue
                processed_championships += 1
                season_synced_matches += len(result.synced_match_ids)
                season_skipped_matches += result.skipped_matches
                total_synced_matches += len(result.synced_match_ids)
                total_skipped_matches += result.skipped_matches
            
            season_elapsed = time.time() - season_start_time
            LOGGER.info("=== SEASON %d SYNC COMPLETED ===", season)
            LOGGER.info(
                "Season %d: %d divisions, %d synced matches, %d skipped matches in %s",
                season,
                len(season_championships),
                season_synced_matches,
                season_skipped_matches,
                _format_hms(season_elapsed),
            )
            if processed_championships:
                LOGGER.info(
                    "Season %d averages: %s/division, %s/synced match",
                    season,
                    _format_hms(season_elapsed / processed_championships),
                    _format_hms(season_elapsed / max(season_synced_matches or 1, 1)),
                )
            LOGGER.info("")
        
        total_elapsed = time.time() - total_start_time
        if len(targets) > 1:
            LOGGER.info("=== FULL SYNC COMPLETED ===")
            LOGGER.info(
                "Total: %d seasons, %d championships, %d synced matches (%d skipped) in %s",
                len(championships_by_season),
                len(targets),
                total_synced_matches,
                total_skipped_matches,
                _format_hms(total_elapsed),
            )
            LOGGER.info(
                "Overall averages: %s/championship, %s/synced match",
                _format_hms(total_elapsed / max(len(targets), 1)),
                _format_hms(total_elapsed / max(total_synced_matches or 1, 1)),
            )

    rate_stats = await get_rate_limit_stats()
    throttle_hits = int(rate_stats.get("throttle_hits", 0))
    throttle_wait = float(rate_stats.get("throttle_wait_seconds", 0.0))
    hourly_events = int(rate_stats.get("hourly_wait_events", 0))
    hourly_wait = float(rate_stats.get("hourly_wait_seconds", 0.0))
    total_requests = int(rate_stats.get("request_count_total", 0))
    avg_per_minute = float(rate_stats.get("average_requests_per_minute", 0.0))

    if total_requests:
        LOGGER.info(
            "Faceit API requests this run: %d (avg %.1f/min)",
            total_requests,
            avg_per_minute,
        )
    if throttle_hits:
        LOGGER.info(
            "Faceit rate limits encountered %d time(s); cumulative enforced wait %s",
            throttle_hits,
            _format_hms(throttle_wait),
        )
    else:
        LOGGER.info("Faceit rate limits were not encountered during this run")

    if hourly_events:
        LOGGER.info(
            "Hourly cap enforced %d time(s); cumulative scheduled wait %s",
            hourly_events,
            _format_hms(hourly_wait),
        )

    if args.verify:
        await _verify_counts()

    await shutdown_clients()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
