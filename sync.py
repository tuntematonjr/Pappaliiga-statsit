

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys
import time
from typing import Any, Sequence

from db_async import create_schema_async, fetch_val, reset_db_async, connection, upsert_championships_async
from division_overrides import load_division_overrides
from division_naming import build_division_name
from faceit_client_async import get_rate_limit_stats, reset_rate_limit_stats, shutdown_clients
import faceit_config
from sync_pipeline import ChampionshipSyncResult, sync_championship_async, update_single_match_async
from utils import format_hms, log_stage
from division_registry import refresh_divisions
from runtime_diagnostics import SyncDiagnostics

LOGGER = logging.getLogger("pappaliiga.sync")
LOG_DIR = Path(os.environ.get("SYNC_LOG_DIR", Path(__file__).with_name("logs")))
DEFAULT_LOG_MAX_FILES = 10


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    if getattr(_configure_logging, "_configured", False):
        root.setLevel(level)
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    log_path = LOG_DIR / f"sync-{timestamp}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    max_files = int(os.environ.get("SYNC_LOG_MAX_FILES", DEFAULT_LOG_MAX_FILES))
    # Use a plain file handler so we don't split the log file by size.
    from logging import FileHandler
    file_handler = FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)
    # Ensure pruning runs after the new log file exists so the new file counts
    try:
        if max_files > 0:
            # ensure file exists (FileHandler opened in append, but touch to be safe)
            try:
                log_path.touch(exist_ok=True)
            except Exception:
                pass
            logs = sorted(LOG_DIR.glob("sync-*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in logs[max_files:]:
                try:
                    old.unlink()
                except Exception:
                    logging.getLogger("pappaliiga.sync").warning("Failed to remove old log file %s", old)
    except Exception:
        logging.getLogger("pappaliiga.sync").exception("Log pruning failed")
    _configure_logging._configured = True


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pappaliiga Faceit sync (async MariaDB pipeline)")
    parser.add_argument("--create-schema", action="store_true", help="Create database schema if missing")
    parser.add_argument("--reset-db", action="store_true", help="Drop and recreate schema (dev only)")
    parser.add_argument("--force-reset", action="store_true", help="Confirm destructive --reset-db")
    parser.add_argument("--championship-id", default=None, help="Sync only the provided championship")
    parser.add_argument("--match-id", default=None, help="Resync a single match")
    parser.add_argument("--full", action="store_true", help="Force full resync for the selected championship")
    parser.add_argument("--all-seasons", action="store_true", help="Sync all seasons (default: current season only)")
    parser.add_argument("--season", type=int, default=None, help="Sync only the provided season number (overrides --all-seasons)")
    parser.add_argument("--verify", action="store_true", help="Run post-sync verification queries")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--end-on-error", action="store_true", help="Stop immediately if a division sync fails")
    parser.add_argument("--refresh-divisions", action="store_true", help="Refresh divisions.json from Faceit before syncing")
    parser.add_argument(
        "--refresh-min-season",
        type=int,
        default=faceit_config.DEFAULT_CURRENT_SEASON,
        help=f"Minimum season to include when refreshing divisions (default: {faceit_config.DEFAULT_CURRENT_SEASON})",
    )
    parser.add_argument("--refresh-dry-run", action="store_true", help="Run the division refresh without writing to disk")
    parser.add_argument("--refresh-allow-empty", action="store_true", help="Allow new divisions with no registered teams")
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=10,
        help="Maximum number of divisions to sync concurrently (default: 10)",
    )
    parser.add_argument(
        "--max-db-concurrency",
        type=int,
        default=getattr(faceit_config, "MAX_DB_WRITER_CONCURRENCY", 3),
        help="Maximum number of concurrent DB writer tasks (default: MAX_DB_WRITER_CONCURRENCY)",
    )
    return parser


def _is_refresh_only(args: argparse.Namespace) -> bool:
    """Return True when caller only requested division refresh."""
    if not args.refresh_divisions:
        return False
    has_sync_target = any(
        [
            args.match_id,
            args.championship_id,
            args.all_seasons,
            args.season is not None,
        ]
    )
    has_setup_task = any([args.create_schema, args.reset_db, args.verify])
    return not has_sync_target and not has_setup_task


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


def _resolve_targets(championship_id: str | None, all_seasons: bool = False, season: int | None = None) -> Sequence[str]:
    if championship_id:
        return [championship_id]

    # If explicit season provided, use that (overrides all_seasons)
    if season is not None:
        season_divisions = [item["championship_id"] for item in faceit_config.DIVISIONS if item.get("season") == season]
        LOGGER.info("Syncing Season %s divisions (%d total)", season, len(season_divisions))
        return season_divisions

    if all_seasons:
        # Return all championships from all seasons
        LOGGER.info("All seasons requested - syncing all divisions from all seasons (%d total)", len(faceit_config.DIVISIONS))
        return [item["championship_id"] for item in faceit_config.DIVISIONS]

    # Default to current season only
    current_season_divisions = [
        item["championship_id"] for item in faceit_config.DIVISIONS 
        if item.get("season") == faceit_config.CURRENT_SEASON
    ]

    LOGGER.info("No specific championship provided - syncing all Season %d divisions (%d total)", 
                faceit_config.CURRENT_SEASON, len(current_season_divisions))
    return current_season_divisions


async def main_async(args: argparse.Namespace) -> int:
    diagnostics = SyncDiagnostics()
    await diagnostics.start()
    try:
        return await _main_async_impl(args, diagnostics)
    finally:
        await diagnostics.stop()


async def _main_async_impl(args: argparse.Namespace, diagnostics: SyncDiagnostics) -> int:
    if args.force_reset and not args.reset_db:
        LOGGER.error("--force-reset must be used together with --reset-db")
        return 2

    max_db_concurrency = max(1, args.max_db_concurrency)
    db_semaphore = asyncio.Semaphore(max_db_concurrency)
    expected_conn_per_worker = max(1, getattr(faceit_config, "DB_CONNECTIONS_PER_WORKER", 3))
    concurrency_budget = max(max_db_concurrency, max(1, args.max_concurrency))
    recommended_pool = concurrency_budget * expected_conn_per_worker
    configured_pool_max = getattr(faceit_config, "DB_POOL_MAX_SIZE", recommended_pool)
    if configured_pool_max < recommended_pool:
        LOGGER.warning(
            "Configured DB pool max (%d) is lower than recommended (%d) for concurrency (workers=%d, conn/worker=%d). "
            "Consider lowering --max-db-concurrency or increasing DB_POOL_MAX_SIZE.",
            configured_pool_max,
            recommended_pool,
            concurrency_budget,
            expected_conn_per_worker,
        )

    if args.reset_db:
        if not args.force_reset:
            LOGGER.error("--reset-db requires --force-reset to avoid accidental data loss")
            return 2
        LOGGER.warning("=" * 70)
        LOGGER.warning("DROPPING ALL TABLES AND RECREATING SCHEMA")
        LOGGER.warning("This will DELETE ALL DATA in the database")
        LOGGER.warning("=" * 70)
        await reset_db_async(confirm=True)
        LOGGER.info("All tables dropped successfully")
        await create_schema_async(force=True)
        LOGGER.info("Schema recreated from %s", Path(__file__).with_name("mariadb_schema.sql"))
        # Do not proceed to syncing when performing a reset. Optionally run verify, then exit.
        if args.verify:
            await _verify_counts()
        LOGGER.info("Database reset complete. Run sync.py to populate data.")
        await shutdown_clients()
        return 0

    await reset_rate_limit_stats()

    if args.refresh_divisions:
        min_season = args.refresh_min_season if args.refresh_min_season is not None else faceit_config.DEFAULT_CURRENT_SEASON
        min_new_division_teams = 0 if args.refresh_allow_empty else 1
        try:
            refresh_result = await refresh_divisions(
                min_season=min_season,
                min_new_division_teams=min_new_division_teams,
                dry_run=args.refresh_dry_run,
            )
        except Exception as exc:
            LOGGER.exception("Failed to refresh divisions: %s", exc)
            return 1

        LOGGER.info(
            "Division refresh complete: total=%d, new=%d, changed=%s",
            refresh_result.total,
            refresh_result.created,
            refresh_result.changed,
        )
        if refresh_result.new_championship_ids:
            LOGGER.info(
                "New championships: %s",
                ", ".join(refresh_result.new_championship_ids),
            )
        if refresh_result.skipped_championship_ids:
            LOGGER.info(
                "Skipped championships lacking teams: %s",
                ", ".join(refresh_result.skipped_championship_ids),
            )
        if args.refresh_dry_run:
            LOGGER.info("Dry run requested - divisions.json left unchanged.")
        else:
            LOGGER.info("Updated divisions written to %s", refresh_result.output_path)

        if _is_refresh_only(args):
            LOGGER.info("Refresh-only invocation detected; skipping sync pipeline.")
            await shutdown_clients()
            return 0

    if args.create_schema:
        await create_schema_async(force=True)
    else:
        await create_schema_async(force=False)

    overrides = load_division_overrides()
    max_concurrency = max(1, args.max_concurrency)
    end_on_error = bool(getattr(args, "end_on_error", False))

    if args.match_id:
        LOGGER.info("Refreshing single match %s", args.match_id)
        try:
            championship_id = await update_single_match_async(args.match_id, diagnostics=diagnostics)
            if championship_id:
                LOGGER.info("Match %s refreshed (championship %s)", args.match_id, championship_id)
        except Exception as exc:
            LOGGER.exception("Failed to refresh match %s: %s", args.match_id, exc)
            return 1
    else:
        targets = _resolve_targets(args.championship_id, args.all_seasons, season=args.season)
        total_start_time = time.perf_counter()
        total_synced_matches = 0
        total_skipped_matches = 0

        division_lookup = {str(item["championship_id"]): item for item in faceit_config.DIVISIONS}
        championships_by_season: dict[int, list[dict[str, Any]]] = {}
        championship_rows: list[dict[str, Any]] = []
        seen_championships: set[str] = set()

        for championship_id in targets:
            if championship_id in seen_championships:
                continue
            seen_championships.add(championship_id)
            division = division_lookup.get(championship_id)
            if not division:
                LOGGER.warning("Division metadata missing for championship %s - skipping", championship_id)
                continue
            season = division.get("season")
            if season is None:
                LOGGER.warning("Division %s missing season - skipping", championship_id)
                continue
            entry = {"championship_id": championship_id, "division": division}
            championships_by_season.setdefault(season, []).append(entry)
            division_num = division.get("division_num")
            is_playoffs = division.get("is_playoffs")
            fallback_slug = f"div{division_num}-s{season}"
            division_name = build_division_name(season, division_num, is_playoffs)
            championship_rows.append(
                {
                    "championship_id": championship_id,
                    "season": season,
                    "division_num": division_num,
                    "name": division_name,
                    "is_playoffs": 1 if is_playoffs else 0,
                    "slug": division.get("slug") or fallback_slug,
                }
            )

        LOGGER.info(
            "Starting sync for %d championship(s) across %d season(s)",
            len(championship_rows),
            len(championships_by_season),
        )

        if championship_rows:
            upsert_start = time.perf_counter()
            try:
                async with connection() as conn:
                    await upsert_championships_async(conn, championship_rows)
                upsert_elapsed = time.perf_counter() - upsert_start
                log_stage(
                    LOGGER,
                    "upsert_championships",
                    upsert_elapsed,
                    counts={"championships": len(championship_rows)},
                    prefix="bootstrap",
                )
            except Exception:
                LOGGER.exception("Unexpected error while upserting championships - continuing with sync")

        # Process each season
        abort_exc: Exception | None = None

        for season in sorted(championships_by_season.keys()):
            season_start_time = time.perf_counter()
            season_synced_matches = 0
            season_skipped_matches = 0
            season_championships = championships_by_season[season]

            LOGGER.info("=== SEASON %d SYNC START ===", season)
            LOGGER.info("Processing %d divisions for Season %d", len(season_championships), season)

            sem = asyncio.Semaphore(max_concurrency)

            async def sync_division(entry: dict[str, Any]) -> ChampionshipSyncResult | None:
                async with sem:
                    championship_id = entry["championship_id"]
                    division = entry["division"]
                    division_name = build_division_name(
                        division.get("season"),
                        division.get("division_num"),
                        division.get("is_playoffs"),
                    )
                    LOGGER.info("Syncing championship %s (%s)", championship_id, division_name)
                    try:
                        result = await sync_championship_async(
                            championship_id,
                            full=args.full,
                            overrides=overrides,
                            division=division,
                            end_on_error=end_on_error,
                            db_semaphore=db_semaphore,
                            diagnostics=diagnostics,
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
                        if end_on_error:
                            raise
                        return None

            try:
                season_results = await asyncio.gather(
                    *(sync_division(entry) for entry in season_championships)
                )
            except Exception as exc:
                if end_on_error:
                    abort_exc = exc
                    LOGGER.error(
                        "Aborting remaining syncs because --end-on-error was supplied (season %s)",
                        season,
                    )
                    break
                raise

            processed_championships = 0
            for result in season_results:
                if not result:
                    continue
                processed_championships += 1
                season_synced_matches += len(result.synced_match_ids)
                season_skipped_matches += result.skipped_matches
                total_synced_matches += len(result.synced_match_ids)
                total_skipped_matches += result.skipped_matches

            season_elapsed = time.perf_counter() - season_start_time
            LOGGER.info("=== SEASON %d SYNC COMPLETED ===", season)
            LOGGER.info(
                "Season %d: %d divisions, %d synced matches, %d skipped matches in %s",
                season,
                len(season_championships),
                season_synced_matches,
                season_skipped_matches,
                format_hms(season_elapsed),
            )
            if processed_championships:
                LOGGER.info(
                    "Season %d averages: %s/division, %s/synced match",
                    season,
                    format_hms(season_elapsed / processed_championships),
                    format_hms(season_elapsed / max(season_synced_matches or 1, 1)),
                )
            LOGGER.info("")

            if abort_exc:
                break

        total_elapsed = time.perf_counter() - total_start_time
        if len(targets) > 1:
            LOGGER.info("=== FULL SYNC COMPLETED ===")
            LOGGER.info(
                "Total: %d seasons, %d championships, %d synced matches (%d skipped) in %s",
                len(championships_by_season),
                len(targets),
                total_synced_matches,
                total_skipped_matches,
                format_hms(total_elapsed),
            )
            LOGGER.info(
                "Overall averages: %s/championship, %s/synced match",
                format_hms(total_elapsed / max(len(targets), 1)),
                format_hms(total_elapsed / max(total_synced_matches or 1, 1)),
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
            format_hms(throttle_wait),
        )
    else:
        LOGGER.info("Faceit rate limits were not encountered during this run")

    if hourly_events:
        LOGGER.info(
            "Hourly cap enforced %d time(s); cumulative scheduled wait %s",
            hourly_events,
            format_hms(hourly_wait),
        )

    if args.verify and not abort_exc:
        await _verify_counts()

    await shutdown_clients()
    if abort_exc:
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    defaults = {
        action.dest: action.default
        for action in parser._actions
        if action.option_strings and action.default is not argparse.SUPPRESS
    }
    provided_args = {
        key: value
        for key, value in vars(args).items()
        if key in defaults and value != defaults[key]
    }
    if provided_args:
        LOGGER.info("Invocation parameters: %s", provided_args)
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
