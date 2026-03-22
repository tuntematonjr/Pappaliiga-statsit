from __future__ import annotations

import asyncio
import logging
import os

import faceit_config
from api.services import season_view_service, seasons_service, stats_service

logger = logging.getLogger("pappaliiga.cache.reheat")


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _bool_env(name: str, default: bool = True) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


REHEAT_ENABLED = _bool_env("PL_CACHE_REHEAT", True)
REHEAT_TIMEOUT_SECONDS = _int_env("PL_CACHE_REHEAT_TIMEOUT", 30)


async def reheat_main_page() -> None:
    if not REHEAT_ENABLED:
        logger.info("Cache reheat disabled via PL_CACHE_REHEAT")
        return

    season = faceit_config.CURRENT_SEASON
    logger.info("Starting cache reheat for season %s", season)

    async def _run() -> None:
        await stats_service.get_overview_stats()
        await stats_service.get_stats_summary("all")
        await stats_service.get_stats_summary("season", season=season)
        await stats_service.get_season_stats(season)
        await seasons_service.get_season_summary(season)
        await seasons_service.get_season_divisions(season)
        await season_view_service.get_season_summary(season)
        await season_view_service.get_divisions(season)

    try:
        await asyncio.wait_for(_run(), timeout=REHEAT_TIMEOUT_SECONDS)
        logger.info("Cache reheat complete")
    except asyncio.TimeoutError:
        logger.warning("Cache reheat timed out after %ss", REHEAT_TIMEOUT_SECONDS)
    except Exception as exc:
        logger.warning("Cache reheat failed: %s", exc)
