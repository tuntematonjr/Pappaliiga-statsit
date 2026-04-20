"""Service layer for season overview endpoints."""
from __future__ import annotations

from typing import Any, Dict, List

from api.services import seasons_service
from api.services.cache_helpers import get_season_revision, select_season_cache


def _as_int(value: Any, default: int = 0) -> int:
    """Best effort integer conversion to avoid Pydantic validation errors."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    """Best effort float conversion to avoid Pydantic validation errors."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def get_season_summary(season_id: int) -> Dict[str, Any]:
    """Return aggregated season statistics tailored for the SPA season overview."""
    cache, ttl_seconds = select_season_cache(season_id)
    if cache is not None:
        revision = await get_season_revision(season_id)
        cache_key = ("season-view-summary", season_id, revision)
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_season_summary(season_id),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_season_summary(season_id)


async def _compute_season_summary(season_id: int) -> Dict[str, Any]:
    summary = await seasons_service.get_season_summary(season_id)
    if not summary:
        return {}

    progress = summary.get("progress") or {}
    summary_totals = {
        "divisions": _as_int(summary.get("divisions")),
        "teams": _as_int(summary.get("teams")),
        "players": _as_int(summary.get("players")),
        "matches": _as_int(summary.get("matches")),
        "maps": _as_int(summary.get("maps")),
        "rounds": _as_int(summary.get("rounds")),
        "kills": _as_int(summary.get("kills")),
        "deaths": _as_int(summary.get("deaths")),
    }

    # Expose detailed progress for the frontend progress rings.
    progress_payload = {
        "divisions_finished": _as_int(
            progress.get("divisions_finished") or summary.get("divisions_finished")
        ),
        "divisions_total": _as_int(progress.get("divisions_total") or summary.get("divisions_total")),
        "regular_matches_played": _as_int(progress.get("regular_matches_played")),
        "regular_matches_total": _as_int(progress.get("regular_matches_total")),
        "playoff_matches_played": _as_int(progress.get("playoff_matches_played")),
        "playoff_matches_total": _as_int(progress.get("playoff_matches_total")),
        "overall_matches_played": _as_int(progress.get("overall_matches_played")),
        "overall_matches_total": _as_int(progress.get("overall_matches_total")),
    }

    return {
        "season_id": _as_int(summary.get("season_id", season_id), season_id),
        "divisions": summary_totals["divisions"],
        "divisions_total": _as_int(progress.get("divisions_total") or summary.get("divisions_total")),
        "divisions_finished": _as_int(progress.get("divisions_finished") or summary.get("divisions_finished")),
        "teams": summary_totals["teams"],
        "players": summary_totals["players"],
        "matches": summary_totals["matches"],
        "maps": summary_totals["maps"],
        "rounds": summary_totals["rounds"],
        "kills": summary_totals["kills"],
        "deaths": summary_totals["deaths"],
        "summary_totals": summary_totals,
        "adr_avg": _as_float(summary.get("adr_avg")),
        "kd_avg": _as_float(summary.get("kd_ratio") or summary.get("kd_avg")),
        "win_rate": _as_float(summary.get("win_rate")),
        "progress": progress_payload,
    }


async def get_divisions(season_id: int) -> List[Dict[str, Any]]:
    """Return divisions for a season with progress for the SPA overview."""
    cache, ttl_seconds = select_season_cache(season_id)
    if cache is not None:
        revision = await get_season_revision(season_id)
        cache_key = ("season-view-divisions", season_id, revision)
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_divisions(season_id),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_divisions(season_id)


async def _compute_divisions(season_id: int) -> List[Dict[str, Any]]:
    # Reuse the canonical season division aggregator to avoid duplicate SQL and drift.
    divisions = await seasons_service.get_season_divisions(season_id)

    result: list[dict[str, Any]] = []
    for row in divisions:
        season_stats = row.get("season") or {}
        playoff_stats = row.get("playoffs") or {}
        winners = row.get("winners") or []
        playoff_winner_team = playoff_stats.get("winner")
        meta_winner_team = row.get("mvp_team") or (
            winners[0].get("team_name") if winners and isinstance(winners[0], dict) else None
        )

        result.append(
            {
                "division_id": str(row.get("division_id") or ""),
                "tier": _as_int(row.get("tier") or row.get("division_num")),
                "name": str(row.get("name") or ""),
                "status": str(row.get("status") or "waiting"),
                "season": {
                    "teams": _as_int(season_stats.get("teams")),
                    "matches_played": _as_int(season_stats.get("matches_played")),
                    "matches_total": _as_int(season_stats.get("matches_total")),
                },
                "playoffs": {
                    "status": str(playoff_stats.get("status") or "waiting"),
                    "teams": _as_int(playoff_stats.get("teams"), 8),
                    "matches_played": _as_int(playoff_stats.get("matches_played")),
                    "matches_total": _as_int(playoff_stats.get("matches_total"), 7),
                    "winner_team": playoff_winner_team,
                    "playoff_championship_id": playoff_stats.get("playoff_championship_id"),
                },
                "meta": {
                    "winner_team": meta_winner_team,
                    "mvp_player": row.get("best_player"),
                },
            }
        )
    return result
