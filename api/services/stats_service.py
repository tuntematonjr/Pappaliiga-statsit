from __future__ import annotations

import asyncio
from typing import Any, Dict, Literal, Optional

from db_async import count_played_matches, count_played_matches_by_season_and_playoff, query_async

from api.exceptions import BadRequestError, NotFoundError
from api.services.cache_helpers import (
    GLOBAL_CACHE,
    get_championship_revision,
    get_global_revision,
    get_season_revision,
    select_season_cache,
)
from api.services.player_counts import get_player_counts
from api.services.season_aggregates import dedupe_team_total, get_season_summary_totals
from division_overrides import combined_status_teams


def _get_excluded_team_ids(championship_id: str) -> set[str]:
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}


def _build_empty_averages() -> dict[str, float]:
    return {
        "avg_kd": 0.0,
        "median_kd": 0.0,
        "avg_adr": 0.0,
        "median_adr": 0.0,
        "avg_hs_pct": 0.0,
        "median_hs_pct": 0.0,
        "avg_kr": 0.0,
        "median_kr": 0.0,
        "avg_first_kills": 0.0,
        "median_first_kills": 0.0,
        "avg_entry_pct": 0.0,
        "median_entry_pct": 0.0,
        "avg_clutch_pct": 0.0,
        "median_clutch_pct": 0.0,
        "avg_flash_success_pct": 0.0,
        "median_flash_success_pct": 0.0,
        "avg_utility_success_pct": 0.0,
        "median_utility_success_pct": 0.0,
    }


def _compute_averages_from_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
    def _safe_number(value: Any) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0

    def _avg(values: list[float]) -> float:
        return (sum(values) / len(values)) if values else 0.0

    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    if not rows:
        return _build_empty_averages()

    kd_values: list[float] = []
    adr_values: list[float] = []
    hs_values: list[float] = []
    kr_values: list[float] = []
    first_kills_values: list[float] = []
    entry_values: list[float] = []
    clutch_values: list[float] = []
    flash_success_values: list[float] = []
    utility_success_values: list[float] = []

    for row in rows:
        kd_values.append(_safe_number(row.get("kd")))
        adr_values.append(_safe_number(row.get("adr")))
        hs_values.append(_safe_number(row.get("hs_pct")))
        kr_values.append(_safe_number(row.get("kr")))
        first_kills_values.append(_safe_number(row.get("first_kills")))

        entry_wins = _safe_number(row.get("entry_wins"))
        entry_count = _safe_number(row.get("entry_count"))
        entry_pct = (entry_wins / entry_count * 100.0) if entry_count > 0 else 0.0
        entry_values.append(entry_pct)

        clutch_wins = _safe_number(row.get("cl_1v1_wins")) + _safe_number(row.get("cl_1v2_wins"))
        clutch_attempts = _safe_number(row.get("cl_1v1_attempts")) + _safe_number(row.get("cl_1v2_attempts"))
        clutch_pct = (clutch_wins / clutch_attempts * 100.0) if clutch_attempts > 0 else 0.0
        clutch_values.append(clutch_pct)

        flash_successes = _safe_number(row.get("flash_successes"))
        flash_count = _safe_number(row.get("flash_count"))
        flash_success_pct = (flash_successes / flash_count * 100.0) if flash_count > 0 else 0.0
        flash_success_values.append(flash_success_pct)

        utility_successes = _safe_number(row.get("utility_successes"))
        utility_count = _safe_number(row.get("utility_count"))
        utility_success_pct = (utility_successes / utility_count * 100.0) if utility_count > 0 else 0.0
        utility_success_values.append(utility_success_pct)

    return {
        "avg_kd": _avg(kd_values),
        "median_kd": _median(kd_values),
        "avg_adr": _avg(adr_values),
        "median_adr": _median(adr_values),
        "avg_hs_pct": _avg(hs_values),
        "median_hs_pct": _median(hs_values),
        "avg_kr": _avg(kr_values),
        "median_kr": _median(kr_values),
        "avg_first_kills": _avg(first_kills_values),
        "median_first_kills": _median(first_kills_values),
        "avg_entry_pct": _avg(entry_values),
        "median_entry_pct": _median(entry_values),
        "avg_clutch_pct": _avg(clutch_values),
        "median_clutch_pct": _median(clutch_values),
        "avg_flash_success_pct": _avg(flash_success_values),
        "median_flash_success_pct": _median(flash_success_values),
        "avg_utility_success_pct": _avg(utility_success_values),
        "median_utility_success_pct": _median(utility_success_values),
    }


SUMMARY_METRIC_DEFINITIONS: tuple[tuple[str, str], ...] = (
    ("divisions", "Divisions"),
    ("teams", "Teams"),
    ("players", "Players"),
    ("matches", "Matches"),
    ("maps", "Maps"),
    ("rounds", "Rounds"),
    ("kills", "Kills"),
    ("deaths", "Deaths"),
)


def _build_metric_list(summary_totals: Dict[str, Any]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for metric_id, label in SUMMARY_METRIC_DEFINITIONS:
        metrics.append(
            {
                "id": metric_id,
                "label": label,
                "value": int(summary_totals.get(metric_id) or 0),
            }
        )
    return metrics


async def _get_lifetime_summary_totals() -> dict[str, int]:
    revision = await get_global_revision()
    cache_key = ("lifetime-summary", revision)
    cached_value, _ = await GLOBAL_CACHE.get_or_set(
        cache_key,
        _compute_lifetime_summary_totals,
    )
    return cached_value


async def _compute_lifetime_summary_totals() -> dict[str, int]:
    (
        div_rows,
        team_rows,
        map_rows,
        map_round_rows,
        team_totals_rows,
        player_totals_rows,
        matches_played,
        player_counts,
    ) = await asyncio.gather(
        # Only count base championships so playoff-only brackets do not inflate the division total.
        query_async("SELECT COUNT(*) AS cnt FROM championships WHERE is_playoffs = 0"),
        query_async("SELECT COUNT(DISTINCT team_id) AS cnt FROM teams"),
        query_async("SELECT COUNT(*) AS cnt FROM maps"),
        query_async(
            """
        SELECT
          COALESCE(SUM(CASE WHEN is_forfeit = 0 THEN (COALESCE(score_team1,0) + COALESCE(score_team2,0)) ELSE 0 END),0) AS total_rounds
        FROM maps
        """
        ),
        query_async(
            """
        SELECT
          COALESCE(SUM(maps_played), 0) AS maps_played_total,
          COALESCE(SUM(rounds_won + rounds_lost), 0) AS rounds_total
        FROM team_season_totals
        """
        ),
        query_async(
            """
        SELECT
          COALESCE(SUM(kills), 0) AS total_kills,
          COALESCE(SUM(deaths), 0) AS total_deaths
        FROM player_season_totals
        """
        ),
        count_played_matches(include_forfeits=False, include_ignored=True),
        get_player_counts(include_all_time=True),
    )
    team_totals_row = team_totals_rows[0] if team_totals_rows else {}
    player_totals_row = player_totals_rows[0] if player_totals_rows else {}

    maps_total = int(map_rows[0]["cnt"] if map_rows else 0)
    fallback_maps = dedupe_team_total(team_totals_row.get("maps_played_total"))
    if maps_total == 0:
        maps_total = fallback_maps

    map_round_row = map_round_rows[0] if map_round_rows else {}
    rounds_total = int(map_round_row.get("total_rounds") or 0)
    fallback_rounds = dedupe_team_total(team_totals_row.get("rounds_total"))
    if rounds_total == 0:
        rounds_total = fallback_rounds

    kills_total = int(player_totals_row.get("total_kills") or 0)
    deaths_total = int(player_totals_row.get("total_deaths") or 0)

    return {
        "divisions": int(div_rows[0]["cnt"] if div_rows else 0),
        "teams": int(team_rows[0]["cnt"] if team_rows else 0),
        "players": int(player_counts.get("all_time_players") or 0),
        # Matches, maps, rounds, kills and deaths combine played games from the regular season and playoffs.
        "matches": int(matches_played or 0),
        "maps": maps_total,
        "rounds": rounds_total,
        "kills": kills_total,
        "deaths": deaths_total,
    }


async def get_overview_stats() -> dict[str, int]:
    revision = await get_global_revision()
    cache_key = ("overview-stats", revision)
    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute_overview_stats)
    return cached_value


async def _compute_overview_stats() -> dict[str, int]:
    season_rows = await query_async(
        "SELECT COUNT(DISTINCT season) AS cnt FROM championships"
    )
    summary_totals = await _get_lifetime_summary_totals()
    return {
        "total_seasons": int(season_rows[0]["cnt"] if season_rows else 0),
        "total_divisions": summary_totals["divisions"],
        "total_teams": summary_totals["teams"],
        "total_players": summary_totals["players"],
        "total_matches": summary_totals["matches"],
        "total_maps_played": summary_totals["maps"],
        "total_rounds": summary_totals["rounds"],
        "total_kills": summary_totals["kills"],
        "total_deaths": summary_totals["deaths"],
        "totals": summary_totals,
    }


_STAT_COLUMN_MAP = {
    "kd": "kd",
    "adr": "adr",
    "hs_pct": "hs_pct",
    "kills": "kills",
    "mvps": "mvps",
    "kr": "kr",
}


async def get_top_players(
    stat: str,
    *,
    season: Optional[int],
    division: Optional[int],
    limit: int,
    min_maps: int,
) -> list[dict[str, Any]]:
    cache = None
    ttl_seconds = None
    revision = None

    if season is not None:
        cache, ttl_seconds = select_season_cache(season)
        if cache is not None:
            revision = await get_season_revision(season)
    else:
        revision = await get_global_revision()
        cache = GLOBAL_CACHE

    cache_key = ("top-players", stat, season, division, limit, min_maps, revision)
    if cache is not None:
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_top_players(stat, season=season, division=division, limit=limit, min_maps=min_maps),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_top_players(stat, season=season, division=division, limit=limit, min_maps=min_maps)


async def _compute_top_players(
    stat: str,
    *,
    season: Optional[int],
    division: Optional[int],
    limit: int,
    min_maps: int,
) -> list[dict[str, Any]]:
    column = _STAT_COLUMN_MAP.get(stat)
    if column is None:
        raise BadRequestError(f"Invalid stat '{stat}'")

    where_clauses = ["pst.maps_played >= :min_maps"]
    params: Dict[str, Any] = {"min_maps": min_maps, "limit": limit}

    if season is not None:
        where_clauses.append("pst.season = :season")
        params["season"] = season

    if division is not None:
        where_clauses.append("pst.division_num = :division")
        params["division"] = division

    where_clause = " AND ".join(where_clauses)

    rows = await query_async(
        f"""
        SELECT pst.player_id, COALESCE(pc.player_name, p.nickname) AS nickname, t.name AS team_name,
               pst.{column} AS stat_value, pst.maps_played,
               pst.season, pst.division_num
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        LEFT JOIN championships c
            ON c.season = pst.season
           AND c.division_num = pst.division_num
           AND COALESCE(c.is_playoffs, 0) = 0
        LEFT JOIN player_championships pc
            ON pc.player_id = pst.player_id
           AND pc.championship_id = c.championship_id
        LEFT JOIN teams t ON t.team_id = pst.team_id
        WHERE {where_clause}
        ORDER BY pst.{column} DESC
        LIMIT :limit
        """,
        params,
    )
    return rows


async def get_division_averages(championship_id: str) -> dict[str, float]:
    revision = await get_championship_revision(str(championship_id))
    cache_key = ("division-averages", championship_id, revision)
    cached_value, _ = await GLOBAL_CACHE.get_or_set(
        cache_key,
        lambda: _compute_division_averages(championship_id),
    )
    return cached_value


async def get_season_averages(season: int) -> dict[str, float]:
    revision = await get_season_revision(season)
    cache_key = ("season-averages", season, revision)
    cached_value, _ = await GLOBAL_CACHE.get_or_set(
        cache_key,
        lambda: _compute_season_averages(season),
    )
    return cached_value


async def _compute_division_averages(championship_id: str) -> dict[str, float]:
    champ_rows = await query_async(
        "SELECT season, division_num FROM championships WHERE championship_id = :champ_id",
        {"champ_id": championship_id},
    )
    if not champ_rows:
        raise NotFoundError(f"Championship {championship_id} not found")

    season = champ_rows[0]["season"]
    division_num = champ_rows[0]["division_num"]
    excluded = _get_excluded_team_ids(championship_id)

    where_clause = "pst.season = :season AND pst.division_num = :division"
    params: Dict[str, Any] = {"season": season, "division": division_num}
    if excluded:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded)))
        where_clause += f" AND pst.team_id NOT IN ({placeholders})"
        for i, tid in enumerate(excluded):
            params[f"ex{i}"] = tid

    rows = await query_async(
        f"""
        SELECT
            pst.kd,
            pst.adr,
            pst.hs_pct,
            pst.kr,
            pst.first_kills,
            pst.entry_wins,
            pst.entry_count,
            pst.cl_1v1_wins,
            pst.cl_1v2_wins,
            pst.cl_1v1_attempts,
            pst.cl_1v2_attempts,
            pst.flash_successes,
            pst.flash_count,
            pst.utility_successes,
            pst.utility_count
        FROM player_season_totals pst
        WHERE {where_clause} AND pst.maps_played >= 3
        """,
        params,
    )

    return _compute_averages_from_rows(rows)


async def _compute_season_averages(season: int) -> dict[str, float]:
    champ_rows = await query_async(
        """
        SELECT championship_id
        FROM championships
        WHERE season = :season AND COALESCE(is_playoffs, 0) = 0
        """,
        {"season": season},
    )
    if not champ_rows:
        raise NotFoundError(f"Season {season} not found")

    excluded: set[str] = set()
    for row in champ_rows:
        champ_id = str(row.get("championship_id") or "")
        if champ_id:
            excluded.update(_get_excluded_team_ids(champ_id))

    where_clause = "pst.season = :season"
    params: Dict[str, Any] = {"season": season}
    if excluded:
        placeholders = ", ".join(f":ex{i}" for i in range(len(excluded)))
        where_clause += f" AND pst.team_id NOT IN ({placeholders})"
        for i, tid in enumerate(excluded):
            params[f"ex{i}"] = tid

    rows = await query_async(
        f"""
        SELECT
            pst.kd,
            pst.adr,
            pst.hs_pct,
            pst.kr,
            pst.first_kills,
            pst.entry_wins,
            pst.entry_count,
            pst.cl_1v1_wins,
            pst.cl_1v2_wins,
            pst.cl_1v1_attempts,
            pst.cl_1v2_attempts,
            pst.flash_successes,
            pst.flash_count,
            pst.utility_successes,
            pst.utility_count
        FROM player_season_totals pst
        WHERE {where_clause} AND pst.maps_played >= 3
        """,
        params,
    )

    return _compute_averages_from_rows(rows)


async def get_season_stats(season: int) -> dict[str, Any]:
    """Return aggregated statistics for an entire season.

    The payload mirrors the legacy shape used by the frontend Home view.
    """

    cache, ttl_seconds = select_season_cache(season)
    if cache is not None:
        revision = await get_season_revision(season)
        cache_key = ("season-stats", season, revision)
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_season_stats(season),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_season_stats(season)


async def _compute_season_stats(season: int) -> dict[str, Any]:
    totals_payload = await get_season_summary_totals(season)
    season_summary_totals = totals_payload["summary_totals"]
    team_totals = totals_payload["team_totals"]
    player_totals = totals_payload["player_totals"]

    # Progress tracking by championship (regular vs playoffs)
    progress_rows = await query_async(
        """
        SELECT
            CASE WHEN c.is_playoffs = 1 THEN 1 ELSE 0 END AS is_playoff,
            COUNT(DISTINCT m.match_id) AS total_matches
        FROM championships c
        LEFT JOIN matches m ON m.championship_id = c.championship_id
        WHERE c.season = :season
        GROUP BY is_playoff
        """,
        {"season": season},
    )
    played_map = await count_played_matches_by_season_and_playoff(
        seasons=[season],
        include_forfeits=True,
        include_ignored=True,
    )
    regular_played = int(played_map.get((season, 0), 0))
    playoff_played = int(played_map.get((season, 1), 0))

    overall_total = 0
    regular = {"played": regular_played, "total": 0}
    playoffs = {"played": playoff_played, "total": 0}

    for row in progress_rows:
        total = int(row.get("total_matches") or 0)
        overall_total += total
        target = playoffs if row.get("is_playoff") else regular
        target["total"] += total
    overall_played = regular["played"] + playoffs["played"]

    progress = {
        "overall": {"played": overall_played, "total": overall_total},
        "regular": regular,
        "playoffs": playoffs,
    }

    # Aggregate map stats across teams for the season
    team_map_rows = await query_async(
        """
        SELECT
            map_name,
            COALESCE(SUM(games), 0) AS games_played,
            COALESCE(SUM(wins), 0) AS wins,
            COALESCE(SUM(played), 0) AS picks,
            COALESCE(SUM(total_own_ban + opp_ban), 0) AS bans,
            COALESCE(SUM(kills), 0) AS kills,
            COALESCE(SUM(deaths), 0) AS deaths,
            COALESCE(SUM(adr * NULLIF(games,0)), 0) AS adr_weighted,
            COALESCE(SUM(CASE WHEN games > 0 THEN games ELSE 0 END), 0) AS adr_weight
        FROM team_map_season_totals
        WHERE season = :season
        GROUP BY map_name
        ORDER BY games_played DESC, map_name ASC
        """,
        {"season": season},
    )

    map_stats: list[dict[str, Any]] = []
    for row in team_map_rows:
        games_played = int(row.get("games_played") or 0)
        wins = int(row.get("wins") or 0)
        kills = float(row.get("kills") or 0)
        deaths = float(row.get("deaths") or 0)
        adr_weight = float(row.get("adr_weight") or 0)
        adr_weighted = float(row.get("adr_weighted") or 0)
        map_stats.append(
            {
                "map_name": row.get("map_name"),
                "maps_played": games_played,
                "wins": wins,
                "win_rate": float(wins) / games_played * 100 if games_played else 0.0,
                "banned": int(row.get("bans") or 0),
                "kills": int(kills),
                "deaths": int(deaths),
                "kd": (kills / deaths) if deaths else (kills if kills else 0.0),
                "adr": adr_weighted / adr_weight if adr_weight else 0.0,
                "rounds_played": 0,
                "clutches": 0,
                "sniper_kills": 0,
                "pistol_kills": 0,
            }
        )

    aggregates = {
        "season": season,
        "total_teams": season_summary_totals["teams"],
        "matches_played_total": team_totals["matches_played_total"],
        "maps_played_total": team_totals["maps_played_total"],
        "rounds_played_total": team_totals["rounds_played_total"],
        "total_players": season_summary_totals["players"],
        "total_kills": season_summary_totals["kills"],
        "total_deaths": season_summary_totals["deaths"],
        "median_adr": player_totals["avg_adr"],
        "median_survival": 0.0,
        "summary_totals": season_summary_totals,
    }

    return {
        "season": season,
        "aggregates": aggregates,
        "summary_totals": season_summary_totals,
        "map_stats": map_stats,
        "leaders": [],
        "progress": progress,
    }


StatsSummaryScope = Literal["all", "season"]


async def _get_season_progress(season: int, summary_totals: dict[str, Any]) -> dict[str, int]:
    progress_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT c.championship_id) AS total_divisions,
            COUNT(
                DISTINCT CASE
                    WHEN (
                        SELECT COUNT(*)
                        FROM matches m2
                        WHERE m2.championship_id = c.championship_id
                          AND NULLIF(m2.finished_at, 0) IS NULL
                    ) = 0
                    THEN c.championship_id
                END
            ) AS finished_divisions
        FROM championships c
        WHERE c.season = :season
          AND c.is_playoffs = 0
        """,
        {"season": season},
    )

    match_progress_rows = await query_async(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN c.is_playoffs = 0 THEN m.match_id END) AS regular_total,
            COUNT(DISTINCT CASE WHEN c.is_playoffs = 1 THEN m.match_id END) AS playoff_total
        FROM championships c
        LEFT JOIN matches m ON m.championship_id = c.championship_id
        WHERE c.season = :season
        """,
        {"season": season},
    )
    played_map = await count_played_matches_by_season_and_playoff(
        seasons=[season],
        include_forfeits=True,
        include_ignored=True,
    )
    regular_played = int(played_map.get((season, 0), 0))
    playoff_played = int(played_map.get((season, 1), 0))

    progress_data = progress_rows[0] if progress_rows else {}
    match_progress_data = match_progress_rows[0] if match_progress_rows else {}

    regular_total = int(match_progress_data.get("regular_total") or 0)
    regular_played = int(regular_played or 0)
    playoff_total = int(match_progress_data.get("playoff_total") or 0)
    playoff_played = int(playoff_played or 0)
    overall_total = regular_total + playoff_total
    overall_played = regular_played + playoff_played

    divisions_total = int(progress_data.get("total_divisions") or 0)
    divisions_finished = int(progress_data.get("finished_divisions") or 0)
    if divisions_total == 0:
        divisions_total = int(summary_totals.get("divisions") or 0)

    return {
        "divisions_finished": divisions_finished,
        "divisions_total": divisions_total,
        "regular_matches_played": regular_played,
        "regular_matches_total": regular_total,
        "playoff_matches_played": playoff_played,
        "playoff_matches_total": playoff_total,
        "overall_matches_played": overall_played,
        "overall_matches_total": overall_total,
    }


async def get_stats_summary(scope: StatsSummaryScope, season: Optional[int] = None) -> dict[str, Any]:
    if scope not in ("all", "season"):
        raise BadRequestError(f"Unsupported stats summary scope '{scope}'")

    if scope == "all":
        revision = await get_global_revision()
        cache_key = ("stats-summary", "all", revision)
        cached_value, _ = await GLOBAL_CACHE.get_or_set(
            cache_key,
            _compute_stats_summary_all,
        )
        return cached_value

    if season is None:
        raise BadRequestError("Season identifier required for season scope")

    cache, ttl_seconds = select_season_cache(season)
    if cache is not None:
        revision = await get_season_revision(season)
        cache_key = ("stats-summary", "season", season, revision)
        cached_value, _ = await cache.get_or_set(
            cache_key,
            lambda: _compute_stats_summary_season(season),
            ttl_seconds=ttl_seconds,
        )
        return cached_value

    return await _compute_stats_summary_season(season)


async def _compute_stats_summary_all() -> dict[str, Any]:
    summary_totals = await _get_lifetime_summary_totals()
    return {
        "scope": "all",
        "season": None,
        "label": "All Seasons",
        "summary_totals": summary_totals,
        "metrics": _build_metric_list(summary_totals),
        "progress": None,
    }


async def _compute_stats_summary_season(season: int) -> dict[str, Any]:
    totals_payload = await get_season_summary_totals(season)
    summary_totals = totals_payload["summary_totals"]
    progress = await _get_season_progress(season, summary_totals)
    return {
        "scope": "season",
        "season": season,
        "label": f"Season {season}",
        "summary_totals": summary_totals,
        "metrics": _build_metric_list(summary_totals),
        "progress": progress,
    }
