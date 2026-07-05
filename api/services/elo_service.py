from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Optional

from db_async import query_async
from elo_config import (
    BASE_K_FACTOR,
    DEFAULT_INITIAL_ELO,
    DYNAMIC_DIVISION_ELO,
    DYNAMIC_K_FACTOR,
    INITIAL_ELO_BOOTSTRAP,
    INITIAL_ELO,
    MAX_ELO_DELTA,
    MIN_ELO_DELTA,
    OUTCOME_WEIGHTS,
    STAT_BASELINES,
    STAT_WEIGHTS,
    bootstrap_initial_elo_for_division,
    clamp_division_multiplier,
    clamp_elo_delta,
    k_factor_for_maps,
)

from api.services.cache_helpers import GLOBAL_CACHE, get_global_revision


def _safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def _centered_metric(value: float, baseline: float) -> float:
    if baseline <= 0:
        return 0.0
    centered = (value / baseline) - 1.0
    return max(-1.5, min(1.5, centered))


def _build_match_metrics(row: dict[str, Any]) -> dict[str, float]:
    maps_played = float(row.get("maps_played") or 0)
    rounds_played = float(row.get("rounds_played") or 0)
    entry_count = float(row.get("entry_count") or 0)
    clutch_attempts = float(row.get("cl_1v1_attempts") or 0) + float(row.get("cl_1v2_attempts") or 0)
    clutch_wins = float(row.get("cl_1v1_wins") or 0) + float(row.get("cl_1v2_wins") or 0)
    flash_count = float(row.get("flash_count") or 0)

    return {
        "kd": float(row.get("kd") or 0.0),
        "kr": float(row.get("kr") or 0.0),
        "adr": float(row.get("adr") or 0.0),
        "mvps_per_map": _safe_div(float(row.get("mvps") or 0), maps_played),
        "entry_success_rate": _safe_div(float(row.get("entry_wins") or 0), entry_count),
        "clutch_success_rate": _safe_div(clutch_wins, clutch_attempts),
        "utility_per_round": _safe_div(float(row.get("utility_damage") or 0), rounds_played),
        "flash_success_rate": _safe_div(float(row.get("flash_successes") or 0), flash_count),
    }


def _compute_stat_score(metrics: dict[str, float]) -> float:
    stat_score = 0.0
    for key, weight in STAT_WEIGHTS.items():
        stat_score += weight * _centered_metric(metrics.get(key, 0.0), STAT_BASELINES[key])
    return stat_score


def _compute_dynamic_division_multipliers(match_rows: list[dict[str, Any]]) -> dict[tuple[int, int], float]:
    by_season: dict[int, list[float]] = defaultdict(list)
    by_season_division: dict[tuple[int, int], list[float]] = defaultdict(list)

    for row in match_rows:
        season = int(row.get("season") or 0)
        division_num = int(row.get("division_num") or 0)
        if season <= 0 or division_num < 0:
            continue
        metrics = _build_match_metrics(row)
        stat_score = _compute_stat_score(metrics)
        by_season[season].append(stat_score)
        by_season_division[(season, division_num)].append(stat_score)

    multipliers: dict[tuple[int, int], float] = {}
    sensitivity = float(DYNAMIC_DIVISION_ELO["sensitivity"])
    rank_reference = float(DYNAMIC_DIVISION_ELO["rank_reference_division"])
    rank_step = float(DYNAMIC_DIVISION_ELO["rank_step"])
    rank_blend = max(0.0, min(1.0, float(DYNAMIC_DIVISION_ELO["rank_blend"])))
    min_samples = float(DYNAMIC_DIVISION_ELO["min_samples_per_division"])
    shrink_samples = max(1.0, float(DYNAMIC_DIVISION_ELO["shrink_to_mean_samples"]))
    fallback = float(DYNAMIC_DIVISION_ELO["fallback_multiplier"])

    for key, scores in by_season_division.items():
        season, _division_num = key
        season_scores = by_season.get(season) or []
        if not season_scores:
            multipliers[key] = fallback
            continue

        n = float(len(scores))
        division_mean = sum(scores) / max(1.0, n)
        season_mean = sum(season_scores) / max(1.0, float(len(season_scores)))
        season_variance = sum((value - season_mean) ** 2 for value in season_scores) / max(1.0, float(len(season_scores)))
        season_std = season_variance ** 0.5

        # Standardize by season spread so coefficients are comparable across seasons.
        relative_gap = (division_mean - season_mean) / max(0.001, season_std)
        shrunk_signal = relative_gap * min(1.0, n / shrink_samples)
        raw_multiplier = 1.0 + (shrunk_signal * sensitivity)
        dynamic_multiplier = clamp_division_multiplier(raw_multiplier)

        rank_multiplier = clamp_division_multiplier(1.0 + ((rank_reference - float(_division_num)) * rank_step))
        dynamic_multiplier = clamp_division_multiplier(
            ((1.0 - rank_blend) * dynamic_multiplier) + (rank_blend * rank_multiplier)
        )

        if n < min_samples:
            blend = max(0.0, min(1.0, n / min_samples))
            dynamic_multiplier = (fallback * (1.0 - blend)) + (dynamic_multiplier * blend)

        multipliers[key] = clamp_division_multiplier(dynamic_multiplier)

    return multipliers


def _compute_match_delta_with_multiplier(
    row: dict[str, Any],
    division_multiplier: float,
    maps_played_before: int,
) -> tuple[float, dict[str, float]]:
    metrics = _build_match_metrics(row)
    stat_score = _compute_stat_score(metrics)

    result = int(row.get("result") or 0)
    outcome_score = OUTCOME_WEIGHTS["win_bonus"] if result == 1 else OUTCOME_WEIGHTS["loss_penalty"]
    dynamic_k = k_factor_for_maps(maps_played_before)
    delta = clamp_elo_delta(dynamic_k * division_multiplier * (stat_score + outcome_score))
    return delta, metrics


def _resolve_new_player_initial_elo(
    *,
    season_num: int,
    division_num: int,
    season_division_elo_pool: dict[tuple[int, int], dict[str, float]],
) -> float:
    pool_key = (season_num, division_num)
    existing_elos = list((season_division_elo_pool.get(pool_key) or {}).values())
    if existing_elos:
        return float(sum(existing_elos) / max(1, len(existing_elos)))
    return float(bootstrap_initial_elo_for_division(division_num))


async def _fetch_match_rows(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    player_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    filters = [
        "NULLIF(m.finished_at, 0) IS NOT NULL",
        "COALESCE(m.ignored_due_ban, 0) = 0",
        "COALESCE(ps.is_forfeit_map, 0) = 0",
        "COALESCE(mp.is_forfeit, 0) = 0",
    ]
    params: dict[str, Any] = {}
    if season is not None:
        filters.append("m.season = :season")
        params["season"] = season
    if division is not None:
        filters.append("m.division_num = :division")
        params["division"] = division
    if player_id is not None:
        filters.append("ps.player_id = :player_id")
        params["player_id"] = player_id

    return await query_async(
        f"""
        SELECT
            ps.player_id,
            MAX(COALESCE(pc.player_name, p.nickname)) AS nickname,
            MAX(p.avatar) AS avatar,
            MAX(p.faceit_url) AS faceit_url,
            MAX(ps.team_id) AS team_id,
            MAX(ps.opponent_team_id) AS opponent_team_id,
            MAX(t.name) AS team_name,
            MAX(ot.name) AS opponent_team_name,
            m.match_id,
            m.championship_id,
            m.season,
            m.division_num,
            m.finished_at,
            MAX(ps.result) AS result,
            COUNT(*) AS maps_played,
            SUM(COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0)) AS rounds_played,
            SUM(ps.kills) AS kills,
            SUM(ps.deaths) AS deaths,
            SUM(ps.assists) AS assists,
            SUM(ps.mvps) AS mvps,
            SUM(ps.headshots) AS headshots,
            SUM(ps.damage) AS damage,
            SUM(ps.enemies_flashed) AS enemies_flashed,
            SUM(ps.flash_count) AS flash_count,
            SUM(ps.flash_successes) AS flash_successes,
            SUM(ps.utility_damage) AS utility_damage,
            SUM(ps.utility_count) AS utility_count,
            SUM(ps.utility_successes) AS utility_successes,
            SUM(ps.utility_enemies) AS utility_enemies,
            SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
            SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
            SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
            SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
            SUM(ps.entry_count) AS entry_count,
            SUM(ps.entry_wins) AS entry_wins,
            COALESCE(SUM(ps.kills) / NULLIF(SUM(ps.deaths), 0), SUM(ps.kills)) AS kd,
            COALESCE(SUM(ps.kills) / NULLIF(SUM(COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0)), 0), 0) AS kr,
            COALESCE(SUM(ps.damage) / NULLIF(SUM(COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0)), 0), 0) AS adr
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN player_championships pc
          ON pc.player_id = ps.player_id
         AND pc.championship_id = m.championship_id
        LEFT JOIN teams t ON t.team_id = ps.team_id
        LEFT JOIN teams ot ON ot.team_id = ps.opponent_team_id
        LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE {' AND '.join(filters)}
        GROUP BY ps.player_id, m.match_id, m.championship_id, m.season, m.division_num, m.finished_at
        ORDER BY m.finished_at ASC, m.match_id ASC, ps.player_id ASC
        """,
        params,
    )


def _build_elo_state(
    match_rows: list[dict[str, Any]],
    *,
    history_player_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    dynamic_multipliers = _compute_dynamic_division_multipliers(match_rows)
    summaries: dict[str, dict[str, Any]] = {}
    histories: dict[str, list[dict[str, Any]]] = defaultdict(list)
    season_division_elo_pool: dict[tuple[int, int], dict[str, float]] = defaultdict(dict)
    player_maps_processed: dict[str, int] = defaultdict(int)
    participants_by_season: dict[int, set[str]] = defaultdict(set)
    participants_by_season_division: dict[tuple[int, int], set[str]] = defaultdict(set)

    for row in match_rows:
        player_id = str(row["player_id"])
        season_num = int(row.get("season") or 0)
        division_num = int(row.get("division_num") or 0)
        current = summaries.get(row["player_id"])
        if current:
            elo_before = float(current["current_elo"])
            initial_elo = float(current.get("initial_elo") or INITIAL_ELO)
        else:
            elo_before = _resolve_new_player_initial_elo(
                season_num=season_num,
                division_num=division_num,
                season_division_elo_pool=season_division_elo_pool,
            )
            initial_elo = elo_before

        maps_played_before = int(player_maps_processed[player_id] or 0)
        division_multiplier = dynamic_multipliers.get(
            (season_num, division_num),
            float(DYNAMIC_DIVISION_ELO["fallback_multiplier"]),
        )
        delta, metrics = _compute_match_delta_with_multiplier(
            row,
            division_multiplier,
            maps_played_before,
        )
        elo_after = max(0.0, elo_before + delta)

        history_row = {
            "match_id": row["match_id"],
            "championship_id": row["championship_id"],
            "season": row["season"],
            "division_num": row["division_num"],
            "finished_at": row["finished_at"],
            "team_id": row.get("team_id"),
            "team_name": row.get("team_name"),
            "opponent_team_id": row.get("opponent_team_id"),
            "opponent_team_name": row.get("opponent_team_name"),
            "elo_before": round(elo_before, 2),
            "elo_after": round(elo_after, 2),
            "elo_delta": round(delta, 2),
            "division_multiplier": round(division_multiplier, 4),
            "result": int(row.get("result") or 0),
            "metrics": metrics,
        }
        participants_by_season[season_num].add(player_id)
        participants_by_season_division[(season_num, division_num)].add(player_id)
        if history_player_ids is None or player_id in history_player_ids:
            histories[player_id].append(history_row)
        season_division_elo_pool[(season_num, division_num)][player_id] = float(elo_after)
        player_maps_processed[player_id] += int(row.get("maps_played") or 0)
        summaries[player_id] = {
            "player_id": player_id,
            "nickname": row.get("nickname"),
            "avatar": row.get("avatar"),
            "faceit_url": row.get("faceit_url"),
            "last_team_id": row.get("team_id"),
            "last_team_name": row.get("team_name"),
            "current_elo": round(elo_after, 2),
            "last_elo_delta": round(delta, 2),
            "matches_processed": int(len(histories[player_id])),
            "last_match_id": row["match_id"],
            "last_finished_at": row["finished_at"],
            "last_championship_id": row["championship_id"],
            "last_division_num": row["division_num"],
            "last_season": row["season"],
            "last_division_multiplier": round(division_multiplier, 4),
            "initial_elo": round(initial_elo, 2),
        }

    return {
        "summaries": summaries,
        "histories": histories,
        "participants_by_season": {
            season_num: sorted(player_ids)
            for season_num, player_ids in participants_by_season.items()
        },
        "participants_by_season_division": {
            f"{season_num}:{division_num}": sorted(player_ids)
            for (season_num, division_num), player_ids in participants_by_season_division.items()
        },
    }


async def _compute_elo_summary_index() -> dict[str, Any]:
    match_rows = await _fetch_match_rows()
    return _build_elo_state(match_rows, history_player_ids=set())


async def _compute_player_elo_index(player_id: str) -> dict[str, Any]:
    match_rows = await _fetch_match_rows()
    return _build_elo_state(match_rows, history_player_ids={str(player_id)})


async def _get_cached_elo_summary_index() -> dict[str, Any]:
    revision = await get_global_revision()
    cache_key = ("elo-summary-index", revision)

    async def _compute() -> dict[str, Any]:
        return await _compute_elo_summary_index()

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def _get_cached_player_elo_index(player_id: str) -> dict[str, Any]:
    revision = await get_global_revision()
    cache_key = ("elo-player-index", str(player_id), revision)

    async def _compute() -> dict[str, Any]:
        return await _compute_player_elo_index(str(player_id))

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, _compute)
    return cached_value


async def get_player_elo_summary(player_id: str) -> dict[str, Any] | None:
    elo_index = await _get_cached_player_elo_index(player_id)
    summary = dict((elo_index.get("summaries") or {}).get(player_id) or {})
    if not summary:
        return {
            "player_id": player_id,
            "current_elo": DEFAULT_INITIAL_ELO,
            "last_elo_delta": 0.0,
            "matches_processed": 0,
            "initial_elo": DEFAULT_INITIAL_ELO,
        }
    return summary


async def get_player_elo_history(
    player_id: str,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    elo_index = await _get_cached_player_elo_index(player_id)
    history = list((elo_index.get("histories") or {}).get(player_id, []))
    if limit > 0:
        history = history[-limit:]
    return history


async def get_player_elo_bundle(
    player_id: str,
    *,
    limit: int = 50,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    elo_index = await _get_cached_player_elo_index(player_id)
    summary = dict((elo_index.get("summaries") or {}).get(player_id) or {})
    if not summary:
        summary = {
            "player_id": player_id,
            "current_elo": DEFAULT_INITIAL_ELO,
            "last_elo_delta": 0.0,
            "matches_processed": 0,
            "initial_elo": DEFAULT_INITIAL_ELO,
        }
    history = list((elo_index.get("histories") or {}).get(player_id, []))
    if limit > 0:
        history = history[-limit:]
    return summary, history


async def get_elo_leaderboard(
    *,
    season: Optional[int] = None,
    division: Optional[int] = None,
    participation_season: Optional[int] = None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    # Always compute Elo and dynamic division multipliers from full history.
    # Endpoint filters are only for deciding which players are displayed.
    elo_index = await _get_cached_elo_summary_index()
    rows = list((elo_index.get("summaries") or {}).values())

    if participation_season is not None or division is not None:
        participants_by_season = elo_index.get("participants_by_season") or {}
        participants_by_season_division = elo_index.get("participants_by_season_division") or {}
        if participation_season is not None and division is not None:
            key = f"{int(participation_season)}:{int(division)}"
            filtered_player_ids = set(participants_by_season_division.get(key) or [])
        elif participation_season is not None:
            filtered_player_ids = set(participants_by_season.get(int(participation_season)) or [])
        else:
            filtered_player_ids = {
                str(player_id)
                for composite_key, player_ids in participants_by_season_division.items()
                if composite_key.split(":", 1)[1] == str(int(division))
                for player_id in (player_ids or [])
            }
        rows = [row for row in rows if str(row.get("player_id")) in filtered_player_ids]
    rows.sort(
        key=lambda row: (
            -float(row.get("current_elo") or INITIAL_ELO),
            -int(row.get("matches_processed") or 0),
            str(row.get("nickname") or ""),
        )
    )
    if limit > 0:
        rows = rows[:limit]
    return rows


def get_public_elo_config() -> dict[str, Any]:
    """Return frontend-safe Elo config snapshot for transparent UI explanation."""
    return {
        "initial_elo": DEFAULT_INITIAL_ELO,
        "base_k_factor": BASE_K_FACTOR,
        "min_elo_delta": MIN_ELO_DELTA,
        "max_elo_delta": MAX_ELO_DELTA,
        "outcome_weights": dict(OUTCOME_WEIGHTS),
        "stat_weights": dict(STAT_WEIGHTS),
        "stat_baselines": dict(STAT_BASELINES),
        "dynamic_division_elo": dict(DYNAMIC_DIVISION_ELO),
        "dynamic_k_factor": dict(DYNAMIC_K_FACTOR),
        "initial_elo_bootstrap": dict(INITIAL_ELO_BOOTSTRAP),
        "formulas": {
            "stat_score": "sum(weight_i * centered(metric_i, baseline_i))",
            "centered": "centered = clamp((metric / baseline) - 1, -1.5, 1.5)",
            "k_dynamic": "K_dynamic(maps) = BASE_K_FACTOR * phase(first_10_maps_high, then_stabilize_to_min)",
            "division_multiplier": "blend(dynamic_season_gap_multiplier, rank_prior_multiplier, rank_blend)",
            "new_player_initial_elo": "elo_before = avg(existing_elos_in_same_season_division) or clamp(1000 + (rank_reference_division-division_num)*rank_step_points)",
            "elo_delta": "delta = clamp(K_dynamic * division_multiplier * (stat_score + outcome_score), MIN_ELO_DELTA, MAX_ELO_DELTA)",
            "elo_update": "elo_after = max(0, elo_before + delta)",
        },
    }