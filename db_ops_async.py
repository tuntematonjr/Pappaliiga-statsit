from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, Optional, Sequence

import asyncmy
from asyncmy import cursors, errors as asyncmy_errors

from db_async import connection

DEFAULT_TEAM_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"

Row = Mapping[str, Any]


@asynccontextmanager
async def _write_connection(
    conn: asyncmy.Connection | None,
    *,
    label: str,
) -> AsyncIterator[asyncmy.Connection]:
    """Return provided connection or acquire a new transactional connection."""
    if conn is not None:
        yield conn
        return
    async with connection(label=label) as owned_conn:
        yield owned_conn
LOGGER = logging.getLogger(__name__)


def _is_retryable_error(exc: Exception) -> bool:
    """Check if database exception is retryable (deadlock or lock timeout)."""
    if isinstance(exc, asyncmy_errors.OperationalError):
        error_code = exc.args[0] if exc.args else 0
        # 1213 = Deadlock, 1205 = Lock wait timeout
        return error_code in (1213, 1205)
    return False


async def _retry_on_deadlock(
    operation,
    *,
    label: str,
    max_attempts: int = 3,
    base_delay: float = 0.1,
) -> Any:
    """
    Retry a coroutine function if it encounters a deadlock or lock timeout.
    Uses exponential backoff with jitter and logs each attempt for diagnostics.
    """
    import random

    LOGGER.debug("retry[%s] start max_attempts=%d", label, max_attempts)
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await operation()
            LOGGER.debug("retry[%s] attempt %d succeeded", label, attempt)
            return result
        except Exception as exc:
            last_exc = exc
            retryable = _is_retryable_error(exc)
            if attempt < max_attempts and retryable:
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                LOGGER.warning(
                    "retry[%s] attempt %d/%d hit %s (%s); sleeping %.2fs",
                    label,
                    attempt,
                    max_attempts,
                    exc.__class__.__name__,
                    getattr(exc, "args", exc),
                    delay,
                )
                LOGGER.debug("retry[%s] sleep-start attempt=%d delay=%.2fs", label, attempt, delay)
                await asyncio.sleep(delay)
                LOGGER.debug("retry[%s] sleep-end attempt=%d", label, attempt)
                continue
            LOGGER.error(
                "retry[%s] giving up after attempt %d/%d (retryable=%s): %s",
                label,
                attempt,
                max_attempts,
                retryable,
                exc,
            )
            raise
    if last_exc:
        raise last_exc
    return None

_TS_EXPR = (
    "COALESCE(m.finished_at, m.started_at, m.scheduled_at, m.configured_at, m.last_seen_at, 0)"
)

_CHAMPIONSHIP_UPSERT_SQL = """
    INSERT INTO championships (championship_id, season, division_num, name, is_playoffs, slug, parent_championship_id, winner_team_id)
    VALUES (%(championship_id)s, %(season)s, %(division_num)s, %(name)s, %(is_playoffs)s, %(slug)s, %(parent_championship_id)s, %(winner_team_id)s)
    ON DUPLICATE KEY UPDATE
      season = VALUES(season),
      division_num = VALUES(division_num),
      name = CASE WHEN championships.name = '' THEN VALUES(name) ELSE championships.name END,
      is_playoffs = VALUES(is_playoffs),
      slug = CASE WHEN championships.slug = '' THEN VALUES(slug) ELSE championships.slug END,
      parent_championship_id = VALUES(parent_championship_id),
      winner_team_id = VALUES(winner_team_id)
"""

_TEAM_UPSERT_SQL = """
    INSERT INTO teams (team_id, name, avatar)
    VALUES (%(team_id)s, %(name)s, %(avatar)s)
    ON DUPLICATE KEY UPDATE
      name = CASE WHEN VALUES(name) <> '' THEN VALUES(name) ELSE teams.name END,
      avatar = CASE WHEN VALUES(avatar) <> '' THEN VALUES(avatar) ELSE teams.avatar END
"""

_MAP_UPSERT_SQL = """
    INSERT INTO maps (
      match_id, season, division_num, round_index, map_name,
      score_team1, score_team2, winner_team_id, is_forfeit
    )
    VALUES (
      %(match_id)s, %(season)s, %(division_num)s, %(round_index)s, %(map_name)s,
      %(score_team1)s, %(score_team2)s, %(winner_team_id)s, %(is_forfeit)s
    )
    ON DUPLICATE KEY UPDATE
      map_name = VALUES(map_name),
      score_team1 = VALUES(score_team1),
      score_team2 = VALUES(score_team2),
      winner_team_id = VALUES(winner_team_id),
      is_forfeit = VALUES(is_forfeit)
"""

_PLAYER_STAT_UPSERT_SQL = """
    INSERT INTO player_stats (
      season, division_num, match_id, round_index, map_id, player_id, team_id, opponent_team_id,
      is_forfeit_map, kills, deaths, assists, kd, kr, adr, hs_pct, mvps, sniper_kills,
      utility_damage, enemies_flashed, flash_count, flash_successes,
      mk_2k, mk_3k, mk_4k, mk_5k,
      clutch_kills, cl_1v1_attempts, cl_1v1_wins,
      cl_1v2_attempts, cl_1v2_wins, entry_count, entry_wins,
      pistol_kills, damage
    )
    VALUES (
      %(season)s, %(division_num)s, %(match_id)s, %(round_index)s, %(map_id)s, %(player_id)s, %(team_id)s, %(opponent_team_id)s,
      %(is_forfeit_map)s, %(kills)s, %(deaths)s, %(assists)s, %(kd)s, %(kr)s, %(adr)s, %(hs_pct)s, %(mvps)s, %(sniper_kills)s,
      %(utility_damage)s, %(enemies_flashed)s, %(flash_count)s, %(flash_successes)s,
      %(mk_2k)s, %(mk_3k)s, %(mk_4k)s, %(mk_5k)s,
      %(clutch_kills)s, %(cl_1v1_attempts)s, %(cl_1v1_wins)s,
      %(cl_1v2_attempts)s, %(cl_1v2_wins)s, %(entry_count)s, %(entry_wins)s,
      %(pistol_kills)s, %(damage)s
    )
    ON DUPLICATE KEY UPDATE
      team_id = VALUES(team_id),
      opponent_team_id = VALUES(opponent_team_id),
      is_forfeit_map = VALUES(is_forfeit_map),
      kills = VALUES(kills),
      deaths = VALUES(deaths),
      assists = VALUES(assists),
      kd = VALUES(kd),
      kr = VALUES(kr),
      adr = VALUES(adr),
      hs_pct = VALUES(hs_pct),
      mvps = VALUES(mvps),
      sniper_kills = VALUES(sniper_kills),
      utility_damage = VALUES(utility_damage),
      enemies_flashed = VALUES(enemies_flashed),
      flash_count = VALUES(flash_count),
      flash_successes = VALUES(flash_successes),
      mk_2k = VALUES(mk_2k),
      mk_3k = VALUES(mk_3k),
      mk_4k = VALUES(mk_4k),
      mk_5k = VALUES(mk_5k),
      clutch_kills = VALUES(clutch_kills),
      cl_1v1_attempts = VALUES(cl_1v1_attempts),
      cl_1v1_wins = VALUES(cl_1v1_wins),
      cl_1v2_attempts = VALUES(cl_1v2_attempts),
      cl_1v2_wins = VALUES(cl_1v2_wins),
      entry_count = VALUES(entry_count),
      entry_wins = VALUES(entry_wins),
      pistol_kills = VALUES(pistol_kills),
      damage = VALUES(damage)
"""

_TEAM_STAT_UPSERT_SQL = """
    INSERT INTO team_stats (
      season, division_num, match_id, round_index, team_id, opponent_team_id,
      map_id, is_forfeit_map, final_score, first_half_score, second_half_score,
      overtime_score, headshot_pct, win
    )
    VALUES (
      %(season)s, %(division_num)s, %(match_id)s, %(round_index)s, %(team_id)s, %(opponent_team_id)s,
      %(map_id)s, %(is_forfeit_map)s, %(final_score)s, %(first_half_score)s, %(second_half_score)s,
      %(overtime_score)s, %(headshot_pct)s, %(win)s
    )
    ON DUPLICATE KEY UPDATE
      opponent_team_id = VALUES(opponent_team_id),
      map_id = VALUES(map_id),
      is_forfeit_map = VALUES(is_forfeit_map),
      final_score = VALUES(final_score),
      first_half_score = VALUES(first_half_score),
      second_half_score = VALUES(second_half_score),
      overtime_score = VALUES(overtime_score),
      headshot_pct = VALUES(headshot_pct),
      win = VALUES(win)
"""


def _normalize_avatar(url: Optional[str]) -> str:
    raw = (url or "").strip()
    return raw or DEFAULT_TEAM_AVATAR


def _normalise_map_key(name: Optional[str], fallback: Optional[int] = None) -> str:
    trimmed = (name or "").strip()
    if trimmed:
        return trimmed
    if fallback is not None:
        return f"map_{fallback}"
    return "unknown"


async def _calc_snapshot_ts_async(conn: asyncmy.Connection, season: int, division_num: int) -> int:
    """Return the max Faceit timestamp for the given season/division."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT COALESCE(MAX({_TS_EXPR}), 0)
            FROM matches m
            WHERE m.season = %s AND m.division_num = %s
            """,
            (season, division_num),
        )
        row = await cur.fetchone()
    snapshot_ts = int(row[0] or 0)
    if snapshot_ts <= 0:
        snapshot_ts = int(time.time())
    return snapshot_ts


async def get_division_snapshot_ts_async(conn: asyncmy.Connection, season: int, division_num: int) -> int:
    """Public helper for retrieving the division snapshot timestamp."""
    return await _calc_snapshot_ts_async(conn, season, division_num)


def _prepare_championship_payload(row: Row) -> Optional[Dict[str, Any]]:
    cid = row.get("championship_id")
    if not cid:
        return None
    return {
        "championship_id": cid,
        "season": row.get("season"),
        "division_num": row.get("division_num"),
        "name": row.get("name"),
        "is_playoffs": 1 if row.get("is_playoffs") else 0,
        "slug": row.get("slug"),
        "parent_championship_id": row.get("parent_championship_id"),
        "winner_team_id": row.get("winner_team_id"),
    }


async def upsert_championships_async(conn: asyncmy.Connection, rows: Iterable[Row]) -> list[str]:
    payloads = []
    for row in rows:
        payload = _prepare_championship_payload(row)
        if payload:
            payloads.append(payload)
    if not payloads:
        return []
    async with conn.cursor() as cur:
        await cur.executemany(_CHAMPIONSHIP_UPSERT_SQL, payloads)
    return [str(item["championship_id"]) for item in payloads]


async def upsert_championship_async(conn: asyncmy.Connection, row: Row) -> str:
    ids = await upsert_championships_async(conn, [row])
    return ids[0] if ids else ""


def _prepare_team_payload(team: Row) -> Optional[Dict[str, Any]]:
    tid = team.get("team_id")
    if not tid:
        return None
    return {
        "team_id": tid,
        "name": team.get("name"),
        "avatar": _normalize_avatar(team.get("avatar")),
    }


async def upsert_teams_bulk_async(
    teams: Iterable[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    payloads = []
    for team in teams:
        payload = _prepare_team_payload(team)
        if payload:
            payloads.append(payload)
    if not payloads:
        return

    op_label = label or "upsert-teams"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.executemany(_TEAM_UPSERT_SQL, payloads)

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_team_async(team: Row, *, conn: asyncmy.Connection | None = None, label: str | None = None) -> None:
    await upsert_teams_bulk_async([team], conn=conn, label=label)


async def upsert_player_async(conn: asyncmy.Connection, player: Row) -> None:
    nickname = (player.get("nickname") or player.get("name") or "").strip()
    payload = {
        "player_id": player.get("player_id") or player.get("id"),
        "nickname": nickname,
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO players (player_id, nickname)
            VALUES (%(player_id)s, %(nickname)s)
            ON DUPLICATE KEY UPDATE nickname = CASE WHEN players.nickname = '' THEN VALUES(nickname) ELSE players.nickname END
            """,
            payload,
        )


async def upsert_players_bulk_async(
    players: Iterable[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    rows = [
        {
            "player_id": p.get("player_id") or p.get("id"),
            "nickname": (p.get("nickname") or p.get("name") or "").strip(),
        }
        for p in players
        if p.get("player_id") or p.get("id")
    ]
    if not rows:
        return

    op_label = label or "upsert-players"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO players (player_id, nickname)
                    VALUES (%(player_id)s, %(nickname)s)
                    ON DUPLICATE KEY UPDATE nickname = CASE WHEN players.nickname = '' THEN VALUES(nickname) ELSE players.nickname END
                    """,
                    rows,
                )

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_match_async(conn: asyncmy.Connection, row: Row) -> None:
    times = [
        row.get("finished_at"),
        row.get("started_at"),
        row.get("scheduled_at"),
        row.get("last_seen_at"),
        row.get("configured_at"),
    ]
    activity_ts = row.get("activity_ts")
    if activity_ts is None:
        activity_ts = max((int(v) for v in times if v), default=0)
    payload = {
        "match_id": row["match_id"],
        "championship_id": row["championship_id"],
        "season": row["season"],
        "division_num": row["division_num"],
        "best_of": row.get("best_of"),
        "configured_at": row.get("configured_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "scheduled_at": row.get("scheduled_at"),
        "status": row.get("status"),
        "last_seen_at": row.get("last_seen_at"),
        "activity_ts": activity_ts,
        "team1_id": row.get("team1_id"),
        "team2_id": row.get("team2_id"),
        "winner_team_id": row.get("winner_team_id"),
        "is_forfeit": 1 if row.get("is_forfeit") else 0,
        "ignored_due_ban": 1 if row.get("ignored_due_ban") else 0,
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO matches (
              match_id, championship_id, season, division_num, best_of,
              configured_at, started_at, finished_at, scheduled_at,
              status, last_seen_at, activity_ts, team1_id, team2_id, winner_team_id,
              is_forfeit, ignored_due_ban
            )
            VALUES (
              %(match_id)s, %(championship_id)s, %(season)s, %(division_num)s, %(best_of)s,
              %(configured_at)s, %(started_at)s, %(finished_at)s, %(scheduled_at)s,
              %(status)s, %(last_seen_at)s, %(activity_ts)s, %(team1_id)s, %(team2_id)s, %(winner_team_id)s,
              %(is_forfeit)s, %(ignored_due_ban)s
            )
            ON DUPLICATE KEY UPDATE
              championship_id = VALUES(championship_id),
              season = VALUES(season),
              division_num = VALUES(division_num),
              best_of = VALUES(best_of),
              configured_at = VALUES(configured_at),
              started_at = VALUES(started_at),
              finished_at = VALUES(finished_at),
              scheduled_at = VALUES(scheduled_at),
              status = VALUES(status),
              last_seen_at = VALUES(last_seen_at),
              activity_ts = VALUES(activity_ts),
              team1_id = VALUES(team1_id),
              team2_id = VALUES(team2_id),
              winner_team_id = VALUES(winner_team_id),
              is_forfeit = VALUES(is_forfeit),
              ignored_due_ban = VALUES(ignored_due_ban)
            """,
            payload,
        )


def _prepare_map_payload(match_id: str, season: int, division_num: int, row: Row) -> Dict[str, Any]:
    return {
        "match_id": match_id,
        "season": season,
        "division_num": division_num,
        "round_index": row["round_index"],
        "map_name": row.get("map_name"),
        "score_team1": row.get("score_team1"),
        "score_team2": row.get("score_team2"),
        "winner_team_id": row.get("winner_team_id"),
        "is_forfeit": 1 if row.get("is_forfeit") else 0,
    }


async def upsert_maps_bulk_async(
    match_id: str,
    season: int,
    division_num: int,
    rows: Iterable[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    payloads = [
        _prepare_map_payload(match_id, season, division_num, row)
        for row in rows
        if "round_index" in row
    ]
    if not payloads:
        return

    op_label = label or f"upsert-maps:{match_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.executemany(_MAP_UPSERT_SQL, payloads)

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_map_async(
    match_id: str,
    season: int,
    division_num: int,
    row: Row,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    await upsert_maps_bulk_async(
        match_id,
        season,
        division_num,
        [row],
        conn=conn,
        label=label,
    )


async def get_map_id_lookup_async(conn: asyncmy.Connection, match_id: str) -> Dict[int, int]:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT round_index, map_id FROM maps WHERE match_id = %s",
            (match_id,),
        )
        pairs = await cur.fetchall()
        return {int(round_index): int(map_id) for round_index, map_id in pairs}


async def replace_map_votes_async(
    match_id: str,
    season: int,
    division_num: int,
    votes: Iterable[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    op_label = label or f"replace-map-votes:{match_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.execute("DELETE FROM map_votes WHERE match_id = %s", (match_id,))
                rows = [
                    {
                        "match_id": match_id,
                        "season": season,
                        "division_num": division_num,
                        "map_name": vote.get("map_name"),
                        "status": vote.get("status"),
                        "selected_by_faction": vote.get("selected_by_faction"),
                        "round_num": vote.get("round_num"),
                        "selected_by_team_id": vote.get("selected_by_team_id"),
                    }
                    for vote in votes
                ]
                if rows:
                    await cur.executemany(
                        """
                        INSERT INTO map_votes (
                          match_id, season, division_num, map_name, status,
                          selected_by_faction, round_num, selected_by_team_id
                        )
                        VALUES (
                          %(match_id)s, %(season)s, %(division_num)s, %(map_name)s, %(status)s,
                          %(selected_by_faction)s, %(round_num)s, %(selected_by_team_id)s
                        )
                        """,
                        rows,
                    )

    await _retry_on_deadlock(_operation, label=op_label)


def _prepare_player_stat_payload(
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Mapping[int, int],
    row: Row,
    is_forfeit_map: bool,
) -> Dict[str, Any]:
    map_id = map_lookup.get(round_index)
    return {
        "season": season,
        "division_num": division_num,
        "match_id": match_id,
        "round_index": round_index,
        "map_id": map_id,
        "player_id": row.get("player_id"),
        "team_id": row.get("team_id"),
        "opponent_team_id": row.get("opponent_team_id"),
        "is_forfeit_map": 1 if is_forfeit_map else 0,
        "kills": row.get("kills"),
        "deaths": row.get("deaths"),
        "assists": row.get("assists"),
        "kd": row.get("kd"),
        "kr": row.get("kr"),
        "adr": row.get("adr"),
        "hs_pct": row.get("hs_pct"),
        "mvps": row.get("mvps"),
        "sniper_kills": row.get("sniper_kills"),
        "utility_damage": row.get("utility_damage"),
        "enemies_flashed": row.get("enemies_flashed"),
        "flash_count": row.get("flash_count"),
        "flash_successes": row.get("flash_successes"),
        "mk_2k": row.get("mk_2k"),
        "mk_3k": row.get("mk_3k"),
        "mk_4k": row.get("mk_4k"),
        "mk_5k": row.get("mk_5k"),
        "clutch_kills": row.get("clutch_kills"),
        "cl_1v1_attempts": row.get("cl_1v1_attempts"),
        "cl_1v1_wins": row.get("cl_1v1_wins"),
        "cl_1v2_attempts": row.get("cl_1v2_attempts"),
        "cl_1v2_wins": row.get("cl_1v2_wins"),
        "entry_count": row.get("entry_count"),
        "entry_wins": row.get("entry_wins"),
        "pistol_kills": row.get("pistol_kills"),
        "damage": row.get("damage"),
    }


async def upsert_player_stats_bulk_async(
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    rows: Iterable[Row],
    forfeit_lookup: Mapping[int, bool],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    payloads = []
    for row in rows:
        round_index = row.get("round_index")
        if round_index is None:
            continue
        payloads.append(
            _prepare_player_stat_payload(
                season,
                division_num,
                match_id,
                int(round_index),
                map_lookup,
                row,
                bool(forfeit_lookup.get(int(round_index), False)),
            )
        )
    if not payloads:
        return

    op_label = label or f"player-stats:{match_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.executemany(_PLAYER_STAT_UPSERT_SQL, payloads)

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_player_stat_async(
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Dict[int, int],
    row: Row,
    is_forfeit_map: bool,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    await upsert_player_stats_bulk_async(
        season=season,
        division_num=division_num,
        match_id=match_id,
        map_lookup=map_lookup,
        rows=[dict(row, round_index=round_index)],
        forfeit_lookup={round_index: is_forfeit_map},
        conn=conn,
        label=label,
    )


def _prepare_team_stat_payload(
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Mapping[int, int],
    row: Row,
    is_forfeit_map: bool,
) -> Dict[str, Any]:
    map_id = map_lookup.get(round_index)
    return {
        "season": season,
        "division_num": division_num,
        "match_id": match_id,
        "round_index": round_index,
        "map_id": map_id,
        "team_id": row.get("team_id"),
        "opponent_team_id": row.get("opponent_team_id"),
        "is_forfeit_map": 1 if is_forfeit_map else 0,
        "final_score": row.get("final_score"),
        "first_half_score": row.get("first_half_score"),
        "second_half_score": row.get("second_half_score"),
        "overtime_score": row.get("overtime_score"),
        "headshot_pct": row.get("headshot_pct"),
        "win": 1 if row.get("win") else 0,
    }


async def upsert_team_stats_bulk_async(
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    rows: Iterable[Row],
    forfeit_lookup: Mapping[int, bool],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    payloads = []
    for row in rows:
        round_index = row.get("round_index")
        if round_index is None:
            continue
        payloads.append(
            _prepare_team_stat_payload(
                season,
                division_num,
                match_id,
                int(round_index),
                map_lookup,
                row,
                bool(forfeit_lookup.get(int(round_index), False)),
            )
        )
    if not payloads:
        return

    op_label = label or f"team-stats:{match_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            async with active_conn.cursor() as cur:
                await cur.executemany(_TEAM_STAT_UPSERT_SQL, payloads)

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_team_stat_async(
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Dict[int, int],
    row: Row,
    is_forfeit_map: bool,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    await upsert_team_stats_bulk_async(
        season=season,
        division_num=division_num,
        match_id=match_id,
        map_lookup=map_lookup,
        rows=[dict(row, round_index=round_index)],
        forfeit_lookup={round_index: is_forfeit_map},
        conn=conn,
        label=label,
    )


async def delete_stats_for_match_async(
    match_id: str,
    snapshot_ts: Optional[int] = None,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    op_label = label or f"delete-stats:{match_id}"
    async with _write_connection(conn, label=op_label) as active_conn:
        effective_snapshot = snapshot_ts
        if effective_snapshot is None:
            async with active_conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT {_TS_EXPR}
                    FROM matches m
                    WHERE m.match_id = %s
                    """,
                    (match_id,),
                )
                row = await cur.fetchone()
            effective_snapshot = int((row or (0,))[0] or 0)
            if effective_snapshot <= 0:
                effective_snapshot = int(time.time())

        async with active_conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO team_stats_prev (
                    season,
                    division_num,
                    match_id,
                    round_index,
                    map_id,
                    team_id,
                    opponent_team_id,
                    is_forfeit_map,
                    final_score,
                    first_half_score,
                    second_half_score,
                    overtime_score,
                    headshot_pct,
                    win,
                    snapshot_ts
                )
                SELECT
                    season,
                    division_num,
                    match_id,
                    round_index,
                    map_id,
                    team_id,
                    opponent_team_id,
                    is_forfeit_map,
                    final_score,
                    first_half_score,
                    second_half_score,
                    overtime_score,
                    headshot_pct,
                    win,
                    %s AS snapshot_ts
                FROM team_stats
                WHERE match_id = %s
                """,
                (effective_snapshot, match_id),
            )
            await cur.execute("DELETE FROM player_stats WHERE match_id = %s", (match_id,))
            await cur.execute("DELETE FROM team_stats WHERE match_id = %s", (match_id,))


async def clear_obsolete_maps_async(
    match_id: str,
    round_indices: Sequence[int],
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    if not round_indices:
        return
    placeholders = ",".join(["%s"] * len(round_indices))
    args = [match_id, *round_indices]
    op_label = label or f"clear-maps:{match_id}"
    async with _write_connection(conn, label=op_label) as active_conn:
        async with active_conn.cursor() as cur:
            await cur.execute(
                f"DELETE FROM maps WHERE match_id = %s AND round_index NOT IN ({placeholders})",
                args,
            )


async def upsert_player_season_totals_async(
    season: int,
    division_num: int,
    player_id: str,
    snapshot_ts: Optional[int] = None,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            effective_snapshot = snapshot_ts or await _calc_snapshot_ts_async(active_conn, season, division_num)
            async with active_conn.cursor(cursors.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT
                      MAX(ps.team_id) AS team_id,
                      SUM(CASE WHEN ps.is_forfeit_map = 0 THEN 1 ELSE 0 END) AS maps_played,
                      SUM(
                          CASE
                              WHEN ps.is_forfeit_map = 0
                              THEN (COALESCE(m.score_team1, 0) + COALESCE(m.score_team2, 0))
                              ELSE 0
                          END
                      ) AS rounds_played,
                      SUM(ps.kills) AS kills,
                      SUM(ps.deaths) AS deaths,
                      SUM(ps.assists) AS assists,
                      COALESCE(AVG(ps.adr), 0) AS adr,
                      COALESCE(AVG(ps.kr), 0) AS kr,
                      COALESCE(AVG(ps.hs_pct), 0) AS hs_pct,
                      SUM(ps.mvps) AS mvps,
                      SUM(ps.sniper_kills) AS sniper_kills,
                      SUM(ps.utility_damage) AS utility_damage,
                      SUM(ps.enemies_flashed) AS enemies_flashed,
                      SUM(ps.flash_count) AS flash_count,
                      SUM(ps.flash_successes) AS flash_successes,
                      SUM(ps.mk_2k) AS mk_2k,
                      SUM(ps.mk_3k) AS mk_3k,
                      SUM(ps.mk_4k) AS mk_4k,
                      SUM(ps.mk_5k) AS mk_5k,
                      SUM(ps.clutch_kills) AS clutch_kills,
                      SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
                      SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
                      SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
                      SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
                      SUM(ps.entry_count) AS entry_count,
                      SUM(ps.entry_wins) AS entry_wins,
                      SUM(ps.pistol_kills) AS pistol_kills,
                      SUM(ps.damage) AS damage
                    FROM player_stats ps
                    JOIN matches mt ON mt.match_id = ps.match_id
                    LEFT JOIN maps m
                      ON m.match_id = ps.match_id
                     AND m.round_index = ps.round_index
                    WHERE ps.season = %s
                      AND ps.division_num = %s
                      AND ps.player_id = %s
                      AND mt.ignored_due_ban = 0
                    """,
                    (season, division_num, player_id),
                )
                stats = await cur.fetchone()

                if not stats:
                    return

                team_id = stats["team_id"]
                maps_played = stats["maps_played"] or 0
                rounds_played = stats["rounds_played"] or 0
                kills = stats["kills"] or 0
                deaths = stats["deaths"] or 0
                assists = stats["assists"] or 0
                adr = float(stats["adr"] or 0.0)
                kr = float(stats["kr"] or 0.0)
                hs_pct = float(stats["hs_pct"] or 0.0)
                mvps = stats["mvps"] or 0
                sniper_kills = stats["sniper_kills"] or 0
                utility_damage = stats["utility_damage"] or 0
                enemies_flashed = stats["enemies_flashed"] or 0
                flash_count = stats["flash_count"] or 0
                flash_successes = stats["flash_successes"] or 0
                mk_2k = stats["mk_2k"] or 0
                mk_3k = stats["mk_3k"] or 0
                mk_4k = stats["mk_4k"] or 0
                mk_5k = stats["mk_5k"] or 0
                clutch_kills = stats["clutch_kills"] or 0
                cl_1v1_attempts = stats["cl_1v1_attempts"] or 0
                cl_1v1_wins = stats["cl_1v1_wins"] or 0
                cl_1v2_attempts = stats["cl_1v2_attempts"] or 0
                cl_1v2_wins = stats["cl_1v2_wins"] or 0
                entry_count = stats["entry_count"] or 0
                entry_wins = stats["entry_wins"] or 0
                pistol_kills = stats["pistol_kills"] or 0
                damage = stats["damage"] or 0

                kd = float(kills) / float(deaths) if deaths else float(kills)
                rating = (float(kr) + float(adr) / 100.0) / 2.0 if maps_played else 0.0

                # Atomically snapshot current state to _prev table (if exists)
                # FOR UPDATE lock prevents concurrent modifications during snapshot
                await cur.execute(
                    """
                    INSERT INTO player_season_totals_prev (
                      season,
                      division_num,
                      player_id,
                      team_id,
                      maps_played,
                      rounds_played,
                      kills,
                      deaths,
                      assists,
                      mvps,
                      sniper_kills,
                      utility_damage,
                      enemies_flashed,
                      flash_count,
                      flash_successes,
                      mk_2k,
                      mk_3k,
                      mk_4k,
                      mk_5k,
                      clutch_kills,
                      cl_1v1_attempts,
                      cl_1v1_wins,
                      cl_1v2_attempts,
                      cl_1v2_wins,
                      entry_count,
                      entry_wins,
                      pistol_kills,
                      adr,
                      kr,
                      kd,
                      rating,
                      hs_pct,
                      damage,
                      snapshot_ts,
                      created_at
                    )
                    SELECT
                      season,
                      division_num,
                      player_id,
                      team_id,
                      maps_played,
                      rounds_played,
                      kills,
                      deaths,
                      assists,
                      mvps,
                      sniper_kills,
                      utility_damage,
                      enemies_flashed,
                      flash_count,
                      flash_successes,
                      mk_2k,
                      mk_3k,
                      mk_4k,
                      mk_5k,
                      clutch_kills,
                      cl_1v1_attempts,
                      cl_1v1_wins,
                      cl_1v2_attempts,
                      cl_1v2_wins,
                      entry_count,
                      entry_wins,
                      pistol_kills,
                      adr,
                      kr,
                      kd,
                      rating,
                      hs_pct,
                      damage,
                      %s as snapshot_ts,
                      CURRENT_TIMESTAMP as created_at
                    FROM player_season_totals
                    WHERE season = %s
                      AND division_num = %s
                      AND player_id = %s
                    FOR UPDATE
                    ON DUPLICATE KEY UPDATE
                      team_id = VALUES(team_id),
                      maps_played = VALUES(maps_played),
                      rounds_played = VALUES(rounds_played),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      assists = VALUES(assists),
                      mvps = VALUES(mvps),
                      sniper_kills = VALUES(sniper_kills),
                      utility_damage = VALUES(utility_damage),
                      enemies_flashed = VALUES(enemies_flashed),
                      flash_count = VALUES(flash_count),
                      flash_successes = VALUES(flash_successes),
                      mk_2k = VALUES(mk_2k),
                      mk_3k = VALUES(mk_3k),
                      mk_4k = VALUES(mk_4k),
                      mk_5k = VALUES(mk_5k),
                      clutch_kills = VALUES(clutch_kills),
                      cl_1v1_attempts = VALUES(cl_1v1_attempts),
                      cl_1v1_wins = VALUES(cl_1v1_wins),
                      cl_1v2_attempts = VALUES(cl_1v2_attempts),
                      cl_1v2_wins = VALUES(cl_1v2_wins),
                      entry_count = VALUES(entry_count),
                      entry_wins = VALUES(entry_wins),
                      pistol_kills = VALUES(pistol_kills),
                      adr = VALUES(adr),
                      kr = VALUES(kr),
                      kd = VALUES(kd),
                      rating = VALUES(rating),
                      hs_pct = VALUES(hs_pct),
                      damage = VALUES(damage),
                      snapshot_ts = VALUES(snapshot_ts),
                      created_at = CURRENT_TIMESTAMP
                    """,
                    (snapshot_ts, season, division_num, player_id),
                )

                await cur.execute(
            """
            INSERT INTO player_season_totals (
              season,
              division_num,
              player_id,
              team_id,
              maps_played,
              rounds_played,
              kills,
              deaths,
              assists,
              adr,
              kr,
              kd,
              rating,
              hs_pct,
              mvps,
              sniper_kills,
              utility_damage,
              enemies_flashed,
              flash_count,
              flash_successes,
              mk_2k,
              mk_3k,
              mk_4k,
              mk_5k,
              clutch_kills,
              cl_1v1_attempts,
              cl_1v1_wins,
              cl_1v2_attempts,
              cl_1v2_wins,
              entry_count,
              entry_wins,
              pistol_kills,
              damage
            )
            VALUES (
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
              team_id = VALUES(team_id),
              maps_played = VALUES(maps_played),
              rounds_played = VALUES(rounds_played),
              kills = VALUES(kills),
              deaths = VALUES(deaths),
              assists = VALUES(assists),
              adr = VALUES(adr),
              kr = VALUES(kr),
              kd = VALUES(kd),
              rating = VALUES(rating),
              hs_pct = VALUES(hs_pct),
              mvps = VALUES(mvps),
              sniper_kills = VALUES(sniper_kills),
              utility_damage = VALUES(utility_damage),
              enemies_flashed = VALUES(enemies_flashed),
              flash_count = VALUES(flash_count),
              flash_successes = VALUES(flash_successes),
              mk_2k = VALUES(mk_2k),
              mk_3k = VALUES(mk_3k),
              mk_4k = VALUES(mk_4k),
              mk_5k = VALUES(mk_5k),
              clutch_kills = VALUES(clutch_kills),
              cl_1v1_attempts = VALUES(cl_1v1_attempts),
              cl_1v1_wins = VALUES(cl_1v1_wins),
              cl_1v2_attempts = VALUES(cl_1v2_attempts),
              cl_1v2_wins = VALUES(cl_1v2_wins),
              entry_count = VALUES(entry_count),
              entry_wins = VALUES(entry_wins),
              pistol_kills = VALUES(pistol_kills),
              damage = VALUES(damage)
            """,
            (
                season,
                division_num,
                player_id,
                team_id,
                maps_played,
                rounds_played,
                kills,
                deaths,
                assists,
                adr,
                kr,
                kd,
                rating,
                hs_pct,
                mvps,
                sniper_kills,
                utility_damage,
                enemies_flashed,
                flash_count,
                flash_successes,
                mk_2k,
                mk_3k,
                mk_4k,
                mk_5k,
                clutch_kills,
                cl_1v1_attempts,
                cl_1v1_wins,
                cl_1v2_attempts,
                cl_1v2_wins,
                entry_count,
                entry_wins,
                pistol_kills,
                damage,
            ),
            )

    op_label = label or f"player-season:{player_id}"
    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_player_map_season_totals_async(
    season: int,
    division_num: int,
    player_id: str,
    snapshot_ts: Optional[int] = None,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    op_label = label or f"player-map-season:{player_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            effective_snapshot = snapshot_ts or await _calc_snapshot_ts_async(active_conn, season, division_num)
            async with active_conn.cursor(cursors.DictCursor) as cur:
                await cur.execute(
                    """
                    INSERT INTO player_map_season_totals_prev (
                      season,
                      division_num,
                      player_id,
                      team_id,
                      map_name,
                      maps_played,
                      rounds_played,
                      kills,
                      deaths,
                      assists,
                      sniper_kills,
                      utility_damage,
                      enemies_flashed,
                      flash_count,
                      flash_successes,
                      mk_2k,
                      mk_3k,
                      mk_4k,
                      mk_5k,
                      entry_count,
                      entry_wins,
                      pistol_kills,
                      clutch_kills,
                      cl_1v1_attempts,
                      cl_1v1_wins,
                      cl_1v2_attempts,
                      cl_1v2_wins,
                      adr,
                      kr,
                      kd,
                      hs_pct,
                      mvps,
                      damage,
                      snapshot_ts,
                      created_at
                    )
                    SELECT
                      season,
                      division_num,
                      player_id,
                      team_id,
                      map_name,
                      maps_played,
                      rounds_played,
                      kills,
                      deaths,
                      assists,
                      sniper_kills,
                      utility_damage,
                      enemies_flashed,
                      flash_count,
                      flash_successes,
                      mk_2k,
                      mk_3k,
                      mk_4k,
                      mk_5k,
                      entry_count,
                      entry_wins,
                      pistol_kills,
                      clutch_kills,
                      cl_1v1_attempts,
                      cl_1v1_wins,
                      cl_1v2_attempts,
                      cl_1v2_wins,
                      adr,
                      kr,
                      kd,
                      hs_pct,
                      mvps,
                      damage,
                      %s as snapshot_ts,
                      CURRENT_TIMESTAMP as created_at
                    FROM player_map_season_totals
                    WHERE season = %s
                      AND division_num = %s
                      AND player_id = %s
                    FOR UPDATE
                    ON DUPLICATE KEY UPDATE
                      team_id = VALUES(team_id),
                      maps_played = VALUES(maps_played),
                      rounds_played = VALUES(rounds_played),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      assists = VALUES(assists),
                      sniper_kills = VALUES(sniper_kills),
                      utility_damage = VALUES(utility_damage),
                      enemies_flashed = VALUES(enemies_flashed),
                      flash_count = VALUES(flash_count),
                      flash_successes = VALUES(flash_successes),
                      mk_2k = VALUES(mk_2k),
                      mk_3k = VALUES(mk_3k),
                      mk_4k = VALUES(mk_4k),
                      mk_5k = VALUES(mk_5k),
                      entry_count = VALUES(entry_count),
                      entry_wins = VALUES(entry_wins),
                      pistol_kills = VALUES(pistol_kills),
                      clutch_kills = VALUES(clutch_kills),
                      cl_1v1_attempts = VALUES(cl_1v1_attempts),
                      cl_1v1_wins = VALUES(cl_1v1_wins),
                      cl_1v2_attempts = VALUES(cl_1v2_attempts),
                      cl_1v2_wins = VALUES(cl_1v2_wins),
                      adr = VALUES(adr),
                      kr = VALUES(kr),
                      kd = VALUES(kd),
                      hs_pct = VALUES(hs_pct),
                      mvps = VALUES(mvps),
                      damage = VALUES(damage)
                    """,
                    (
                        effective_snapshot,
                        season,
                        division_num,
                        player_id,
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO player_map_season_totals (
                      season,
                      division_num,
                      player_id,
                      team_id,
                      map_name,
                      maps_played,
                      rounds_played,
                      kills,
                      deaths,
                      assists,
                      sniper_kills,
                      utility_damage,
                      enemies_flashed,
                      flash_count,
                      flash_successes,
                      mk_2k,
                      mk_3k,
                      mk_4k,
                      mk_5k,
                      entry_count,
                      entry_wins,
                      pistol_kills,
                      clutch_kills,
                      cl_1v1_attempts,
                      cl_1v1_wins,
                      cl_1v2_attempts,
                      cl_1v2_wins,
                      adr,
                      kr,
                      kd,
                      hs_pct,
                      mvps,
                      damage
                    )
                    SELECT
                      ps.season,
                      ps.division_num,
                      ps.player_id,
                      ps.team_id,
                      COALESCE(
                        NULLIF(TRIM(mp.map_name), ''),
                        CONCAT('map_', COALESCE(mp.round_index, ps.round_index))
                      ) AS map_name,
                      COUNT(*) AS maps_played,
                      SUM(
                        COALESCE(mp.score_team1 + mp.score_team2, 0)
                      ) AS rounds_played,
                      SUM(ps.kills) AS kills,
                      SUM(ps.deaths) AS deaths,
                      SUM(ps.assists) AS assists,
                      SUM(ps.sniper_kills) AS sniper_kills,
                      SUM(ps.utility_damage) AS utility_damage,
                      SUM(ps.enemies_flashed) AS enemies_flashed,
                      SUM(ps.flash_count) AS flash_count,
                      SUM(ps.flash_successes) AS flash_successes,
                      SUM(ps.mk_2k) AS mk_2k,
                      SUM(ps.mk_3k) AS mk_3k,
                      SUM(ps.mk_4k) AS mk_4k,
                      SUM(ps.mk_5k) AS mk_5k,
                      SUM(ps.entry_count) AS entry_count,
                      SUM(ps.entry_wins) AS entry_wins,
                      SUM(ps.pistol_kills) AS pistol_kills,
                      SUM(ps.clutch_kills) AS clutch_kills,
                      SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
                      SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
                      SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
                      SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
                      COALESCE(AVG(ps.adr), 0) AS adr,
                      COALESCE(AVG(ps.kr), 0) AS kr,
                      COALESCE(AVG(ps.kd), 0) AS kd,
                      COALESCE(AVG(ps.hs_pct), 0) AS hs_pct,
                      AVG(ps.mvps) AS mvps,
                      SUM(ps.damage) AS damage
                    FROM player_stats ps
                    LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.map_id = ps.map_id
                    WHERE ps.season = %s
                      AND ps.division_num = %s
                      AND ps.player_id = %s
                      AND ps.is_forfeit_map = 0
                    GROUP BY
                      ps.season,
                      ps.division_num,
                      ps.player_id,
                      ps.team_id,
                      COALESCE(
                        NULLIF(TRIM(mp.map_name), ''),
                        CONCAT('map_', COALESCE(mp.round_index, ps.round_index))
                      )
                    ON DUPLICATE KEY UPDATE
                      team_id = VALUES(team_id),
                      maps_played = VALUES(maps_played),
                      rounds_played = VALUES(rounds_played),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      assists = VALUES(assists),
                      sniper_kills = VALUES(sniper_kills),
                      utility_damage = VALUES(utility_damage),
                      enemies_flashed = VALUES(enemies_flashed),
                      flash_count = VALUES(flash_count),
                      flash_successes = VALUES(flash_successes),
                      mk_2k = VALUES(mk_2k),
                      mk_3k = VALUES(mk_3k),
                      mk_4k = VALUES(mk_4k),
                      mk_5k = VALUES(mk_5k),
                      entry_count = VALUES(entry_count),
                      entry_wins = VALUES(entry_wins),
                      pistol_kills = VALUES(pistol_kills),
                      clutch_kills = VALUES(clutch_kills),
                      cl_1v1_attempts = VALUES(cl_1v1_attempts),
                      cl_1v1_wins = VALUES(cl_1v1_wins),
                      cl_1v2_attempts = VALUES(cl_1v2_attempts),
                      cl_1v2_wins = VALUES(cl_1v2_wins),
                      adr = VALUES(adr),
                      kr = VALUES(kr),
                      kd = VALUES(kd),
                      hs_pct = VALUES(hs_pct),
                      mvps = VALUES(mvps),
                      damage = VALUES(damage)
                    """,
                    (
                        season,
                        division_num,
                        player_id,
                    ),
                )

    await _retry_on_deadlock(_operation, label=op_label)


async def upsert_team_season_totals_async(
    season: int,
    division_num: int,
    team_id: str,
    snapshot_ts: Optional[int] = None,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    if not team_id:
        return

    op_label = label or f"team-season:{team_id}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            effective_snapshot = snapshot_ts or await _calc_snapshot_ts_async(active_conn, season, division_num)
            async with active_conn.cursor(cursors.DictCursor) as cur:
                await cur.execute(
                """
                SELECT
                    COUNT(DISTINCT CASE
                        WHEN m.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND (m.team1_id = %s OR m.team2_id = %s)
                        THEN m.match_id END
                    ) AS matches_played,
                    COUNT(DISTINCT CASE
                        WHEN m.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND m.winner_team_id = %s
                         AND (m.team1_id = %s OR m.team2_id = %s)
                        THEN m.match_id END
                    ) AS matches_won,
                    SUM(CASE
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND (m.team1_id = %s OR m.team2_id = %s)
                        THEN 1 ELSE 0 END
                    ) AS maps_played,
                    SUM(CASE
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND mp.winner_team_id = %s
                         AND (m.team1_id = %s OR m.team2_id = %s)
                        THEN 1 ELSE 0 END
                    ) AS maps_won,
                    SUM(CASE
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND m.team1_id = %s
                        THEN COALESCE(mp.score_team1, 0)
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND m.team2_id = %s
                        THEN COALESCE(mp.score_team2, 0)
                        ELSE 0 END
                    ) AS rounds_won,
                    SUM(CASE
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND m.team1_id = %s
                        THEN COALESCE(mp.score_team2, 0)
                        WHEN mp.is_forfeit = 0
                         AND m.ignored_due_ban = 0
                         AND m.team2_id = %s
                        THEN COALESCE(mp.score_team1, 0)
                        ELSE 0 END
                    ) AS rounds_lost
                FROM matches m
                LEFT JOIN maps mp ON mp.match_id = m.match_id
                WHERE m.season = %s
                  AND m.division_num = %s
                  AND (m.team1_id = %s OR m.team2_id = %s)
                """,
                (
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    team_id,
                    season,
                    division_num,
                    team_id,
                    team_id,
                ),
            )
                totals = await cur.fetchone()

                if not totals:
                    await cur.execute(
                        """
                        DELETE FROM team_season_totals
                        WHERE season = %s
                          AND division_num = %s
                          AND team_id = %s
                        """,
                        (season, division_num, team_id),
                    )
                    return

                await cur.execute(
                    """
                    INSERT INTO team_season_totals_prev (
                      season,
                      division_num,
                      team_id,
                      matches_played,
                      matches_won,
                      maps_played,
                      maps_won,
                      rounds_won,
                      rounds_lost,
                      snapshot_ts,
                      created_at
                    )
                    SELECT 
                      season,
                      division_num,
                      team_id,
                      matches_played,
                      matches_won,
                      maps_played,
                      maps_won,
                      rounds_won,
                      rounds_lost,
                      %s as snapshot_ts,
                      CURRENT_TIMESTAMP as created_at
                    FROM team_season_totals
                    WHERE season = %s
                      AND division_num = %s
                      AND team_id = %s
                    FOR UPDATE
                    ON DUPLICATE KEY UPDATE
                      matches_played = VALUES(matches_played),
                      matches_won = VALUES(matches_won),
                      maps_played = VALUES(maps_played),
                      maps_won = VALUES(maps_won),
                      rounds_won = VALUES(rounds_won),
                      rounds_lost = VALUES(rounds_lost),
                      snapshot_ts = VALUES(snapshot_ts),
                      created_at = CURRENT_TIMESTAMP
                    """,
                    (effective_snapshot, season, division_num, team_id),
                )

                await cur.execute(
                    """
                    INSERT INTO team_season_totals (
                      season,
                      division_num,
                      team_id,
                      matches_played,
                      matches_won,
                      maps_played,
                      maps_won,
                      rounds_won,
                      rounds_lost
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                      matches_played = VALUES(matches_played),
                      matches_won    = VALUES(matches_won),
                      maps_played    = VALUES(maps_played),
                      maps_won       = VALUES(maps_won),
                      rounds_won     = VALUES(rounds_won),
                      rounds_lost    = VALUES(rounds_lost)
                    """,
                    (
                        season,
                        division_num,
                        team_id,
                        totals["matches_played"] or 0,
                        totals["matches_won"] or 0,
                        totals["maps_played"] or 0,
                        totals["maps_won"] or 0,
                        totals["rounds_won"] or 0,
                        totals["rounds_lost"] or 0,
                    ),
                )

    await _retry_on_deadlock(_operation, label=op_label)



async def upsert_map_catalog_async(conn: asyncmy.Connection, row: Row) -> None:
    """Insert or update a maps_catalog entry.

    Expects a mapping with keys: map_id, pretty_name, image_sm, image_lg
    """
    payload = {
        "map_id": row.get("map_id"),
        "pretty_name": row.get("pretty_name") or row.get("map_id") or "",
        "image_sm": row.get("image_sm") or "",
        "image_lg": row.get("image_lg") or "",
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO maps_catalog (map_id, pretty_name, image_sm, image_lg)
            VALUES (%(map_id)s, %(pretty_name)s, %(image_sm)s, %(image_lg)s)
            ON DUPLICATE KEY UPDATE
              pretty_name = CASE WHEN maps_catalog.pretty_name = '' THEN VALUES(pretty_name) ELSE maps_catalog.pretty_name END,
              image_sm = CASE WHEN VALUES(image_sm) <> '' THEN VALUES(image_sm) ELSE maps_catalog.image_sm END,
              image_lg = CASE WHEN VALUES(image_lg) <> '' THEN VALUES(image_lg) ELSE maps_catalog.image_lg END,
              updated_at = CURRENT_TIMESTAMP
            """,
            payload,
        )


async def upsert_team_map_season_totals_async(
    season: int,
    division_num: int,
    team_id: str,
    map_name: Optional[str] = None,
    snapshot_ts: Optional[int] = None,
    *,
    conn: asyncmy.Connection | None = None,
    label: str | None = None,
) -> None:
    if not team_id:
        return

    op_label = label or f"team-map-season:{team_id}:{map_name or 'all'}"

    async def _operation() -> None:
        async with _write_connection(conn, label=op_label) as active_conn:
            effective_snapshot = snapshot_ts or await _calc_snapshot_ts_async(active_conn, season, division_num)
            async with active_conn.cursor(cursors.DictCursor) as cur:
                if map_name:
                    target_maps = [_normalise_map_key(map_name)]
                else:
                    await cur.execute(
                        """
                        SELECT DISTINCT
                            COALESCE(
                                NULLIF(TRIM(mp.map_name), ''),
                                CONCAT('map_', mp.round_index)
                            ) AS map_name
                        FROM matches m
                        JOIN maps mp ON mp.match_id = m.match_id
                        WHERE m.season = %s
                          AND m.division_num = %s
                          AND (m.team1_id = %s OR m.team2_id = %s)
                        """,
                        (season, division_num, team_id, team_id),
                    )
                    rows = await cur.fetchall()
                    target_maps = [row["map_name"] for row in rows or []]

                if not target_maps:
                    return

                for map_key in target_maps:
                    await cur.execute(
                        """
                        INSERT INTO team_map_season_totals_prev (
                            season,
                            division_num,
                            team_id,
                            map_name,
                            played,
                            picks,
                            opp_picks,
                            wins,
                            games,
                            ban1,
                            ban2,
                            opp_ban,
                            total_own_ban,
                            decov,
                            kills,
                            deaths,
                            mvps,
                            rd,
                            kd,
                            adr,
                            damage,
                            utility_damage,
                            snapshot_ts,
                            created_at
                        )
                        SELECT
                        season,
                        division_num,
                        team_id,
                        map_name,
                        played,
                        picks,
                        opp_picks,
                        wins,
                        games,
                        ban1,
                        ban2,
                        opp_ban,
                        total_own_ban,
                        decov,
                        kills,
                        deaths,
                        mvps,
                        rd,
                        kd,
                        adr,
                        damage,
                        utility_damage,
                        %s as snapshot_ts,
                        CURRENT_TIMESTAMP as created_at
                    FROM team_map_season_totals
                    WHERE season = %s
                      AND division_num = %s
                      AND team_id = %s
                      AND map_name = %s
                    FOR UPDATE
                    ON DUPLICATE KEY UPDATE
                        played = VALUES(played),
                        picks = VALUES(picks),
                        opp_picks = VALUES(opp_picks),
                        wins = VALUES(wins),
                        games = VALUES(games),
                        ban1 = VALUES(ban1),
                        ban2 = VALUES(ban2),
                        opp_ban = VALUES(opp_ban),
                        total_own_ban = VALUES(total_own_ban),
                        decov = VALUES(decov),
                        kills = VALUES(kills),
                        deaths = VALUES(deaths),
                        mvps = VALUES(mvps),
                        rd = VALUES(rd),
                        kd = VALUES(kd),
                        adr = VALUES(adr),
                        damage = VALUES(damage),
                        utility_damage = VALUES(utility_damage),
                        snapshot_ts = VALUES(snapshot_ts),
                        created_at = CURRENT_TIMESTAMP
                    """,
                    (effective_snapshot, season, division_num, team_id, map_key),
                )

                    await cur.execute(
                        """
                        INSERT INTO team_map_season_totals (
                            season,
                            division_num,
                            team_id,
                            map_name,
                            played,
                            picks,
                            opp_picks,
                            wins,
                            games,
                            ban1,
                            ban2,
                            opp_ban,
                            total_own_ban,
                            decov,
                            kills,
                            deaths,
                            mvps,
                            rd,
                            kd,
                            adr,
                            damage,
                            utility_damage
                        )
                        SELECT
                            %s AS season,
                            %s AS division_num,
                            %s AS team_id,
                            %s AS map_name,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                 AND (m.team1_id = %s OR m.team2_id = %s)
                                THEN 1 ELSE 0 END
                            ) AS played,
                            SUM(CASE
                                WHEN m.team1_id = %s
                                 AND mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN 1 ELSE 0 END
                            ) AS picks,
                            SUM(CASE
                                WHEN m.team2_id = %s
                                 AND mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN 1 ELSE 0 END
                            ) AS opp_picks,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                 AND mp.winner_team_id = %s
                                THEN 1 ELSE 0 END
                            ) AS wins,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN m.best_of ELSE 0 END
                            ) AS games,
                            SUM(CASE
                                WHEN mv.status = 'banned'
                                 AND mv.selected_by_team_id = %s
                                THEN 1 ELSE 0 END
                            ) AS ban1,
                            SUM(CASE
                                WHEN mv.status = 'banned'
                                 AND mv.selected_by_faction = 'faction2'
                                 AND m.team2_id = %s
                                THEN 1 ELSE 0 END
                            ) AS ban2,
                            SUM(CASE
                                WHEN mv.status = 'banned'
                                 AND mv.selected_by_team_id != %s
                                THEN 1 ELSE 0 END
                            ) AS opp_ban,
                            SUM(CASE
                                WHEN mv.status = 'picked'
                                 AND mv.selected_by_team_id = %s
                                THEN 1 ELSE 0 END
                            ) AS total_own_ban,
                            SUM(CASE
                                WHEN mv.status = 'decider'
                                THEN 1 ELSE 0 END
                            ) AS decov,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN COALESCE(ps.kills, 0)
                                ELSE 0 END
                            ) AS kills,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN COALESCE(ps.deaths, 0)
                                ELSE 0 END
                            ) AS deaths,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN COALESCE(ps.mvps, 0)
                                ELSE 0 END
                            ) AS mvps,
                            0 AS rd,
                            0 AS kd,
                            COALESCE(AVG(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                 AND ps.avg_adr IS NOT NULL
                                THEN ps.avg_adr
                                ELSE NULL END
                            ), 0) AS adr,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN COALESCE(ps.damage, 0)
                                ELSE 0 END
                            ) AS damage,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0
                                 AND m.ignored_due_ban = 0
                                THEN COALESCE(ps.utility_damage, 0)
                                ELSE 0 END
                            ) AS utility_damage
                        FROM matches m
                        JOIN maps mp ON mp.match_id = m.match_id
                        LEFT JOIN map_votes mv ON mv.match_id = m.match_id AND mv.map_name = mp.map_name
                        LEFT JOIN (
                            SELECT
                                match_id,
                                map_id,
                                SUM(kills) AS kills,
                                SUM(deaths) AS deaths,
                                SUM(mvps) AS mvps,
                                AVG(adr) AS avg_adr,
                                SUM(damage) AS damage,
                                SUM(utility_damage) AS utility_damage
                            FROM player_stats
                            WHERE team_id = %s
                            GROUP BY match_id, map_id
                        ) ps ON ps.match_id = m.match_id AND ps.map_id = mp.map_id
                        WHERE m.season = %s
                          AND m.division_num = %s
                          AND COALESCE(NULLIF(TRIM(mp.map_name), ''), CONCAT('map_', mp.round_index)) = %s
                          AND (m.team1_id = %s OR m.team2_id = %s)
                        GROUP BY season, division_num, team_id, map_name
                        ON DUPLICATE KEY UPDATE
                            played = VALUES(played),
                            picks = VALUES(picks),
                            opp_picks = VALUES(opp_picks),
                            wins = VALUES(wins),
                            games = VALUES(games),
                            ban1 = VALUES(ban1),
                            ban2 = VALUES(ban2),
                            opp_ban = VALUES(opp_ban),
                            total_own_ban = VALUES(total_own_ban),
                            decov = VALUES(decov),
                            kills = VALUES(kills),
                            deaths = VALUES(deaths),
                            mvps = VALUES(mvps),
                            rd = VALUES(rd),
                            kd = VALUES(kd),
                            adr = VALUES(adr),
                            damage = VALUES(damage),
                            utility_damage = VALUES(utility_damage)
                        """,
                        [
                            season,
                            division_num,
                            team_id,
                            map_key,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            team_id,
                            season,
                            division_num,
                            map_key,
                            team_id,
                            team_id,
                        ],
                    )

    await _retry_on_deadlock(_operation, label=op_label)


async def get_all_championships_for_season(conn: asyncmy.Connection, season: int) -> List[Row]:
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            "SELECT * FROM championships WHERE season = %s ORDER BY division_num",
            (season,),
        )
        return await cur.fetchall()


async def get_all_base_divisions_for_championship(conn: asyncmy.Connection, championship_id: str) -> List[Row]:
    """Fetches all base divisions (not playoffs) for a given root championship ID."""
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                c.championship_id as division_id,
                c.division_num as tier,
                c.name,
                'waiting' as status -- Placeholder
            FROM championships c
            WHERE c.season = (SELECT season FROM championships WHERE championship_id = %s LIMIT 1)
              AND c.is_playoffs = 0
            ORDER BY c.division_num;
            """,
            (championship_id,),
        )
        return await cur.fetchall()


async def get_all_base_divisions_for_season(conn: asyncmy.Connection, season: int) -> List[Row]:
    """Fetches all base divisions (not playoffs) for a given season."""
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                c.championship_id as division_id,
                c.division_num as tier,
                c.name,
                'waiting' as status -- Placeholder
            FROM championships c
            WHERE c.season = %s
              AND c.is_playoffs = 0
            ORDER BY c.division_num;
            """,
            (season,),
        )
        return await cur.fetchall()


async def get_maps_for_division(conn: asyncmy.Connection, season: int, division_num: int) -> List[Row]:
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            "SELECT * FROM maps WHERE season = %s AND division_num = %s",
            (season, division_num),
        )
        return await cur.fetchall()


async def get_division_stats_for_v3(conn: asyncmy.Connection, division_id: int) -> Row:
    """Fetches aggregated stats for a single division for the v3 API."""
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                (SELECT COUNT(DISTINCT tst.team_id)
                 FROM team_season_totals tst
                 JOIN championships c ON tst.division_num = c.division_num AND tst.season = c.season
                 WHERE c.championship_id = %(division_id)s) AS teams,
                (SELECT COUNT(*)
                 FROM matches m
                 WHERE m.championship_id = %(division_id)s AND m.status = 'finished') AS matches_played,
                (SELECT COUNT(*)
                 FROM matches m
                 WHERE m.championship_id = %(division_id)s) AS matches_total,
                (SELECT status FROM matches WHERE championship_id = %(division_id)s ORDER BY finished_at DESC, scheduled_at DESC LIMIT 1) as division_status
            """,
            {"division_id": division_id},
        )
        season_stats = await cur.fetchone()

        await cur.execute(
            """
            SELECT
                (SELECT COUNT(*)
                 FROM matches m
                 JOIN championships c ON m.championship_id = c.championship_id
                 WHERE c.parent_championship_id = %(division_id)s AND m.status = 'finished') AS matches_played,
                (SELECT COUNT(*)
                 FROM matches m
                 JOIN championships c ON m.championship_id = c.championship_id
                 WHERE c.parent_championship_id = %(division_id)s) AS matches_total,
                (SELECT t.name
                 FROM teams t
                 JOIN championships c ON c.winner_team_id = t.team_id
                 WHERE c.parent_championship_id = %(division_id)s LIMIT 1) AS winner_team
            """,
            {"division_id": division_id},
        )
        playoff_stats = await cur.fetchone()

        return {
            "season": season_stats,
            "playoffs": playoff_stats,
        }
