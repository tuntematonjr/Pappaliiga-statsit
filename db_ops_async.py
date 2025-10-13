from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import asyncmy

DEFAULT_TEAM_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"

Row = Mapping[str, Any]


def _normalize_avatar(url: Optional[str]) -> str:
    raw = (url or "").strip()
    return raw or DEFAULT_TEAM_AVATAR


async def upsert_championship_async(conn: asyncmy.Connection, row: Row) -> str:
    payload = {
        "championship_id": row.get("championship_id"),
        "season": row.get("season"),
        "division_num": row.get("division_num"),
        "name": row.get("name"),
        "is_playoffs": 1 if row.get("is_playoffs") else 0,
        "slug": row.get("slug"),
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO championships (championship_id, season, division_num, name, is_playoffs, slug)
            VALUES (%(championship_id)s, %(season)s, %(division_num)s, %(name)s, %(is_playoffs)s, %(slug)s)
            ON DUPLICATE KEY UPDATE
              season = VALUES(season),
              division_num = VALUES(division_num),
              name = CASE WHEN championships.name = '' THEN VALUES(name) ELSE championships.name END,
              is_playoffs = VALUES(is_playoffs),
              slug = CASE WHEN championships.slug = '' THEN VALUES(slug) ELSE championships.slug END
            """,
            payload,
        )
    return str(payload["championship_id"])


async def upsert_team_async(conn: asyncmy.Connection, team: Row) -> None:
    payload = {
        "team_id": team.get("team_id"),
        "name": team.get("name"),
        "avatar": _normalize_avatar(team.get("avatar")),
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO teams (team_id, name, avatar)
            VALUES (%(team_id)s, %(name)s, %(avatar)s)
            ON DUPLICATE KEY UPDATE
              name = CASE WHEN VALUES(name) <> '' THEN VALUES(name) ELSE teams.name END,
              avatar = CASE WHEN VALUES(avatar) <> '' THEN VALUES(avatar) ELSE teams.avatar END
            """,
            payload,
        )


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


async def upsert_players_bulk_async(conn: asyncmy.Connection, players: Iterable[Row]) -> None:
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
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO players (player_id, nickname)
            VALUES (%(player_id)s, %(nickname)s)
            ON DUPLICATE KEY UPDATE nickname = CASE WHEN players.nickname = '' THEN VALUES(nickname) ELSE players.nickname END
            """,
            rows,
        )


async def upsert_match_async(conn: asyncmy.Connection, row: Row) -> None:
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
              status, last_seen_at, team1_id, team2_id, winner_team_id,
              is_forfeit, ignored_due_ban
            )
            VALUES (
              %(match_id)s, %(championship_id)s, %(season)s, %(division_num)s, %(best_of)s,
              %(configured_at)s, %(started_at)s, %(finished_at)s, %(scheduled_at)s,
              %(status)s, %(last_seen_at)s, %(team1_id)s, %(team2_id)s, %(winner_team_id)s,
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
              team1_id = VALUES(team1_id),
              team2_id = VALUES(team2_id),
              winner_team_id = VALUES(winner_team_id),
              is_forfeit = VALUES(is_forfeit),
              ignored_due_ban = VALUES(ignored_due_ban)
            """,
            payload,
        )


async def upsert_map_async(
    conn: asyncmy.Connection,
    match_id: str,
    season: int,
    division_num: int,
    row: Row,
) -> None:
    payload = {
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
    async with conn.cursor() as cur:
        await cur.execute(
            """
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
            """,
            payload,
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
    conn: asyncmy.Connection,
    match_id: str,
    season: int,
    division_num: int,
    votes: Iterable[Row],
) -> None:
    async with conn.cursor() as cur:
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
        if not rows:
            return
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


async def upsert_player_stat_async(
    conn: asyncmy.Connection,
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Dict[int, int],
    row: Row,
    is_forfeit_map: bool,
) -> None:
    map_id = map_lookup.get(round_index)
    payload = {
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
    async with conn.cursor() as cur:
        await cur.execute(
            """
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
            """,
            payload,
        )


async def upsert_team_stat_async(
    conn: asyncmy.Connection,
    season: int,
    division_num: int,
    match_id: str,
    round_index: int,
    map_lookup: Dict[int, int],
    row: Row,
    is_forfeit_map: bool,
) -> None:
    map_id = map_lookup.get(round_index)
    payload = {
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
    async with conn.cursor() as cur:
        await cur.execute(
            """
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
            """,
            payload,
        )


async def delete_stats_for_match_async(conn: asyncmy.Connection, match_id: str) -> None:
    async with conn.cursor() as cur:
        await cur.execute("DELETE FROM player_stats WHERE match_id = %s", (match_id,))
        await cur.execute("DELETE FROM team_stats WHERE match_id = %s", (match_id,))


async def clear_obsolete_maps_async(conn: asyncmy.Connection, match_id: str, round_indices: Sequence[int]) -> None:
    if not round_indices:
        return
    placeholders = ",".join(["%s"] * len(round_indices))
    args = [match_id, *round_indices]
    async with conn.cursor() as cur:
        await cur.execute(
            f"DELETE FROM maps WHERE match_id = %s AND round_index NOT IN ({placeholders})",
            args,
        )


async def upsert_player_season_totals_async(conn: asyncmy.Connection, season: int, division_num: int, player_id: str) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
              MAX(ps.team_id) AS team_id,
              SUM(CASE WHEN ps.is_forfeit_map = 0 THEN 1 ELSE 0 END) AS maps_played,
              SUM(CASE WHEN ps.is_forfeit_map = 0 THEN (COALESCE(m.score_team1,0)+COALESCE(m.score_team2,0)) ELSE 0 END) AS rounds_played,
              SUM(ps.kills) AS kills,
              SUM(ps.deaths) AS deaths,
              SUM(ps.assists) AS assists,
              AVG(ps.adr) AS adr,
              AVG(ps.kr) AS kr,
              AVG(ps.hs_pct) AS hs_pct,
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
            LEFT JOIN maps m ON m.match_id = ps.match_id AND m.round_index = ps.round_index
            WHERE ps.season = %s AND ps.division_num = %s AND ps.player_id = %s
                AND mt.ignored_due_ban = 0
            """,
            (season, division_num, player_id),
        )
        row = await cur.fetchone()
        if not row:
            return
        (team_id, maps_played, rounds_played, kills, deaths, assists, adr, kr, hs_pct, mvps, sniper_kills,
         utility_damage, enemies_flashed, flash_count, flash_successes, mk_2k, mk_3k, mk_4k, mk_5k,
         clutch_kills, cl_1v1_attempts, cl_1v1_wins, cl_1v2_attempts, cl_1v2_wins, entry_count, entry_wins,
         pistol_kills, damage) = row
        
        kd = float(kills) / float(deaths) if deaths else float(kills or 0)
        rating = (float(kr or 0) + float(adr or 0) / 100.0) / 2.0
        
        await cur.execute(
            """
            INSERT INTO player_season_totals (
              season, division_num, player_id, team_id,
              maps_played, rounds_played, kills, deaths, assists,
              adr, kr, kd, rating, hs_pct, mvps, sniper_kills,
              utility_damage, enemies_flashed, flash_count, flash_successes,
              mk_2k, mk_3k, mk_4k, mk_5k, clutch_kills,
              cl_1v1_attempts, cl_1v1_wins, cl_1v2_attempts, cl_1v2_wins,
              entry_count, entry_wins, pistol_kills, damage
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                season, division_num, player_id, team_id,
                maps_played or 0, rounds_played or 0, kills or 0, deaths or 0, assists or 0,
                adr or 0, kr or 0, kd, rating, hs_pct or 0, mvps or 0, sniper_kills or 0,
                utility_damage or 0, enemies_flashed or 0, flash_count or 0, flash_successes or 0,
                mk_2k or 0, mk_3k or 0, mk_4k or 0, mk_5k or 0, clutch_kills or 0,
                cl_1v1_attempts or 0, cl_1v1_wins or 0, cl_1v2_attempts or 0, cl_1v2_wins or 0,
                entry_count or 0, entry_wins or 0, pistol_kills or 0, damage or 0,
            ),
        )


async def upsert_team_season_totals_async(conn: asyncmy.Connection, season: int, division_num: int, team_id: str) -> None:
    if not team_id:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
                            COUNT(DISTINCT CASE
                                WHEN m.is_forfeit = 0 AND m.ignored_due_ban = 0 AND
                                         (m.team1_id = %s OR m.team2_id = %s)
                                THEN m.match_id END) AS matches_played,
                            COUNT(DISTINCT CASE
                                WHEN m.is_forfeit = 0 AND m.ignored_due_ban = 0 AND m.winner_team_id = %s AND
                                         (m.team1_id = %s OR m.team2_id = %s)
                                THEN m.match_id END) AS matches_won,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND
                                         (m.team1_id = %s OR m.team2_id = %s)
                                THEN 1 ELSE 0 END) AS maps_played,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND mp.winner_team_id = %s AND
                                         (m.team1_id = %s OR m.team2_id = %s)
                                THEN 1 ELSE 0 END) AS maps_won,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND m.team1_id = %s
                                    THEN COALESCE(mp.score_team1, 0)
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND m.team2_id = %s
                                    THEN COALESCE(mp.score_team2, 0)
                                ELSE 0
                            END) AS rounds_won,
                            SUM(CASE
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND m.team1_id = %s
                                    THEN COALESCE(mp.score_team2, 0)
                                WHEN mp.is_forfeit = 0 AND m.ignored_due_ban = 0 AND m.team2_id = %s
                                    THEN COALESCE(mp.score_team1, 0)
                                ELSE 0
                            END) AS rounds_lost
                        FROM matches m
                        LEFT JOIN maps mp ON mp.match_id = m.match_id
                        WHERE m.season = %s AND m.division_num = %s AND (m.team1_id = %s OR m.team2_id = %s)
                        """,
            (
                # 14 occurrences for all team_id placeholders before WHERE
                team_id, team_id,             # matches_played (team1_id, team2_id)
                team_id, team_id, team_id,    # matches_won (winner_team_id, team1_id, team2_id)
                team_id, team_id,             # maps_played (team1_id, team2_id)
                team_id, team_id, team_id,    # maps_won (winner_team_id, team1_id, team2_id)
                team_id, team_id,             # rounds_won (team1_id, team2_id)
                team_id, team_id,             # rounds_lost (team1_id, team2_id)
                season,
                division_num,
                team_id, team_id,             # WHERE ... (team1_id OR team2_id)
            ),
        )


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
