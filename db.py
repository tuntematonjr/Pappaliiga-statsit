# Tiny SQLite helpers. Keep it simple and explicit.
import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

def get_conn(db_path: str):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def init_db(con: sqlite3.Connection):
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        con.executescript(f.read())
    con.commit()

def upsert_division(con, division):
    con.execute(
        """INSERT INTO divisions(division_id, name, slug, championship_id)
        VALUES(?,?,?,?)
        ON CONFLICT(division_id) DO UPDATE SET name=excluded.name, slug=excluded.slug, championship_id=excluded.championship_id
        """,
        (division["division_id"], division["name"], division["slug"], division["championship_id"]),
    )

def upsert_match(con, m):
    con.execute(
        """INSERT INTO matches(match_id, division_id, competition_id, competition_name, best_of, game, faceit_url,
               configured_at, started_at, finished_at, team1_id, team1_name, team2_id, team2_name, winner_team_id)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(match_id) DO UPDATE SET
            division_id=excluded.division_id,
            competition_id=excluded.competition_id,
            competition_name=excluded.competition_name,
            best_of=excluded.best_of,
            game=excluded.game,
            faceit_url=excluded.faceit_url,
            configured_at=excluded.configured_at,
            started_at=excluded.started_at,
            finished_at=excluded.finished_at,
            team1_id=excluded.team1_id,
            team1_name=excluded.team1_name,
            team2_id=excluded.team2_id,
            team2_name=excluded.team2_name,
            winner_team_id=excluded.winner_team_id
        """, (
            m["match_id"], m["division_id"],
            m.get("competition_id"), m.get("competition_name"),
            m.get("best_of"), m.get("game"), m.get("faceit_url"),
            m.get("configured_at"), m.get("started_at"), m.get("finished_at"),
            m.get("team1_id"), m.get("team1_name"), m.get("team2_id"), m.get("team2_name"),
            m.get("winner_team_id"),
        )
    )

def upsert_map(con, mp):
    con.execute(
        """INSERT INTO maps(match_id, round_index, map_name, score_team1, score_team2, winner_team_id)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(match_id, round_index) DO UPDATE SET
            map_name=excluded.map_name,
            score_team1=excluded.score_team1,
            score_team2=excluded.score_team2,
            winner_team_id=excluded.winner_team_id
        """, (
            mp["match_id"], mp["round_index"], mp.get("map_name"),
            mp.get("score_team1"), mp.get("score_team2"), mp.get("winner_team_id"),
        )
    )

def insert_vote(con, v):
    con.execute(
        """INSERT INTO map_votes(match_id, map_name, status, selected_by_faction, round_num, selected_by_team_id)
        VALUES(?,?,?,?,?,?)""",
        (v["match_id"], v.get("map_name"), v.get("status"), v.get("selected_by_faction"), v.get("round_num"), v.get("selected_by_team_id"))
    )

def upsert_team_stat(con, ts):
    con.execute(
        """INSERT INTO team_stats(match_id, round_index, team_id, team_name, kills, deaths, assists, kd, kr, adr, hs_pct,
                                     mvps, sniper_kills, utility_damage, flash_assists, entry_count, entry_wins,
                                     mk_2k, mk_3k, mk_4k, mk_5k)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(match_id, round_index, team_id) DO UPDATE SET
            team_name=excluded.team_name,
            kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists,
            kd=excluded.kd, kr=excluded.kr, adr=excluded.adr, hs_pct=excluded.hs_pct,
            mvps=excluded.mvps, sniper_kills=excluded.sniper_kills, utility_damage=excluded.utility_damage,
            flash_assists=excluded.flash_assists, entry_count=excluded.entry_count, entry_wins=excluded.entry_wins,
            mk_2k=excluded.mk_2k, mk_3k=excluded.mk_3k, mk_4k=excluded.mk_4k, mk_5k=excluded.mk_5k
        """, (
            ts["match_id"], ts["round_index"], ts["team_id"], ts.get("team_name"),
            ts.get("kills"), ts.get("deaths"), ts.get("assists"),
            ts.get("kd"), ts.get("kr"), ts.get("adr"), ts.get("hs_pct"),
            ts.get("mvps"), ts.get("sniper_kills"), ts.get("utility_damage"),
            ts.get("flash_assists"), ts.get("entry_count"), ts.get("entry_wins"),
            ts.get("mk_2k"), ts.get("mk_3k"), ts.get("mk_4k"), ts.get("mk_5k"),
        )
    )

def upsert_player_stat(con, ps):
    con.execute(
        """INSERT INTO player_stats(match_id, round_index, player_id, nickname, team_id, team_name, kills, deaths, assists,
                                      kd, kr, adr, hs_pct, mvps, sniper_kills, utility_damage, flash_assists,
                                      mk_3k, mk_4k, mk_5k,
                                      cl_1v1_wins, cl_1v1_attempts, cl_1v2_wins, cl_1v2_attempts,
                                      cl_1v3_wins, cl_1v3_attempts, cl_1v4_wins, cl_1v4_attempts,
                                      cl_1v5_wins, cl_1v5_attempts)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(match_id, round_index, player_id, nickname) DO UPDATE SET
            team_id=excluded.team_id, team_name=excluded.team_name,
            kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists,
            kd=excluded.kd, kr=excluded.kr, adr=excluded.adr, hs_pct=excluded.hs_pct,
            mvps=excluded.mvps, sniper_kills=excluded.sniper_kills, utility_damage=excluded.utility_damage,
            flash_assists=excluded.flash_assists, mk_3k=excluded.mk_3k, mk_4k=excluded.mk_4k, mk_5k=excluded.mk_5k,
            cl_1v1_wins=excluded.cl_1v1_wins, cl_1v1_attempts=excluded.cl_1v1_attempts,
            cl_1v2_wins=excluded.cl_1v2_wins, cl_1v2_attempts=excluded.cl_1v2_attempts,
            cl_1v3_wins=excluded.cl_1v3_wins, cl_1v3_attempts=excluded.cl_1v3_attempts,
            cl_1v4_wins=excluded.cl_1v4_wins, cl_1v4_attempts=excluded.cl_1v4_attempts,
            cl_1v5_wins=excluded.cl_1v5_wins, cl_1v5_attempts=excluded.cl_1v5_attempts
        """, (
            ps["match_id"], ps["round_index"], ps.get("player_id"), ps.get("nickname"),
            ps.get("team_id"), ps.get("team_name"),
            ps.get("kills"), ps.get("deaths"), ps.get("assists"),
            ps.get("kd"), ps.get("kr"), ps.get("adr"), ps.get("hs_pct"),
            ps.get("mvps"), ps.get("sniper_kills"), ps.get("utility_damage"), ps.get("flash_assists"),
            ps.get("mk_3k"), ps.get("mk_4k"), ps.get("mk_5k"),
            ps.get("cl_1v1_wins"), ps.get("cl_1v1_attempts"),
            ps.get("cl_1v2_wins"), ps.get("cl_1v2_attempts"),
            ps.get("cl_1v3_wins"), ps.get("cl_1v3_attempts"),
            ps.get("cl_1v4_wins"), ps.get("cl_1v4_attempts"),
            ps.get("cl_1v5_wins"), ps.get("cl_1v5_attempts"),
        )
    )

def commit(con):
    con.commit()
