# Sync script: fetch matches, details, stats, and democracy votes; store into SQLite.
import time
from typing import Dict, Any, List, Tuple
from pathlib import Path
from collections import defaultdict

from faceit_config import DIVISIONS, RATE_LIMIT_SLEEP
from faceit_client import list_championship_matches, get_match_details, get_match_stats, get_democracy_history
from db import (
    get_conn, init_db, upsert_division, upsert_match, upsert_map,
    upsert_team_stat, upsert_player_stat, insert_vote, commit,
    match_exists, match_fully_synced
)

DB_PATH = str(Path(__file__).with_name("faceit_reports.sqlite"))

def safe_int(x, default=0):
    try:
        return int(x)
    except:
        try:
            return int(float(x))
        except:
            return default

def safe_float(x, default=0.0):
    try:
        return float(x)
    except:
        try:
            return float(str(x).replace(",", "."))
        except:
            return default

def ratio(num, den):
    if den == 0:
        return 0.0
    return float(num) / float(den)

def derive_kd(kills, deaths):
    return kills / deaths if deaths else float(kills)

def map_faction_to_team_id(match_details: Dict[str, Any]) -> Dict[str, str]:
    # match_details['teams'] commonly includes 'faction1' and 'faction2' objects with 'team_id' and 'name'.
    m = {}
    try:
        t = match_details.get("teams", {})
        for faction in ("faction1", "faction2"):
            if faction in t and isinstance(t[faction], dict):
                team_id = t[faction].get("team_id") or t[faction].get("faction_id")
                m[faction] = team_id or ""
    except:
        pass
    return m

def extract_team_names(match_details: Dict[str, Any]) -> Dict[str, str]:
    names = {}
    try:
        t = match_details.get("teams", {})
        for faction in ("faction1", "faction2"):
            if faction in t and isinstance(t[faction], dict):
                names[faction] = t[faction].get("name") or t[faction].get("team_name") or ""
    except:
        pass
    return names

def parse_match_basic(div, match_item: Dict[str, Any], details: Dict[str, Any]) -> Dict[str, Any]:
    # Build matches row
    factions_to_team = map_faction_to_team_id(details)
    names = extract_team_names(details)
    winner_team_id = details.get("results", {}).get("winner")
    best_of = details.get("match_type") or details.get("best_of") or match_item.get("best_of") or 1
    try:
        best_of = int(best_of)
    except:
        best_of = 1

    # team1/2 based on faction1/2 to keep consistent
    m = {
        "match_id": match_item.get("match_id") or details.get("match_id"),
        "division_id": div["division_id"],
        "competition_id": match_item.get("competition_id"),
        "competition_name": match_item.get("competition_name") or details.get("competition_name"),
        "best_of": best_of,
        "game": match_item.get("game") or details.get("game"),
        "faceit_url": match_item.get("faceit_url") or details.get("faceit_url"),
        "configured_at": match_item.get("configured_at") or details.get("configured_at"),
        "started_at": match_item.get("started_at") or details.get("started_at"),
        "finished_at": match_item.get("finished_at") or details.get("finished_at"),
        "team1_id": factions_to_team.get("faction1"),
        "team1_name": names.get("faction1"),
        "team2_id": factions_to_team.get("faction2"),
        "team2_name": names.get("faction2"),
        "winner_team_id": winner_team_id,
    }
    return m

def parse_round_map_row(match_id: str, round_index: int, round_stats: Dict[str, Any], team1_id: str, team2_id: str) -> Dict[str, Any]:
    # round_stats typically has keys like 'Map', 'Score', 'Winner' etc. Scores often '16-12' string.
    map_name = round_stats.get("Map") or round_stats.get("Map Name") or round_stats.get("map")
    score_str = round_stats.get("Score") or ""
    s1 = s2 = None
    if isinstance(score_str, str) and "-" in score_str:
        left, right = score_str.split("-", 1)
        try:
            s1 = int(left.strip())
            s2 = int(right.strip())
        except:
            s1 = s2 = None
    winner_faction = round_stats.get("Winner")  # sometimes a team name
    winner_team_id = None
    # winner may be a name; we will set id later by comparing to team names if needed
    return {
        "match_id": match_id,
        "round_index": round_index,
        "map_name": map_name,
        "score_team1": s1,
        "score_team2": s2,
        "winner_team_id": winner_team_id,
    }

def aggregate_team_stats_from_players(players: List[Dict[str, Any]], team_id: str, team_name: str) -> Dict[str, Any]:
    agg = defaultdict(int)
    adr_total = 0.0
    kr_total = 0.0
    hs_pct_accum = 0.0
    hs_count = 0
    for p in players:
        ps = p.get("player_stats", {})
        # Basic
        kills = safe_int(ps.get("Kills", 0))
        deaths = safe_int(ps.get("Deaths", 0))
        assists = safe_int(ps.get("Assists", 0))
        mvps = safe_int(ps.get("MVPs", 0))
        sniper_kills = safe_int(ps.get("Sniper Kills", 0)) or safe_int(ps.get("AWP Kills", 0))
        flash_assists = safe_int(ps.get("Flash Assists", 0))

        # Utility
        util = safe_int(ps.get("Utility Damage", 0))
        adr = safe_float(ps.get("ADR", 0.0))
        kr = safe_float(ps.get("K/R Ratio", 0.0))

        # Multi kills
        mk3 = safe_int(ps.get("Triple Kills", 0))
        mk4 = safe_int(ps.get("Quadro Kills", 0)) or safe_int(ps.get("Quadra Kills", 0))
        mk5 = safe_int(ps.get("Penta Kills", 0)) or safe_int(ps.get("ACE", 0))
        mk2 = safe_int(ps.get("Double Kills", 0))

        # Entries (best-effort; some stats sets include First Kills)
        entries = safe_int(ps.get("First Kills", 0))
        entry_wins = entries  # best-effort

        agg["kills"] += kills
        agg["deaths"] += deaths
        agg["assists"] += assists
        agg["mvps"] += mvps
        agg["sniper_kills"] += sniper_kills
        agg["utility_damage"] += util
        agg["flash_assists"] += flash_assists
        agg["mk_2k"] += mk2
        agg["mk_3k"] += mk3
        agg["mk_4k"] += mk4
        agg["mk_5k"] += mk5
        agg["entry_count"] += entries
        agg["entry_wins"] += entry_wins

        if adr > 0:
            adr_total += adr
        if kr > 0:
            kr_total += kr
        hs = ps.get("Headshots %") or ps.get("HS %")
        if hs is not None:
            hs_pct_accum += safe_float(hs)
            hs_count += 1

    deaths = agg["deaths"]
    kd_val = derive_kd(agg["kills"], deaths)
    # Team averages for ADR, KR, HS% are mean over players
    players_n = len(players) if players else 1
    adr_mean = adr_total / max(1, players_n)
    kr_mean = kr_total / max(1, players_n)
    hs_mean = hs_pct_accum / max(1, hs_count or players_n)

    return {
        "kills": agg["kills"],
        "deaths": agg["deaths"],
        "assists": agg["assists"],
        "kd": kd_val,
        "kr": kr_mean,
        "adr": adr_mean,
        "hs_pct": hs_mean,
        "mvps": agg["mvps"],
        "sniper_kills": agg["sniper_kills"],
        "utility_damage": agg["utility_damage"],
        "flash_assists": agg["flash_assists"],
        "entry_count": agg["entry_count"],
        "entry_wins": agg["entry_wins"],
        "mk_2k": agg["mk_2k"],
        "mk_3k": agg["mk_3k"],
        "mk_4k": agg["mk_4k"],
        "mk_5k": agg["mk_5k"],
    }

def extract_player_row(match_id: str, round_index: int, pl: Dict[str, Any], team_id: str, team_name: str) -> Dict[str, Any]:
    ps = pl.get("player_stats", {}) or {}
    nickname = pl.get("nickname") or ps.get("Player") or ""
    player_id = pl.get("player_id") or pl.get("player_id") or ""
    kills = safe_int(ps.get("Kills", 0))
    deaths = safe_int(ps.get("Deaths", 0))
    assists = safe_int(ps.get("Assists", 0))
    adr = safe_float(ps.get("ADR", 0.0))
    kr = safe_float(ps.get("K/R Ratio", 0.0))
    kd_val = derive_kd(kills, deaths)
    hs_pct = safe_float(ps.get("Headshots %", 0.0) or ps.get("HS %", 0.0))
    mvps = safe_int(ps.get("MVPs", 0))
    sniper_kills = safe_int(ps.get("Sniper Kills", 0)) or safe_int(ps.get("AWP Kills", 0))
    util = safe_int(ps.get("Utility Damage", 0))
    flash_assists = safe_int(ps.get("Flash Assists", 0))
    mk3 = safe_int(ps.get("Triple Kills", 0))
    mk4 = safe_int(ps.get("Quadro Kills", 0)) or safe_int(ps.get("Quadra Kills", 0))
    mk5 = safe_int(ps.get("Penta Kills", 0)) or safe_int(ps.get("ACE", 0))
    # --- Clutches & clutch kills (suoraan Faceit-player_stats-kentistä) ---
    clutch_kills = safe_int(ps.get("Clutch Kills", 0))
    c11_attempts = safe_int(ps.get("1v1Count", 0))
    c11_wins     = safe_int(ps.get("1v1Wins", 0))
    c12_attempts = safe_int(ps.get("1v2Count", 0))
    c12_wins     = safe_int(ps.get("1v2Wins", 0))

    # Attempts unknown -> 0 by default
    return {
        "match_id": match_id,
        "round_index": round_index,
        "player_id": player_id,
        "nickname": nickname,
        "team_id": team_id,
        "team_name": team_name,
        "kills": kills, "deaths": deaths, "assists": assists,
        "kd": kd_val, "kr": kr, "adr": adr, "hs_pct": hs_pct,
        "mvps": mvps, "sniper_kills": sniper_kills, "utility_damage": util, "flash_assists": flash_assists,
        "mk_3k": mk3, "mk_4k": mk4, "mk_5k": mk5,
        "clutch_kills": clutch_kills,
        "cl_1v1_attempts": c11_attempts, "cl_1v1_wins": c11_wins,
        "cl_1v2_attempts": c12_attempts, "cl_1v2_wins": c12_wins,
    }

def sync_division(con, div):
    print(f"[SYNC] Division {div['division_id']} - {div['name']} ...")
    upsert_division(con, div)
    matches = list_championship_matches(div["championship_id"], match_type="past")
    print(f"[SYNC] Found {len(matches)} matches")
    for m in matches:
        match_id = m.get("match_id")
        if not match_id:
            continue

        # 1) Jos ottelu on jo “fully synced”, skippaa heti
        if match_fully_synced(con, match_id):
            print(f"[SKIP] {match_id} on jo synkattu (maps + player_stats) – ei päivitystä.")
            continue

        # 2) Jos ottelu on kannassa mutta ei fully-synced, jatketaan varovasti (täydennetään puuttuvat osat)
        if match_exists(con, match_id):
            print(f"[RESUME] {match_id} löytyi matches-taulusta – täydennetään puuttuvat taulut.")

        # Vasta tässä vaiheessa haetaan API:sta
        details = get_match_details(match_id)

        stats = {}
        try:
            stats = get_match_stats(match_id)
        except Exception as e:
            print(f"[WARN] stats fetch failed for {match_id}: {e}")


        # Insert match basic row
        match_row = parse_match_basic(div, m, details)
        upsert_match(con, match_row)

        # Map faction -> team id and names (needed for votes + team/player stats)
        factions_to_team = map_faction_to_team_id(details)
        names = extract_team_names(details)
        team1_id, team2_id = factions_to_team.get("faction1"), factions_to_team.get("faction2")
        team1_name, team2_name = names.get("faction1"), names.get("faction2")

        # --- Rounds (maps) + teams + players ---
        rounds = (stats.get("rounds") or [])
        for idx, rnd in enumerate(rounds, start=1):
            round_stats = rnd.get("round_stats", {}) or {}
            teams_arr = rnd.get("teams") or []  # oikea rakenne: list of teams each with team_stats + players

            # Luo map-rivi: nimi + tulos
            map_row = parse_round_map_row(match_id, idx, round_stats, team1_id, team2_id)

            # Jos Score puuttuu round_statsista, päättele pisteet teamien 'Final Score' -kentästä
            if (map_row["score_team1"] is None or map_row["score_team2"] is None) and len(teams_arr) >= 2:
                # yhdistä fac -> idx
                t_by_faction = { (t.get("team_id") or t.get("faction")): t for t in teams_arr }
                t1 = t_by_faction.get("faction1") or teams_arr[0]
                t2 = t_by_faction.get("faction2") or teams_arr[1]
                fs1 = t1.get("team_stats", {}).get("Final Score")
                fs2 = t2.get("team_stats", {}).get("Final Score")
                try:
                    map_row["score_team1"] = int(fs1) if fs1 is not None else map_row["score_team1"]
                    map_row["score_team2"] = int(fs2) if fs2 is not None else map_row["score_team2"]
                except Exception:
                    pass

            # Päätä voittaja pisteistä
            if map_row["winner_team_id"] is None and map_row["score_team1"] is not None and map_row["score_team2"] is not None:
                map_row["winner_team_id"] = team1_id if map_row["score_team1"] > map_row["score_team2"] else (
                    team2_id if map_row["score_team2"] > map_row["score_team1"] else None
                )

            upsert_map(con, map_row)

            # Käsittele jokainen team roundilla
            for t in teams_arr:
                tid_raw = t.get("team_id") or t.get("faction") or ""
                if tid_raw == "faction1":
                    tid, tname = team1_id, team1_name
                elif tid_raw == "faction2":
                    tid, tname = team2_id, team2_name
                else:
                    # jos API antaa oikean id:n, käytä sitä; fallback nimen perusteella
                    tid = tid_raw or (team1_id if t.get("faction") == "faction1" else team2_id)
                    tname = t.get("team_stats", {}).get("Team") or (team1_name if tid == team1_id else team2_name)

                players = t.get("players") or []
                # Aggretoi tiimitaso suoraan pelaajista (varmin tapa)
                if players:
                    ts = aggregate_team_stats_from_players(players, tid, tname)
                    ts.update({"match_id": match_id, "round_index": idx, "team_id": tid, "team_name": tname})
                    upsert_team_stat(con, ts)

                    # Tallenna pelaajat
                    for p in players:
                        pr = extract_player_row(match_id, idx, p, tid, tname)
                        upsert_player_stat(con, pr)


        # Democracy votes (maps only; ignore server/location)
        try:
            demo = get_democracy_history(match_id)
            tickets = demo.get("payload", {}).get("tickets", [])
            for t in tickets:
                if t.get("entity_type") != "map":
                    # skip server/location vetoes explicitly
                    continue
                entities = t.get("entities", [])
                for ent in entities:
                    v = {
                        "match_id": match_id,
                        "map_name": ent.get("guid"),
                        "status": ent.get("status"),
                        "selected_by_faction": ent.get("selected_by"),
                        "round_num": ent.get("round"),
                        "selected_by_team_id": None,
                    }
                    fac = v["selected_by_faction"]
                    if fac in factions_to_team:
                        v["selected_by_team_id"] = factions_to_team[fac]
                    insert_vote(con, v)
            time.sleep(RATE_LIMIT_SLEEP)
        except Exception as e:
            print(f"[WARN] democracy fetch failed for {match_id}: {e}")

        commit(con)
        time.sleep(RATE_LIMIT_SLEEP)

def main():
    con = get_conn(DB_PATH)
    init_db(con)
    for div in DIVISIONS:
        sync_division(con, div)
    commit(con)
    con.close()
    print("[DONE] Sync complete.")

if __name__ == "__main__":
    main()
