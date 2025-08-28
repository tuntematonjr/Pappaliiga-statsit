# debug_match_players.py
import os, sys, json, sqlite3
from typing import Any, Dict

# Käytetään olemassa olevaa clienttia (lukee FACEIT_API_KEY:n)
try:
    from faceit_client import get_match_details, get_match_stats
except Exception as e:
    print("faceit_client.py ei saatavilla tai import epäonnistui:", e)
    sys.exit(1)

DB_PATH = os.path.join(os.path.dirname(__file__), "faceit_reports.sqlite")

def flat_keys(d: Dict[str, Any]) -> str:
    return ", ".join(sorted(d.keys()))

def api_inspect(match_id: str, dump_json: bool=False) -> None:
    print(f"[API] match_id={match_id}")
    try:
        details = get_match_details(match_id)
        stats   = get_match_stats(match_id)
    except Exception as e:
        print("[API] haku epäonnistui:", e)
        return

    if dump_json:
        print("\n=== RAW details ===")
        print(json.dumps(details, indent=2, ensure_ascii=False))
        print("\n=== RAW stats ===")
        print(json.dumps(stats, indent=2, ensure_ascii=False))

    # Yritetään Faceit stats -rakenne: rounds -> teams -> players
    rounds = stats.get("rounds") or []
    print(f"\n[API] rounds: {len(rounds)}")
    for ri, rnd in enumerate(rounds, start=1):
        teams = rnd.get("teams") or []
        print(f"\n-- Round #{ri} --")
        for ti, team in enumerate(teams, start=1):
            tname = team.get("name") or team.get("team_id") or f"team{ti}"
            print(f"  Team: {tname}")
            players = team.get("players") or []
            for p in players:
                nick = p.get("nickname") or p.get("player_nickname") or p.get("player_id")
                pid  = p.get("player_id")
                # Tulosta saatavilla olevat avaimet yhdellä rivillä
                print(f"    - {nick} ({pid}) :: keys = [{flat_keys(p)}]")

def db_inspect(match_id: str) -> None:
    if not os.path.exists(DB_PATH):
        print(f"[DB] Ei löydy: {DB_PATH}")
        return
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    print(f"\n[DB] player_stats rivit matchille {match_id}:")
    cur.execute("""
        SELECT nickname, team_name, team_id, round_index, kills, deaths, assists,
               adr, kr, hs_pct, utility_damage, sniper_kills,
               mk_3k, mk_4k, mk_5k, clutches_won
        FROM player_stats
        WHERE match_id=?
        ORDER BY team_name, nickname, round_index
    """, (match_id,))
    rows = cur.fetchall()
    if not rows:
        print("  (ei rivejä)")
    else:
        for r in rows:
            print(f"  - [{r['round_index']}] {r['team_name']} / {r['nickname']}: "
                  f"K/D/A {r['kills']}/{r['deaths']}/{r['assists']}, "
                  f"ADR {r['adr']:.1f}, KR {r['kr']:.2f}, HS% {r['hs_pct']:.1f}, "
                  f"UTIL {r['utility_damage']}, AWP {r['sniper_kills']}, "
                  f"3K/4K/5K {r['mk_3k']}/{r['mk_4k']}/{r['mk_5k']}, "
                  f"Clutches {r['clutches_won']}")
    con.close()

def main():
    if len(sys.argv) < 2:
        print("Käyttö: python debug_match_players.py <match_id> [--dump-json] [--from-db]")
        sys.exit(1)
    match_id = sys.argv[1]
    dump = "--dump-json" in sys.argv
    from_db = "--from-db" in sys.argv

    api_inspect(match_id, dump_json=dump)
    if from_db:
        db_inspect(match_id)

if __name__ == "__main__":
    main()
