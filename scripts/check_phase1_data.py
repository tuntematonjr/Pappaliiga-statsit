"""Quick diagnostic to verify Phase 1 data availability."""
import asyncio
from db_async import query_async, connection


async def check_data():
    print("=== Checking Phase 1 Data Availability ===\n")
    
    # 1. Check if we have team_season_totals data
    print("1. Team Season Totals:")
    rows = await query_async(
        """
        SELECT COUNT(*) as count, 
               COUNT(DISTINCT team_id) as teams,
               COUNT(DISTINCT season) as seasons
        FROM team_season_totals
        """
    )
    if rows:
        print(f"   ✅ {rows[0]['count']} records, {rows[0]['teams']} teams, {rows[0]['seasons']} seasons")
    else:
        print("   ❌ No data")
    
    # 2. Check matches for recent form
    print("\n2. Match History (for recent form):")
    rows = await query_async(
        """
        SELECT COUNT(*) as total_matches,
               COUNT(CASE WHEN finished_at IS NOT NULL THEN 1 END) as finished_matches,
               COUNT(CASE WHEN winner_team_id IS NOT NULL THEN 1 END) as matches_with_winner
        FROM matches
        """
    )
    if rows:
        r = rows[0]
        print(f"   ✅ {r['total_matches']} total, {r['finished_matches']} finished, {r['matches_with_winner']} with winner")
    else:
        print("   ❌ No data")
    
    # 3. Check player stats for role detection
    print("\n3. Player Stats (for role detection):")
    rows = await query_async(
        """
        SELECT COUNT(*) as records,
               COUNT(DISTINCT player_id) as players,
               SUM(CASE WHEN sniper_kills > 0 THEN 1 ELSE 0 END) as with_awp,
               SUM(CASE WHEN entry_count > 0 THEN 1 ELSE 0 END) as with_entry,
               SUM(CASE WHEN clutch_kills > 0 THEN 1 ELSE 0 END) as with_clutch
        FROM player_season_totals
        """
    )
    if rows:
        r = rows[0]
        print(f"   ✅ {r['records']} records for {r['players']} players")
        print(f"      - {r['with_awp']} with AWP data")
        print(f"      - {r['with_entry']} with entry data")
        print(f"      - {r['with_clutch']} with clutch data")
    else:
        print("   ❌ No data")
    
    # 4. Sample standings calculation
    print("\n4. Standings Calculation (sample):")
    rows = await query_async(
        """
        SELECT 
            tst.team_id,
            t.name as team_name,
            tst.matches_won,
            tst.matches_played,
            (CAST(tst.rounds_won AS SIGNED) - CAST(tst.rounds_lost AS SIGNED)) as round_diff
        FROM team_season_totals tst
        JOIN teams t ON t.team_id = tst.team_id
        WHERE tst.season = (SELECT MAX(season) FROM team_season_totals)
        ORDER BY tst.matches_won DESC, round_diff DESC
        LIMIT 5
        """
    )
    if rows:
        print(f"   ✅ Top 5 teams in latest season:")
        for idx, r in enumerate(rows, 1):
            print(f"      {idx}. {r['team_name']}: {r['matches_won']}W/{r['matches_played']}M (RD: {r['round_diff']})")
    else:
        print("   ❌ No data")
    
    # 5. Sample recent form
    print("\n5. Recent Form (sample team):")
    # Get a team with matches
    team_rows = await query_async(
        """
        SELECT team_id
        FROM team_season_totals
        WHERE matches_played > 3
        LIMIT 1
        """
    )
    if team_rows:
        team_id = team_rows[0]['team_id']
        rows = await query_async(
            """
            SELECT 
                m.match_id,
                m.finished_at,
                m.winner_team_id,
                m.team1_id,
                m.team2_id
            FROM matches m
            WHERE (m.team1_id = :team_id OR m.team2_id = :team_id)
              AND m.finished_at IS NOT NULL
            ORDER BY m.finished_at DESC
            LIMIT 5
            """,
            {"team_id": team_id}
        )
        if rows:
            form = []
            for r in rows:
                result = "W" if str(r['winner_team_id']) == str(team_id) else "L"
                form.append(result)
            print(f"   ✅ Team {team_id}: {' '.join(form)}")
        else:
            print(f"   ⚠️  Team {team_id} has no finished matches")
    else:
        print("   ❌ No teams with matches")
    
    print("\n=== All checks complete ===")


if __name__ == "__main__":
    asyncio.run(check_data())
