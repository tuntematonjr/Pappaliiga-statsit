"""Division and season API endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from async_db import query_async
from api.services.player_counts import get_player_counts
from division_overrides import combined_status_teams

# Default avatar when remote avatar is missing or blocked
DEFAULT_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"


def get_excluded_team_ids(championship_id: str) -> set[str]:
    """Get set of team IDs to exclude (banned + quit). Accepts championship id as string."""
    teams = combined_status_teams(str(championship_id))
    return {team["team_id"] for team in teams}

router = APIRouter()


class SeasonInfo(BaseModel):
    """Season metadata."""
    season: int
    divisions: List[int]
    championship_ids: List[str]


class DivisionSummary(BaseModel):
    """Division summary info."""
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    # Fields used by the frontend home view
    teams_count: int | None = 0
    played_matches: int | None = 0
    total_matches: int | None = 0
    last_updated: Optional[str] = None
    tier: Optional[str] = None


class TeamBasic(BaseModel):
    """Basic team info for lists."""
    team_id: str
    team_name: str
    display_name: Optional[str]
    avatar: Optional[str]
    matches_played: int = 0
    matches_won: int = 0
    matches_lost: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    match_win_rate: float = 0.0
    maps_played: int = 0
    maps_won: int = 0
    maps_lost: int = 0
    rounds_won: int = 0
    rounds_lost: int = 0
    rounds_diff: int = 0
    kills: int = 0
    deaths: int = 0
    kd: float = 0.0
    adr: float = 0.0
    damage: int = 0


class DivisionDetails(BaseModel):
    """Full division details."""
    championship_id: str
    slug: str
    name: str
    season: int
    division_num: int
    is_playoff: bool
    teams: List[TeamBasic]
    excluded_team_ids: List[str]
    map_stats: Optional[List[Dict[str, Any]]] = None
    leaders: Optional[List[Dict[str, Any]]] = None
    aggregates: Optional[Dict[str, Any]] = None
    player_count: int | None = None
    season_player_count: int | None = None
    all_time_player_count: int | None = None


class MapVoteStats(BaseModel):
    """Map pick/ban statistics for a division."""
    map_name: str
    total_votes: int
    picks: int
    bans: int
    deciders: int
    pick_rate: float
    ban_rate: float


@router.get("/seasons", response_model=List[SeasonInfo])
async def get_seasons():
    """Get all seasons with their divisions."""
    rows = await query_async(
        """
        SELECT DISTINCT season, division_num, championship_id
        FROM championships
        ORDER BY season DESC, division_num
        """
    )
    
    # Group by season
    seasons_map: dict[int, dict] = {}
    for row in rows:
        season = row["season"]
        if season not in seasons_map:
            seasons_map[season] = {
                "season": season,
                "divisions": [],
                "championship_ids": []
            }
        seasons_map[season]["divisions"].append(row["division_num"])
        seasons_map[season]["championship_ids"].append(row["championship_id"])
    
    return list(seasons_map.values())


@router.get("", response_model=List[DivisionSummary])
async def get_all_divisions():
    """Get all divisions across all seasons."""
    rows = await query_async(
        """
        SELECT championship_id, slug, name, season, division_num,
               is_playoffs AS is_playoff
        FROM championships
        ORDER BY season DESC, division_num, is_playoffs
        """
    )
    return [DivisionSummary(**row) for row in rows]


@router.get("/season/{season}", response_model=List[DivisionSummary])
async def get_divisions_by_season(season: int):
    """Get all divisions for a specific season."""
    # Return division metadata plus aggregates so the frontend can compute progress
    # Compute per-division aggregates using only existing tables (championships, matches).
    # teams_count is derived from distinct team1/team2 entries in matches for the championship.
    rows = await query_async(
        """
        SELECT
            c.championship_id,
            c.slug,
            c.name,
            c.season,
            c.division_num,
            CASE WHEN c.slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff,
            0 AS teams_count,
            COUNT(DISTINCT CASE WHEN m.finished_at IS NOT NULL THEN m.match_id END) AS played_matches,
            COUNT(DISTINCT m.match_id) AS total_matches,
            MAX(m.updated_at) AS last_updated
        FROM championships c
        LEFT JOIN matches m ON c.championship_id = m.championship_id
        WHERE c.season = :season
        GROUP BY c.championship_id, c.slug, c.name, c.season, c.division_num, is_playoff
        ORDER BY c.division_num ASC
        """,
        {"season": season}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"No divisions found for season {season}")
    
    return [
        {
            "championship_id": r["championship_id"],
            "slug": r["slug"],
            "name": r["name"],
            "season": int(r["season"]),
            "division_num": int(r["division_num"]),
            "is_playoff": bool(r["is_playoff"]),
            "teams_count": int(r.get("teams_count") or 0),
            "played_matches": int(r.get("played_matches") or 0),
            "total_matches": int(r.get("total_matches") or 0),
            "last_updated": (r.get("last_updated").isoformat() if r.get("last_updated") is not None else None) if hasattr(r.get("last_updated"), 'isoformat') else (r.get("last_updated") or None),
            "tier": r.get("tier")
        }
        for r in rows
    ]


@router.get("/by-slug/{slug}", response_model=DivisionDetails)
async def get_division_by_slug(slug: str):
    """Get full division details by slug (e.g., 'div1-s11')."""
    # Get championship info
    champ_rows = await query_async(
        """
     SELECT championship_id, slug, name, season, division_num,
         CASE WHEN slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff
        FROM championships
        WHERE slug = :slug
        """,
        {"slug": slug}
    )
    
    if not champ_rows:
        raise HTTPException(status_code=404, detail=f"Division '{slug}' not found")
    
    return await _get_division_details(champ_rows[0])


async def _fetch_championship_row_by_id(championship_id: str) -> dict:
    """Fetch a single championship row by its ID or raise 404."""
    champ_rows = await query_async(
        """
        SELECT championship_id, slug, name, season, division_num,
            CASE WHEN slug LIKE '%%-po%%' THEN 1 ELSE 0 END AS is_playoff
        FROM championships
        WHERE championship_id = :champ_id
        """,
        {"champ_id": championship_id}
    )
    
    if not champ_rows:
        raise HTTPException(status_code=404, detail=f"Championship '{championship_id}' not found")

    return champ_rows[0]


@router.get("/{championship_id}", response_model=DivisionDetails)
async def get_division_by_id(championship_id: str):
    """Get full division details by championship ID (primary endpoint)."""
    champ_row = await _fetch_championship_row_by_id(championship_id)
    return await _get_division_details(champ_row)


@router.get("/{championship_id}/details", response_model=DivisionDetails)
async def get_division_by_id_legacy(championship_id: str):
    """Legacy path retained for compatibility with older clients."""
    champ_row = await _fetch_championship_row_by_id(championship_id)
    
    return await _get_division_details(champ_row)


async def _get_division_details(champ: dict) -> dict:
    """Helper to fetch full division details from championship row."""
    championship_id = champ["championship_id"]
    season = int(champ["season"])
    division_num = int(champ["division_num"])
    
    # Fetch team info plus season aggregates (matches/maps/rounds) and aggregated kills/deaths/damage
    team_rows = await query_async(
        """
        SELECT DISTINCT t.team_id, t.name AS team_name, t.name AS display_name, t.avatar,
               COALESCE(tst.matches_played, 0) AS matches_played,
               COALESCE(tst.matches_won, 0) AS matches_won,
               COALESCE(tst.maps_played, 0) AS maps_played,
               COALESCE(tst.maps_won, 0) AS maps_won,
               COALESCE(tst.rounds_won, 0) AS rounds_won,
               COALESCE(tst.rounds_lost, 0) AS rounds_lost,
               COALESCE(agg.kills, 0) AS kills,
               COALESCE(agg.deaths, 0) AS deaths,
               COALESCE(agg.damage, 0) AS damage
        FROM teams t
        JOIN matches m ON (m.team1_id = t.team_id OR m.team2_id = t.team_id)
        LEFT JOIN team_season_totals tst ON tst.team_id = t.team_id AND tst.season = :season AND tst.division_num = :division
        LEFT JOIN (
            SELECT team_id, SUM(kills) AS kills, SUM(deaths) AS deaths, SUM(damage) AS damage
            FROM team_map_season_totals
            WHERE season = :season AND division_num = :division
            GROUP BY team_id
        ) agg ON agg.team_id = t.team_id
        WHERE m.championship_id = :champ_id
        ORDER BY team_name, team_id
        """,
        {"champ_id": championship_id, "season": season, "division": division_num}
    )

    # Get excluded teams
    excluded = get_excluded_team_ids(championship_id)

    # Fetch players participating for each team in this division
    player_rows = await query_async(
        """
        SELECT
            pst.team_id,
            pst.player_id,
            pst.maps_played,
            pst.rounds_played,
            pst.kills,
            pst.deaths,
            p.nickname
        FROM player_season_totals pst
        JOIN players p ON p.player_id = pst.player_id
        WHERE pst.season = :season
          AND pst.division_num = :division
        """,
        {"season": season, "division": division_num},
    )

    players_by_team: dict[str, list[dict[str, Any]]] = {}
    unique_player_ids: set[str] = set()
    for prow in player_rows:
        team_id = prow.get("team_id")
        if not team_id:
            continue
        player_id = str(prow.get("player_id") or "")
        if player_id:
            unique_player_ids.add(player_id)
        players_by_team.setdefault(team_id, []).append(
            {
                "player_id": prow.get("player_id"),
                "nickname": prow.get("nickname"),
                "maps_played": int(prow.get("maps_played") or 0),
                "rounds_played": int(prow.get("rounds_played") or 0),
                "kills": int(prow.get("kills") or 0),
                "deaths": int(prow.get("deaths") or 0),
            }
        )

    for plist in players_by_team.values():
        plist.sort(key=lambda p: (p.get("nickname") or "").lower())

    # Normalize team stats for frontend consumption (matches, maps, rounds, win% etc.)
    teams: list[dict[str, Any]] = []
    for t in team_rows:
        matches_played = int(t.get("matches_played") or 0)
        matches_won = int(t.get("matches_won") or 0)
        matches_lost = max(matches_played - matches_won, 0)
        maps_played = int(t.get("maps_played") or 0)
        maps_won = int(t.get("maps_won") or 0)
        maps_lost = max(maps_played - maps_won, 0)
        rounds_won = int(t.get("rounds_won") or 0)
        rounds_lost = int(t.get("rounds_lost") or 0)
        rounds_diff = rounds_won - rounds_lost
        kills = int(t.get("kills") or 0)
        deaths = int(t.get("deaths") or 0)
        damage = int(t.get("damage") or 0)
        total_rounds = rounds_won + rounds_lost

        kd = kills / deaths if deaths else (float(kills) if kills else 0.0)
        adr = damage / total_rounds if total_rounds else 0.0
        match_win_rate = (matches_won / matches_played * 100) if matches_played else 0.0
        map_win_rate = (maps_won / maps_played * 100) if maps_played else 0.0

        teams.append({
            "team_id": t["team_id"],
            "team_name": t["team_name"],
            "display_name": t.get("display_name"),
            "avatar": t.get("avatar") or DEFAULT_AVATAR,
            "matches_played": matches_played,
            "matches_won": matches_won,
            "matches_lost": matches_lost,
            "wins": matches_won,
            "losses": matches_lost,
            "win_rate": round(map_win_rate, 1),
            "match_win_rate": round(match_win_rate, 1),
            "maps_played": maps_played,
            "maps_won": maps_won,
            "maps_lost": maps_lost,
            "rounds_won": rounds_won,
            "rounds_lost": rounds_lost,
            "rounds_diff": rounds_diff,
            "kills": kills,
            "deaths": deaths,
            "kd": round(kd, 2) if isinstance(kd, float) else kd,
            "adr": round(adr, 1),
            "damage": damage,
            "players": players_by_team.get(t["team_id"], []),
        })

    # Compute aggregated player counts (division, season, all-time)
    player_counts = await get_player_counts(season=season, division=division_num, include_all_time=True)
    division_player_count = player_counts.get("division_players")
    if (division_player_count is None or division_player_count == 0) and unique_player_ids:
        division_player_count = len(unique_player_ids)

    season_player_count = player_counts.get("season_players") or 0
    all_time_player_count = player_counts.get("all_time_players") or 0

    # Get map stats aggregated for the division
    map_stats = await _get_division_map_stats(championship_id, season, division_num)
    
    # Get division aggregates
    aggregates = await _get_division_aggregates(
        championship_id,
        season,
        division_num,
    )
    
    # Get leaders
    leaders = await _get_division_leaders(championship_id, season, division_num)
    
    return {
        "championship_id": championship_id,
        "slug": champ["slug"],
        "name": champ["name"],
        "season": champ["season"],
        "division_num": champ["division_num"],
        "is_playoff": bool(champ["is_playoff"]),
        "teams": teams,
        "excluded_team_ids": list(excluded),
        "map_stats": map_stats,
        "aggregates": aggregates,
        "leaders": leaders,
        "player_count": int(division_player_count or 0),
        "season_player_count": int(season_player_count),
        "all_time_player_count": int(all_time_player_count),
    }


async def _get_division_map_stats(championship_id: str, season: int, division_num: int) -> List[Dict[str, Any]]:
    """Get aggregated map stats for a division."""
    # Aggregate player stats by map for this division
    rows = await query_async(
        """
        WITH division_matches AS (
            SELECT match_id
            FROM matches
            WHERE championship_id = :champ_id
              AND season = :season
              AND division_num = :division
        ),
        division_maps AS (
            SELECT
                m.map_id,
                m.map_name,
                COALESCE(m.score_team1, 0) AS score_team1,
                COALESCE(m.score_team2, 0) AS score_team2
            FROM maps m
            JOIN division_matches dm ON dm.match_id = m.match_id
            WHERE m.map_name IS NOT NULL
              AND m.season = :season
              AND m.division_num = :division
              AND m.is_forfeit = 0
        ),
        player_totals AS (
            SELECT
                LOWER(dm.map_name) AS map_key,
                dm.map_name,
                COUNT(DISTINCT dm.map_id) AS maps_played,
                SUM(ps.kills) AS kills,
                SUM(ps.deaths) AS deaths,
                SUM(ps.damage) AS damage,
                AVG(ps.adr) AS adr,
                AVG(ps.kr) AS kr,
                SUM(ps.utility_damage) AS utility_damage,
                SUM(ps.enemies_flashed) AS enemies_flashed,
                SUM(ps.flash_count) AS flash_count,
                SUM(ps.sniper_kills) AS sniper_kills,
                SUM(ps.assists) AS assists,
                SUM(ps.mk_2k) AS k2,
                SUM(ps.mk_3k) AS k3,
                SUM(ps.mk_4k) AS k4,
                SUM(ps.mk_5k) AS ace,
                SUM(ps.pistol_kills) AS pistol_kills
            FROM division_maps dm
            LEFT JOIN player_stats ps ON (
                ps.map_id = dm.map_id
                AND ps.is_forfeit_map = 0
                AND ps.season = :season
                AND ps.division_num = :division
            )
            GROUP BY LOWER(dm.map_name), dm.map_name
        ),
        round_totals AS (
            SELECT
                LOWER(dm.map_name) AS map_key,
                SUM(dm.score_team1 + dm.score_team2) AS rounds_played
            FROM division_maps dm
            GROUP BY LOWER(dm.map_name)
        ),
        map_vote_totals AS (
            SELECT
                LOWER(v.map_name) AS map_key,
                COUNT(*) AS banned
            FROM map_votes v
            JOIN division_matches dm ON dm.match_id = v.match_id
            WHERE v.map_name IS NOT NULL
              AND v.season = :season
              AND v.division_num = :division
              AND LOWER(v.status) IN ('banned','ban','drop','removed','remove','veto')
            GROUP BY LOWER(v.map_name)
        )
        SELECT
            pt.map_name,
            mc.pretty_name,
            mc.image_sm,
            pt.maps_played,
            COALESCE(mvt.banned, 0) AS banned,
            COALESCE(pt.kills, 0) AS kills,
            COALESCE(pt.deaths, 0) AS deaths,
            COALESCE(pt.damage, 0) AS damage,
            COALESCE(rt.rounds_played, 0) AS rounds_played,
            COALESCE(pt.adr, 0) AS adr,
            COALESCE(pt.kr, 0) AS kr,
            CASE WHEN COALESCE(pt.deaths, 0) = 0 THEN 0 ELSE COALESCE(pt.utility_damage, 0) / NULLIF(pt.deaths, 0) END AS udpr,
            CASE WHEN COALESCE(pt.flash_count, 0) = 0 THEN 0 ELSE COALESCE(pt.enemies_flashed, 0) / NULLIF(pt.flash_count, 0) END AS enemy_flash,
            COALESCE(pt.sniper_kills, 0) AS sniper_kills,
            COALESCE(pt.assists, 0) AS assists,
            COALESCE(pt.k2, 0) AS k2,
            COALESCE(pt.k3, 0) AS k3,
            COALESCE(pt.k4, 0) AS k4,
            COALESCE(pt.ace, 0) AS ace,
            COALESCE(pt.pistol_kills, 0) AS pistol_kills
        FROM player_totals pt
        LEFT JOIN round_totals rt ON rt.map_key = pt.map_key
        LEFT JOIN map_vote_totals mvt ON mvt.map_key = pt.map_key
        LEFT JOIN maps_catalog mc ON mc.map_id = pt.map_name
        ORDER BY pt.maps_played DESC, pt.map_name
        """,
        {"champ_id": championship_id, "season": season, "division": division_num}
    )
    
    result = []
    for r in rows:
        maps_played = int(r.get("maps_played") or 0)
        rounds_played = int(r.get("rounds_played") or 0)
        
        result.append({
            "map_name": r.get("pretty_name") or r["map_name"],
            "curr": {
                "logo": r.get("image_sm"),
                "maps_played": maps_played,
                "banned": int(r.get("banned") or 0),
                "rounds_played": rounds_played,
                "rounds_per_map": round(rounds_played / maps_played, 2) if maps_played > 0 else 0,
                "kills": int(r.get("kills") or 0),
                "deaths": int(r.get("deaths") or 0),
                "adr": round(float(r.get("adr") or 0), 1),
                "kr": round(float(r.get("kr") or 0), 2),
                "udpr": round(float(r.get("udpr") or 0), 2),
                "enemy_flash": round(float(r.get("enemy_flash") or 0), 2),
                "sniper_kills": int(r.get("sniper_kills") or 0),
                "assists": int(r.get("assists") or 0),
                "k2": int(r.get("k2") or 0),
                "k3": int(r.get("k3") or 0),
                "k4": int(r.get("k4") or 0),
                "ace": int(r.get("ace") or 0),
                "pistol_kills": int(r.get("pistol_kills") or 0),
            }
        })
    
    return result


async def _get_division_aggregates(
    championship_id: str,
    season: int,
    division_num: int,
) -> Dict[str, Any]:
    """Get division-wide aggregate statistics."""
    params = {"champ_id": championship_id, "season": season, "division": division_num}

    # Aggregate map-level counts (exclude forfeits and ignored matches)
    map_rows = await query_async(
        """
        SELECT
            COUNT(*) AS maps_played_total,
            COALESCE(SUM(COALESCE(m.score_team1, 0) + COALESCE(m.score_team2, 0)), 0) AS rounds_played_total
        FROM maps m
        JOIN matches ma ON ma.match_id = m.match_id
        WHERE ma.championship_id = :champ_id
          AND ma.season = :season
          AND ma.division_num = :division
          AND ma.ignored_due_ban = 0
          AND m.is_forfeit = 0
        """,
        params,
    )

    map_data = map_rows[0] if map_rows else {"maps_played_total": 0, "rounds_played_total": 0}
    maps_played_total = int(map_data.get("maps_played_total") or 0)
    rounds_played_total = int(map_data.get("rounds_played_total") or 0)

    # Fetch per-player season totals for aggregate sums and medians
    player_rows = await query_async(
        """
        SELECT rounds_played, kills, deaths, adr, kr
        FROM player_season_totals
        WHERE season = :season
          AND division_num = :division
          AND maps_played > 0
        """,
        params,
    )

    total_kills = 0
    total_deaths = 0
    adr_values: list[float] = []
    kr_values: list[float] = []
    survival_pct_values: list[float] = []

    for row in player_rows:
        kills = int(row.get("kills") or 0)
        deaths = int(row.get("deaths") or 0)
        rounds_played = float(row.get("rounds_played") or 0)
        adr = row.get("adr")
        kr = row.get("kr")

        total_kills += kills
        total_deaths += deaths

        if isinstance(adr, (int, float)):
            adr_values.append(float(adr))
        if isinstance(kr, (int, float)):
            kr_values.append(float(kr))
        if rounds_played > 0:
            deaths_per_round = deaths / rounds_played
            survival_pct = max(0.0, 1.0 - deaths_per_round) * 100.0
            survival_pct_values.append(survival_pct)

    def _median(values: list[float]) -> float | str:
        if not values:
            return "-"
        sorted_vals = sorted(values)
        mid = len(sorted_vals) // 2
        if len(sorted_vals) % 2:
            return sorted_vals[mid]
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2

    median_adr_raw = _median(adr_values)
    median_kr_raw = _median(kr_values)
    median_survival_raw = _median(survival_pct_values)

    median_adr = "-" if median_adr_raw == "-" else round(float(median_adr_raw), 1)
    median_kr = "-" if median_kr_raw == "-" else round(float(median_kr_raw), 2)
    median_survival = "-" if median_survival_raw == "-" else round(float(median_survival_raw), 1)

    return {
        "maps_played_total": maps_played_total,
        "rounds_played_total": rounds_played_total,
        "total_kills": total_kills,
        "total_deaths": total_deaths,
        "median_adr": median_adr,
        "median_kr": median_kr,
        "median_survival": median_survival,
    }



async def _get_division_leaders(championship_id: str, season: int, division_num: int) -> List[Dict[str, Any]]:
    """Get top players by category for division leaders board."""
    leader_definitions: list[dict[str, Any]] = [
        {"title": 'Liiga Ruusu', "description": "Pelaa enemmän kuin ehtii nukkua. Klassinen 'vielä yks matsi' -mentaliteetti. Eniten kierroksia pelattu.", "value_expr": 'pst.rounds_played', "statKey": 'Most Rounds', "order": 'DESC', "decimals": 0, "min_maps": 1},
        {"title": '"Nuori" osuja', "description": 'Näyttää nuorille, että vanha jaksaa vielä painaa. Paras K/D.', "value_expr": 'pst.kd', "statKey": 'Best K/D', "order": 'DESC', "decimals": 2},
        {"title": 'ADR-luvat kunnossa', "description": 'Jokainen luoti osuu... ainakin johonkin. Käsi tärisee, mutta tulosta tulee. Paras ADR.', "value_expr": 'pst.adr', "statKey": 'Best ADR', "order": 'DESC', "decimals": 2},
        {"title": 'Papalla on aim assist', "description": 'Vaimo kyselee koska tuut nukkumaan, mut papalla on flow päällä. Paras Kill/Round', "value_expr": 'pst.kr', "statKey": 'Best K/R', "order": 'DESC', "decimals": 2},
        {"title": 'DPS-dinosaurus', "description": 'Kaikki vahinko, ei voittoja – mutta numerot näyttää hyvältä! Suurin total damage.', "value_expr": 'pst.damage', "statKey": 'Most Total Damage', "order": 'DESC', "decimals": 0},
        {"title": 'Viikatemiehet', "description": 'Voittamattomat pelaajat. Eniten tappoja.', "value_expr": 'pst.kills', "statKey": 'Most Kills', "order": 'DESC', "decimals": 0},
        {"title": 'Spectaattori', "description": 'Näkee enemmän deathcamia kuin peliä. Eniten kuolemia.', "value_expr": 'pst.deaths', "statKey": 'Most Deaths', "order": 'DESC', "decimals": 0},
        {"title": 'Tukipappajoukot', "description": 'Syöttää frägejä kuin Pappa grillimakkaraa. Eniten Assists.', "value_expr": 'pst.assists', "statKey": 'Most Assists', "order": 'DESC', "decimals": 0},
        {"title": 'Parhaat suonenvedot', "description": 'Käsi muistaa sen spray-patterin vieläkin. Paras HS%', "value_expr": 'pst.hs_pct', "statKey": 'Best HS%', "order": 'DESC', "decimals": 2, "as_percent": True},
        {"title": 'Pelin isähahmo', "description": 'MVP, koska joku muukin tarvitsee roolimallin. Eniten MVP.', "value_expr": 'pst.mvps', "statKey": 'Most MVPs', "order": 'DESC', "decimals": 0},
        {"title": 'Vanha pää, kova käsi', "description": 'Refleksit ei kuolleet vielä. Eniten Clutch Kills.', "value_expr": 'pst.clutch_kills', "statKey": 'Most Clutch Kills', "order": 'DESC', "decimals": 0},
        {"title": 'Parhaat nitrot', "description": '1v3? Ei ongelmaa – ainakin jos nitrot ehtii vaikuttaa. Paras Clutch WR%.', "value_expr": 'CASE WHEN (COALESCE(pst.cl_1v1_attempts,0) + COALESCE(pst.cl_1v2_attempts,0)) > 0 THEN 100.0 * (COALESCE(pst.cl_1v1_wins,0) + COALESCE(pst.cl_1v2_wins,0)) / (COALESCE(pst.cl_1v1_attempts,0) + COALESCE(pst.cl_1v2_attempts,0)) END', "statKey": 'Best Clutch WR%', "order": 'DESC', "decimals": 2, "as_percent": True, "extra_where": ['(COALESCE(pst.cl_1v1_attempts,0) + COALESCE(pst.cl_1v2_attempts,0)) >= 5']},
        {"title": 'Kranaatti vyö tyhjäksi', "description": 'Polttaa enemmän kuin 2000-luvun LANit. Eniten utility damage.', "value_expr": 'pst.utility_damage', "statKey": 'Most Utility Damage', "order": 'DESC', "decimals": 0},
        {"title": 'Mikä pahan tappaisi', "description": 'Ei puske – jää eloon, säästää eläkkeelle. Klassinen pappa-strat. Paras Survival%.', "value_expr": 'CASE WHEN pst.rounds_played > 0 THEN 100.0 * (1 - COALESCE(pst.deaths,0) / pst.rounds_played) END', "statKey": 'Best Survival%', "order": 'DESC', "decimals": 2, "as_percent": True, "extra_where": ['pst.rounds_played >= 30']},
        {"title": 'Tilastopappa', "description": 'Kaikki numerot kunnossa, vaikka crosshair ei ole. Paras Rating1.', "value_expr": 'pst.rating', "statKey": 'Best Rating1', "order": 'DESC', "decimals": 2},
        {"title": 'Valot päälle, papat!', "description": 'Heittää flashin ennen kuin round edes alkaa. Eniten flashbangheja heittänyt.', "value_expr": 'pst.flash_count', "statKey": 'Most flashes thrown', "order": 'DESC', "decimals": 0},
        {"title": 'Flash Bang Dance', "description": 'Vihu näkee enemmän välähdyksiä kuin diskoissa 90-luvulla. Eniten vihollisia sokaistu.', "value_expr": 'pst.enemies_flashed', "statKey": 'Most Flashed', "order": 'DESC', "decimals": 0},
        {"title": 'Täysmaito', "description": 'Kerrankin flash osuu muualle kuin omaan tiimiin. Eniten onnistuneita flashbangheja%.', "value_expr": 'CASE WHEN pst.flash_count > 0 THEN 100.0 * COALESCE(pst.flash_successes,0) / pst.flash_count END', "statKey": 'Most Successful Flash%', "order": 'DESC', "decimals": 2, "as_percent": True, "extra_where": ['pst.flash_count >= 10']},
        {"title": 'Eläkeläis-Eagle', "description": 'Kun rahaa ei oo rifleen, mutta luotto omaan käteen löytyy. Eniten pistoolitappoja.', "value_expr": 'pst.pistol_kills', "statKey": 'Most Pistol Kills', "order": 'DESC', "decimals": 0},
        {"title": 'Bossikielto peruttu', "description": 'Zoomaa ja muistelee CSGO-päiviä. Eniten sniper tappoja.', "value_expr": 'pst.sniper_kills', "statKey": 'Most Sniper Kills', "order": 'DESC', "decimals": 0},
    ]

    categories: list[dict[str, Any]] = []

    for meta in leader_definitions:
        value_expr = meta["value_expr"]
        order = meta.get("order", "DESC")
        extra_where = meta.get("extra_where", [])
        if isinstance(extra_where, str):
            extra_where = [extra_where]
        filters = "".join(f" AND {cond}" for cond in extra_where)
        query = f"""
            SELECT
                p.player_id,
                p.nickname,
                {value_expr} AS value,
                pst.maps_played,
                pst.rounds_played,
                pst.team_id,
                t.name AS team_name,
                t.avatar AS team_logo
            FROM player_season_totals pst
            JOIN players p ON p.player_id = pst.player_id
            LEFT JOIN teams t ON t.team_id = pst.team_id
            WHERE pst.season = :season AND pst.division_num = :division
              AND pst.maps_played >= :min_maps
            {filters}
            ORDER BY value {order}
            LIMIT :limit
        """
        rows = await query_async(
            query,
            {"season": season, "division": division_num, "min_maps": meta.get("min_maps", 3), "limit": meta.get("limit", 5)},
        )

        leaders: list[dict[str, Any]] = []
        decimals = meta.get("decimals")

        for row in rows:
            value = row.get("value")
            if value is None:
                continue
            if isinstance(value, (int, float)):
                val = float(value)
                if meta.get("as_percent") and val <= 1:
                    val *= 100.0
                val = round(val, decimals) if decimals is not None else int(round(val))
            else:
                val = value

            leaders.append(
                {
                    "id": f"{meta.get('statKey', meta.get('title'))}-{row['player_id']}",
                    "playerId": row["player_id"],
                    "playerName": row["nickname"],
                    "title": row["nickname"],
                    "teamId": row.get("team_id"),
                    "teamName": row.get("team_name") or "",
                    "teamLogo": row.get("team_logo") or DEFAULT_AVATAR,
                    "logo": row.get("team_logo") or DEFAULT_AVATAR,
                    "value": val,
                    "mapsPlayed": row.get("maps_played", 0),
                    "roundsPlayed": row.get("rounds_played", 0),
                    "subtitle": f"{int(row.get('maps_played', 0))} karttaa / {int(row.get('rounds_played', 0))} kierrosta",
                }
            )

        if leaders:
            categories.append(
                {
                    "id": meta.get("statKey", meta.get("title")),
                    "categoryTitle": meta.get("title"),
                    "description": meta.get("description"),
                    "statKey": meta.get("statKey"),
                    "leaders": leaders,
                }
            )

    return categories
