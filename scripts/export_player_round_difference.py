"""Export per-player round-difference summaries by season and playoff status.

Examples:
    python scripts/export_player_round_difference.py
    python scripts/export_player_round_difference.py --min-season 12
    python scripts/export_player_round_difference.py --min-season 12 --output custom.csv

If --min-season is omitted, the script prompts for it. By default, the CSV is
written under exports/ in the project root. Each row contains one player,
season, and playoff-status combination for every season from the selected
minimum onwards. Map wins and losses are based on map scores; tied maps count
as played but not as won or lost.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db_async


FIELDNAMES = [
    "season",
    "division",
    "is_playoffs",
    "player_faceit_id",
    "player_name",
    "team_faceit_id",
    "team_name",
    "maps_played",
    "maps_won",
    "maps_lost",
    "rounds_won",
    "rounds_lost",
    "rd",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export player round wins, round losses, and round difference "
            "summarized by season and regular-season/playoff status."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output CSV path. Defaults to exports/"
            "player-round-difference-from-season-<min-season>.csv."
        ),
    )
    parser.add_argument(
        "--min-season",
        type=int,
        help="Include this season and every later season; prompted for if omitted.",
    )
    args = parser.parse_args()
    if args.min_season is not None and args.min_season <= 0:
        parser.error("--min-season must be a positive integer.")
    return args


def resolve_minimum_season(min_season: int | None) -> int:
    if min_season is not None:
        return min_season

    while True:
        try:
            response = input("Minimum season number to export: ").strip()
        except EOFError:
            raise SystemExit(
                "A minimum season is required; provide it with --min-season."
            ) from None

        try:
            season = int(response)
        except ValueError:
            print("Enter a positive season number.")
            continue
        if season > 0:
            return season
        print("Enter a positive season number.")


async def fetch_round_summaries(min_season: int) -> list[dict[str, Any]]:
    filters = [
        "m.season >= :min_season",
        "NULLIF(m.finished_at, 0) IS NOT NULL",
        "COALESCE(m.ignored_due_ban, 0) = 0",
        "COALESCE(m.is_forfeit, 0) = 0",
        "COALESCE(ps.is_forfeit_map, 0) = 0",
        "COALESCE(mp.is_forfeit, 0) = 0",
        "mp.score_team1 IS NOT NULL",
        "mp.score_team2 IS NOT NULL",
        "ps.team_id IS NOT NULL",
        "ps.team_id IN (m.team1_id, m.team2_id)",
    ]
    params: dict[str, Any] = {"min_season": min_season}

    return await db_async.query_async(
        f"""
        SELECT
            m.season,
            m.division_num AS division,
            c.is_playoffs,
            ps.player_id AS player_faceit_id,
            MAX(COALESCE(pc.player_name, p.nickname)) AS player_name,
            ps.team_id AS team_faceit_id,
            MAX(COALESCE(tc.team_name, t.name)) AS team_name,
            COUNT(DISTINCT mp.map_id) AS maps_played,
            COUNT(DISTINCT CASE
                WHEN (ps.team_id = m.team1_id AND mp.score_team1 > mp.score_team2)
                  OR (ps.team_id = m.team2_id AND mp.score_team2 > mp.score_team1)
                THEN mp.map_id
            END) AS maps_won,
            COUNT(DISTINCT CASE
                WHEN (ps.team_id = m.team1_id AND mp.score_team1 < mp.score_team2)
                  OR (ps.team_id = m.team2_id AND mp.score_team2 < mp.score_team1)
                THEN mp.map_id
            END) AS maps_lost,
            SUM(
                CASE
                    WHEN ps.team_id = m.team1_id THEN mp.score_team1
                    ELSE mp.score_team2
                END
            ) AS rounds_won,
            SUM(
                CASE
                    WHEN ps.team_id = m.team1_id THEN mp.score_team2
                    ELSE mp.score_team1
                END
            ) AS rounds_lost
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        JOIN championships c ON c.championship_id = m.championship_id
        LEFT JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN player_championships pc
          ON pc.player_id = ps.player_id
         AND pc.championship_id = m.championship_id
        LEFT JOIN teams t ON t.team_id = ps.team_id
        LEFT JOIN team_championships tc
          ON tc.team_id = ps.team_id
         AND tc.championship_id = m.championship_id
        JOIN maps mp
          ON mp.match_id = ps.match_id
         AND mp.round_index = ps.round_index
        WHERE {' AND '.join(filters)}
        GROUP BY m.season, m.division_num, c.is_playoffs, ps.player_id, ps.team_id
        ORDER BY m.season ASC, m.division_num ASC, c.is_playoffs ASC, ps.player_id ASC
        """,
        params,
    )


def write_output(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            rounds_won = int(row.get("rounds_won") or 0)
            rounds_lost = int(row.get("rounds_lost") or 0)
            writer.writerow(
                {
                    "season": row["season"],
                    "division": row["division"],
                    "is_playoffs": (
                        "true" if bool(int(row["is_playoffs"])) else "false"
                    ),
                    "player_faceit_id": row["player_faceit_id"],
                    "player_name": row.get("player_name") or "",
                    "team_faceit_id": row["team_faceit_id"],
                    "team_name": row.get("team_name") or "",
                    "maps_played": int(row.get("maps_played") or 0),
                    "maps_won": int(row["maps_won"]),
                    "maps_lost": int(row["maps_lost"]),
                    "rounds_won": rounds_won,
                    "rounds_lost": rounds_lost,
                    "rd": rounds_won - rounds_lost,
                }
            )


async def run(args: argparse.Namespace) -> None:
    min_season = resolve_minimum_season(args.min_season)
    output_path = args.output or (
        PROJECT_ROOT
        / "exports"
        / f"player-round-difference-from-season-{min_season}.csv"
    )
    rows = await fetch_round_summaries(min_season)
    write_output(output_path, rows)
    print(f"Wrote {len(rows)} rows to {output_path}.")


async def main_async(args: argparse.Namespace) -> None:
    try:
        await run(args)
    finally:
        await db_async.close_pool()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
