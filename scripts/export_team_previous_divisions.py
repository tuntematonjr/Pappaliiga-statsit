"""Export the previous season's division for teams listed in a CSV file.

Example:
    python scripts/export_team_previous_divisions.py \
        --input joukkueet-syksy-2026.csv \
        --output joukkueet-syksy-2026-divarit.csv \
        --season 13

The requested season is the season represented by the input CSV. By default,
the lookup season is one season earlier. The CSV must contain ``TeamName`` and
``FaceitTeamId`` columns.
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


OUTPUT_COLUMN = "LastSeasonDivision"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add each team's previous regular-season division to a CSV file."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input CSV containing TeamName and FaceitTeamId columns.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output CSV path.",
    )
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="Season represented by the input teams; lookup defaults to season - 1.",
    )
    parser.add_argument(
        "--previous-season",
        type=int,
        help="Override the lookup season instead of using --season - 1.",
    )
    parser.add_argument(
        "--output-column",
        default=OUTPUT_COLUMN,
        help=f"Name of the added CSV column (default: {OUTPUT_COLUMN}).",
    )
    return parser.parse_args()


def read_input(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        required = {"TeamName", "FaceitTeamId"}
        missing = required - set(fieldnames)
        if missing:
            missing_names = ", ".join(sorted(missing))
            raise ValueError(f"CSV is missing required column(s): {missing_names}")
        return list(reader), fieldnames


async def fetch_previous_divisions(
    team_ids: list[str],
    previous_season: int,
) -> dict[str, str]:
    unique_ids = list(dict.fromkeys(team_id for team_id in team_ids if team_id))
    if not unique_ids:
        return {}

    placeholders = ", ".join(f":team_id_{index}" for index in range(len(unique_ids)))
    params: dict[str, Any] = {
        f"team_id_{index}": team_id for index, team_id in enumerate(unique_ids)
    }
    params["previous_season"] = previous_season

    # team_championships is the normal source. Totals provide a fallback for
    # older data where the participation history was not populated.
    rows = await db_async.query_async(
        f"""
        SELECT team_id, division_num, source_priority, championship_id
        FROM (
            SELECT
                tc.team_id,
                c.division_num,
                0 AS source_priority,
                c.championship_id,
                tc.updated_at AS sort_time
            FROM team_championships tc
            JOIN championships c ON c.championship_id = tc.championship_id
            WHERE tc.team_id IN ({placeholders})
              AND c.season = :previous_season
              AND c.is_playoffs = 0

            UNION ALL

            SELECT
                tst.team_id,
                tst.division_num,
                1 AS source_priority,
                c.championship_id,
                NULL AS sort_time
            FROM team_season_totals tst
            JOIN championships c
              ON c.season = tst.season
             AND c.division_num = tst.division_num
             AND c.is_playoffs = 0
            WHERE tst.team_id IN ({placeholders})
              AND tst.season = :previous_season
        ) history
        ORDER BY team_id, source_priority, sort_time DESC, championship_id
        """,
        params,
    )

    result: dict[str, str] = {}
    for row in rows:
        team_id = str(row["team_id"])
        if team_id not in result:
            result[team_id] = str(row["division_num"])
    return result


def write_output(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    division_by_team: dict[str, str],
    output_column: str,
) -> None:
    output_fields = [field for field in fieldnames if field != output_column]
    output_fields.append(output_column)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output_row = dict(row)
            team_id = (row.get("FaceitTeamId") or "").strip()
            output_row[output_column] = division_by_team.get(team_id, "-")
            writer.writerow(output_row)


async def run(args: argparse.Namespace) -> None:
    rows, fieldnames = read_input(args.input)
    previous_season = args.previous_season or args.season - 1
    team_ids = [(row.get("FaceitTeamId") or "").strip() for row in rows]
    divisions = await fetch_previous_divisions(team_ids, previous_season)
    write_output(args.output, rows, fieldnames, divisions, args.output_column)
    found_count = sum(1 for team_id in team_ids if team_id in divisions)
    print(
        f"Wrote {len(rows)} rows to {args.output} "
        f"(lookup season {previous_season}; found {found_count})."
    )


async def main_async(args: argparse.Namespace) -> None:
    try:
        await run(args)
    finally:
        await db_async.close_pool()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()